#!/bin/env python3
import itertools
import logging
import os
import time
from collections import deque

from telethon import utils
from telethon.errors import ChatAdminRequiredError
from telethon.extensions import BinaryReader
from telethon.tl import types, functions

__log__ = logging.getLogger(__name__)


VALID_TYPES = {
    'photo', 'document', 'video', 'audio', 'sticker', 'voice'
}


class _EntityDownloader:
    """
    Helper class to concisely keep track on which entities need to be
    dumped, which already have been dumped, and a function to dump them.
    """
    def __init__(self, client, dumper):
        self.client = client
        self.dumper = dumper
        self._pending = deque()
        self._pending_ids = set()
        self._dumped_ids = set()

    def extend_pending(self, entities):
        """Extends the queue of pending entities."""
        for entity in entities:
            if isinstance(entity, types.User):
                if entity.deleted or entity.min:
                    continue  # Empty name would cause IntegrityError
            elif isinstance(entity, types.Chat):
                # No need to queue these, extra request not needed
                self._dump_entity(entity)
                continue
            elif isinstance(entity, types.Channel):
                if entity.left:
                    continue  # Getting full info triggers ChannelPrivateError
            else:
                # Drop UserEmpty, ChatEmpty, ChatForbidden and ChannelForbidden
                continue
            eid = utils.get_peer_id(entity)
            if eid not in self._dumped_ids and not eid in self._pending_ids:
                self._pending_ids.add(eid)
                self._pending.append(entity)

    def _dump_entity(self, entity):
        needed_sleep = 1
        eid = utils.get_peer_id(entity)

        __log__.debug('Dumping entity %s', utils.get_display_name(entity))
        if isinstance(entity, types.User):
            full = self.client(functions.users.GetFullUserRequest(entity))
            photo_id = self.dumper.dump_media(full.profile_photo)
            self.dumper.dump_user(full, photo_id=photo_id)

        elif isinstance(entity, types.Chat):
            needed_sleep = 0
            photo_id = self.dumper.dump_media(entity.photo)
            self.dumper.dump_chat(entity, photo_id=photo_id)

        elif isinstance(entity, types.Channel):
            full = self.client(functions.channels.GetFullChannelRequest(entity))
            photo_id = self.dumper.dump_media(full.full_chat.chat_photo)
            if entity.megagroup:
                self.dumper.dump_supergroup(full.full_chat, entity, photo_id)
            else:
                self.dumper.dump_channel(full.full_chat, entity, photo_id)

        self._pending_ids.discard(eid)
        self._dumped_ids.add(eid)
        return needed_sleep

    def __bool__(self):
        return bool(self._pending)

    def __len__(self):
        return len(self._pending)

    def pop_pending(self):
        """Pops a pending entity off the queue and returns needed sleep."""
        if self._pending:
            return self._dump_entity(self._pending.popleft())
        else:
            return 0


class Downloader:
    def __init__(self, client, config):
        self.client = client
        self.max_size = int(config['MaxSize'])
        self.types = {x.strip().lower()
                     for x in (config.get('MediaWhitelist') or '').split(',')
                     if x.strip()}
        self.media_folder = os.path.join(config['OutputDirectory'], 'usermedia')
        # TODO make 'usermedia' a config option
        assert all(x in VALID_TYPES for x in self.types)

    def check_media(self, media):
        """
        Checks whether the given MessageMedia should be downloaded or not.
        """
        if not media or not self.max_size:
            return False
        if not self.types:
            return True

        if isinstance(media, types.MessageMediaPhoto):
            if 'photo' not in self.types:
                return False
        elif isinstance(media, types.MessageMediaDocument):
            if not isinstance(media, types.Document):
                return False
            for attr in media.attributes:
                if isinstance(attr, types.DocumentAttributeSticker):
                    return 'sticker' in self.types
                elif isinstance(attr, types.DocumentAttributeVideo):
                    return 'video' in self.types
                elif isinstance(attr, types.DocumentAttributeAudio):
                    if attr.voice:
                        return 'voice' in self.types
                    return 'audio' in self.types
            if 'document' not in self.types:
                return False
        return True

    def download_media(self, msg, target_id):
        """Save media to disk in self.media_folder (under OutputDirectory)."""
        # TODO Make usermedia/ folder an config option
        # TODO Make name format string a config option, and consider folders per context
        if isinstance(msg, types.Message):
            media = msg.media
        else:
            media = msg
        os.makedirs(self.media_folder, exist_ok=True)
        file_name_prefix = os.path.join(self.media_folder,'{}-{}-'.format(target_id, msg.id))
        if isinstance(media, types.MessageMediaDocument) and not hasattr(
                media.document, 'stickerset'):
            try:
                file_name = file_name_prefix + next(
                    a for a in media.document.attributes
                    if isinstance(a, types.DocumentAttributeFilename)
                ).file_name
            except StopIteration:
                file_name = self.media_folder  # Inferred by Telethon
            return self.client.download_media(msg, file=file_name)
        elif isinstance(media, types.MessageMediaPhoto):
            file_name = file_name_prefix + media.photo.date.strftime('photo_%Y-%m-%d_%H-%M-%S.jpg')
            return self.client.download_media(media, file=file_name)
        else:
            return None

    def save_messages(self, dumper, target_id):
        """
        Download and dump messages and media (depending on media config)
        from the target using the dumper, then dump all entities found.
        """
        target = self.client.get_input_entity(target_id)
        req = functions.messages.GetHistoryRequest(
            peer=target,
            offset_id=0,
            offset_date=None,
            add_offset=0,
            limit=dumper.chunk_size,
            max_id=0,
            min_id=0,
            hash=0
        )
        __log__.info('Starting dump with %s', target)
        chunks_left = dumper.max_chunks
        if isinstance(target, types.InputPeerSelf):
            target_id = self.client.get_me(input_peer=True).user_id
        else:
            target_id = utils.get_peer_id(target)

        entity_downloader = _EntityDownloader(self.client, dumper)
        if isinstance(target, (types.InputPeerChat, types.InputPeerChannel)):
            try:
                __log__.info('Getting participants...')
                participants = self.client.get_participants(target)
                added, removed = dumper.dump_participants_delta(
                    target_id, ids=[x.id for x in participants]
                )
                entity_downloader.extend_pending([p for p in participants if
                    p.id in added or p.id in removed])
                __log__.info('Saved %d new members, %d left the chat.',
                             len(added), len(removed))
            except ChatAdminRequiredError:
                __log__.info('Getting participants aborted (not admin).')

        req.offset_id, req.offset_date, stop_at = dumper.get_resume(target_id)
        if req.offset_id:
            __log__.info('Resuming at %s (%s)', req.offset_date, req.offset_id)

        found = dumper.get_message_count(target_id)
        while True:
            start = time.time()
            history = self.client(req)

            # Queue users and chats for dumping
            entity_downloader.extend_pending(
                itertools.chain(history.users, history.chats)
            )
            # Since the flood waits we would get from spamming GetFullX and
            # GetHistory are the same and are independent of each other, we can
            # ignore the 'recommended' sleep from pop_pending and use the later
            # sleep (1 - time_taken) for both of these (halving time taken here).
            entity_downloader.pop_pending()

            for m in history.messages:
                if isinstance(m, types.Message):
                    if self.check_media(m.media):
                        self.download_media(m, target_id)

                    fwd_id = dumper.dump_forward(m.fwd_from)
                    media_id = dumper.dump_media(m.media)
                    dumper.dump_message(m, target_id,
                                        forward_id=fwd_id, media_id=media_id)

                elif isinstance(m, types.MessageService):
                    dumper.dump_message_service(m, media_id=None)

                else:
                    __log__.warning('Skipping message %s', m)
                    continue

            total_messages = getattr(history, 'count', len(history.messages))
            if history.messages:
                # We may reinsert some we already have (so found > total)
                found = min(found + len(history.messages), total_messages)
                req.offset_id = min(m.id for m in history.messages)
                req.offset_date = min(m.date for m in history.messages)

            __log__.debug('Downloaded {}/{} ({:.1%})'.format(
                found, total_messages, found / total_messages
            ))

            if len(history.messages) < req.limit:
                __log__.info('Received less messages than limit, done.')
                # Receiving less messages than the limit means we have reached
                # the end, so we need to exit. Next time we'll start from offset
                # 0 again so we can check for new messages.
                max_msg_id = dumper.get_message_id(target_id, 'MAX')
                dumper.save_resume(target_id, stop_at=max_msg_id)
                break

            # We dump forward (message ID going towards 0), so as soon
            # as the minimum message ID (now in offset ID) is less than
            # the highest ID ("closest" bound we need to reach), stop.
            if req.offset_id <= stop_at:
                __log__.info('Reached already-dumped messages, done.')
                max_msg_id = dumper.get_message_id(target_id, 'MAX')
                dumper.save_resume(target_id, stop_at=max_msg_id)
                break

            # Keep track of the last target ID (smallest one),
            # so we can resume from here in case of interruption.
            dumper.save_resume(
                target_id, msg=req.offset_id, msg_date=req.offset_date,
                stop_at=stop_at  # We DO want to preserve stop_at though.
            )

            chunks_left -= 1  # 0 means infinite, will reach -1 and never 0
            if chunks_left == 0:
                __log__.info('Reached maximum amount of chunks, done.')
                break

            dumper.commit()
            # 30 request in 30 seconds (sleep a second *between* requests)
            time.sleep(max(1 - (time.time() - start), 0))
        dumper.commit()

        __log__.info(
            'Done. Retrieving full information about {} missing entities.'
            .format(len(entity_downloader))
        )
        # TODO Save their profile picture
        while entity_downloader:
            start = time.time()
            needed_sleep = entity_downloader.pop_pending()
            dumper.commit()
            time.sleep(max(needed_sleep - (time.time() - start), 0))

        __log__.info('Dump with %s finished', target)

    def fetch_dialogs(self, cache_file='dialogs.tl', force=False):
        """Get a list of dialogs, and dump new data from them"""
        # TODO What to do about cache invalidation?
        if not force and os.path.isfile(cache_file):
            with open(cache_file, 'rb') as f, BinaryReader(stream=f) as reader:
                entities = []
                while True:
                    try:
                        entities.append(reader.tgread_object())
                    except BufferError:
                        break  # No more data left to read
                return entities
        with open(cache_file, 'wb') as f:
            entities = [d.entity for d in self.client.get_dialogs(limit=None)]
            for entity in entities:
                f.write(bytes(entity))

        return entities

    def load_entities_from_str(self, string):
        """Helper function to load entities from the config file"""
        for who in string.split(','):
            who = who.strip()
            if (not who.startswith('+') and who.isdigit()) or who.startswith('-'):
                yield self.client.get_input_entity(int(who))
            else:
                yield self.client.get_input_entity(who)
