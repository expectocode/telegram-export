#!/bin/env python3
import itertools
import logging
import os
import time
from collections import deque, defaultdict

import datetime
from telethon import utils
from telethon.errors import ChatAdminRequiredError
from telethon.extensions import BinaryReader
from telethon.tl import types, functions

__log__ = logging.getLogger(__name__)


VALID_TYPES = {
    'photo', 'document', 'video', 'audio', 'sticker', 'voice', 'chatphoto'
}


class _EntityDownloader:
    """
    Helper class to concisely keep track on which entities need to be
    dumped, which already have been dumped, and a function to dump them.

    If no photo_fmt is provided, entity photos will not be downloaded.
    """
    def __init__(self, client, dumper, photo_fmt=None):
        self.client = client
        self.dumper = dumper
        self.photo_fmt = photo_fmt
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
            self.download_profile_photo(full.profile_photo, entity)

        elif isinstance(entity, types.Chat):
            needed_sleep = 0
            photo_id = self.dumper.dump_media(entity.photo)
            self.dumper.dump_chat(entity, photo_id=photo_id)
            self.download_profile_photo(entity.photo, entity)

        elif isinstance(entity, types.Channel):
            full = self.client(functions.channels.GetFullChannelRequest(entity))
            photo_id = self.dumper.dump_media(full.full_chat.chat_photo)
            if entity.megagroup:
                self.dumper.dump_supergroup(full.full_chat, entity, photo_id)
            else:
                self.dumper.dump_channel(full.full_chat, entity, photo_id)
            self.download_profile_photo(full.full_chat.chat_photo, entity)

        self._pending_ids.discard(eid)
        self._dumped_ids.add(eid)
        return needed_sleep

    def download_profile_photo(self, photo, target, known_id=None):
        """
        Similar to Downloader.download_media() but for profile photos.

        Has no effect if there is no photo format (thus it is "disabled").
        """
        if not self.photo_fmt:
            return

        date = datetime.datetime.now()
        if isinstance(photo, (types.UserProfilePhoto, types.ChatPhoto)):
            if isinstance(photo.photo_big, types.FileLocation):
                location = photo.photo_big
            elif isinstance(photo.photo_small, types.FileLocation):
                location = photo.photo_small
            else:
                return
        elif isinstance(photo, types.Photo):
            for size in photo.sizes:
                if isinstance(size, types.PhotoSize):
                    if isinstance(size.location, types.FileLocation):
                        location = size.location
                        break
            else:
                return
            date = photo.date
            if known_id is None:
                known_id = photo.id
        else:
            return

        if known_id is None:
            known_id = utils.get_peer_id(target)

        formatter = defaultdict(
            str,
            id=known_id,
            context_id=utils.get_peer_id(target),
            sender_id=utils.get_peer_id(target),
            ext='.jpg',
            type='chatphoto',
            filename=date.strftime('chatphoto_%Y-%m-%d_%H-%M-%S'),
            name=utils.get_display_name(target) or 'unknown',
            sender_name=utils.get_display_name(target) or 'unknown'
        )
        filename = date.strftime(self.photo_fmt).format_map(formatter)
        if not filename.endswith(formatter['ext']):
            if filename.endswith('.'):
                filename = filename[:-1]
            filename += formatter['ext']

        os.makedirs(os.path.dirname(filename), exist_ok=True)
        return self.client.download_file(types.InputFileLocation(
            volume_id=location.volume_id,
            local_id=location.local_id,
            secret=location.secret
        ), file=filename, part_size_kb=256)

    def __bool__(self):
        return bool(self._pending)

    def __len__(self):
        return len(self._pending)

    def pop_pending(self):
        """Pops a pending entity off the queue and returns needed sleep."""
        if self._pending:
            return self._dump_entity(self._pending.popleft())
        return 0


class Downloader:
    """
    Download dialogs and their associated data, and dump them.
    Make Telegram API requests and sleep for the appropriate time.
    """
    def __init__(self, client, config):
        self.client = client
        self.max_size = int(config['MaxSize'])
        self.types = {x.strip().lower()
                      for x in (config.get('MediaWhitelist') or '').split(',')
                      if x.strip()}
        self.media_fmt = os.path.join(config['OutputDirectory'],
                                      config['MediaFilenameFmt'])
        assert all(x in VALID_TYPES for x in self.types)
        if self.types:
            self.types.add('unknown')  # Always allow "unknown" media types

    @staticmethod
    def _get_media_type(media):
        """
        Returns the friendly type string for the given MessageMedia.
        """
        if not media:
            return ''
        if isinstance(media, types.MessageMediaPhoto):
            return 'photo'
        elif isinstance(media, types.MessageMediaDocument):
            if not isinstance(media, types.Document):
                return False
            for attr in media.attributes:
                if isinstance(attr, types.DocumentAttributeSticker):
                    return 'sticker'
                elif isinstance(attr, types.DocumentAttributeVideo):
                    return 'video'
                elif isinstance(attr, types.DocumentAttributeAudio):
                    if attr.voice:
                        return 'voice'
                    return 'audio'
            return 'document'
        return 'unknown'

    @staticmethod
    def _get_media_extension(media):
        pass

    def check_media(self, media):
        """
        Checks whether the given MessageMedia should be downloaded or not.
        """
        if not media or not self.max_size:
            return False
        if not self.types:
            return True
        return self._get_media_type(media) in self.types

    def download_media(self, msg, target_id, entities):
        """
        Save media to disk using the self.media_fmt under OutputDirectory.

        The entities parameter must be a dictionary consisting of {id: entity}
        and it *has* to contain the IDs for sender_id and context_id.
        """
        media = msg.media
        if isinstance(media, types.MessageMediaPhoto):
            if isinstance(media.photo, types.PhotoEmpty):
                return None
        elif isinstance(media, types.MessageMediaDocument):
            if isinstance(media.document, types.DocumentEmpty):
                return None
        else:
            return None

        formatter = defaultdict(
            str,
            id=msg.id,
            context_id=target_id,
            sender_id=msg.from_id or 0,
            ext=utils.get_extension(media) or '.bin',
            type=self._get_media_type(media) or 'unknown',
            name=utils.get_display_name(entities[target_id]) or 'unknown',
            sender_name=utils.get_display_name(
                entities.get(msg.from_id)) or 'unknown'
        )
        filename = None
        if isinstance(media, types.MessageMediaDocument):
            for attr in media.document.attributes:
                if isinstance(attr, types.DocumentAttributeFilename):
                    filename = attr.file_name

        formatter['filename'] = filename or msg.date.strftime(
            '{}_%Y-%m-%d_%H-%M-%S'.format(formatter['type'])
        )
        filename = msg.date.strftime(self.media_fmt).format_map(formatter)
        if not filename.endswith(formatter['ext']):
            if filename.endswith('.'):
                filename = filename[:-1]
            filename += formatter['ext']

        os.makedirs(os.path.dirname(filename), exist_ok=True)
        return self.client.download_media(media, file=filename)

    def save_messages(self, dumper, target_id):
        """
        Download and dump messages and media (depending on media config)
        from the target using the dumper, then dump all entities found.
        """
        target_in = self.client.get_input_entity(target_id)
        target = self.client.get_entity(target_in)
        target_id = utils.get_peer_id(target)
        req = functions.messages.GetHistoryRequest(
            peer=target_in,
            offset_id=0,
            offset_date=None,
            add_offset=0,
            limit=dumper.chunk_size,
            max_id=0,
            min_id=0,
            hash=0
        )
        __log__.info('Starting dump with %s', utils.get_display_name(target))
        chunks_left = dumper.max_chunks

        entity_downloader = _EntityDownloader(
            self.client,
            dumper,
            photo_fmt=self.media_fmt if 'chatphoto' in self.types else None
        )
        if isinstance(target_in, (types.InputPeerChat, types.InputPeerChannel)):
            try:
                __log__.info('Getting participants...')
                participants = self.client.get_participants(target_in)
                added, removed = dumper.dump_participants_delta(
                    target_id, ids=[x.id for x in participants]
                )
                entity_downloader.extend_pending(
                    [p for p in participants if p.id in added or p.id in removed])
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

            # Get media needs access to the entities from this batch
            entities = {utils.get_peer_id(x): x for x in
                        itertools.chain(history.users, history.chats)}
            entities[target_id] = target

            # Queue users and chats for dumping
            entity_downloader.extend_pending(
                itertools.chain(history.users, history.chats)
            )
            # Since the flood waits we would get from spamming GetFullX and
            # GetHistory are the same and are independent of each other, we can
            # ignore the 'recommended' sleep from pop_pending and use the later
            # sleep (1 - time_taken) for both of these, halving time taken here
            entity_downloader.pop_pending()

            for m in history.messages:
                if isinstance(m, types.Message):
                    if self.check_media(m.media):
                        self.download_media(m, target_id, entities)

                    fwd_id = dumper.dump_forward(m.fwd_from)
                    media_id = dumper.dump_media(m.media)
                    dumper.dump_message(m, target_id,
                                        forward_id=fwd_id, media_id=media_id)

                elif isinstance(m, types.MessageService):
                    if isinstance(m.action, types.MessageActionChatEditPhoto):
                        media_id = dumper.dump_media(m.action.photo)
                        entity_downloader.download_profile_photo(
                            m.action.photo, target, known_id=m.id
                        )
                    else:
                        media_id = None
                    dumper.dump_message_service(m, target_id,
                                                media_id=media_id)
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
            'Done. Retrieving full information about %s missing entities.',
            len(entity_downloader)
        )
        while entity_downloader:
            start = time.time()
            needed_sleep = entity_downloader.pop_pending()
            dumper.commit()
            time.sleep(max(needed_sleep - (time.time() - start), 0))

        __log__.info('Dump with %s finished', utils.get_display_name(target))

    def save_admin_log(self, dumper, target_id):
        """
        Download and dumps the entire available admin log for the given
        channel. You must have permission to view the admin log for it.
        """
        target_in = self.client.get_input_entity(target_id)
        target = self.client.get_entity(target_in)
        target_id = utils.get_peer_id(target)
        req = functions.channels.GetAdminLogRequest(
            target_in, q='', min_id=0, max_id=0, limit=100
        )
        __log__.info('Starting admin log dump for %s',
                     utils.get_display_name(target))

        # TODO Resume admin log?
        # Rather silly considering logs only last up to two days and
        # there isn't much information in them (due to their short life).
        chunks_left = dumper.max_chunks
        entity_downloader = _EntityDownloader(
            self.client,
            dumper,
            photo_fmt=self.media_fmt if 'chatphoto' in self.types else None
        )
        while True:
            start = time.time()
            result = self.client(req)
            __log__.debug('Downloaded another chunk of the admin log.')
            entity_downloader.extend_pending(
                itertools.chain(result.users, result.chats)
            )
            entity_downloader.pop_pending()
            if not result.events:
                break

            for event in result.events:
                if isinstance(event.action,
                              types.ChannelAdminLogEventActionChangePhoto):
                    media_id1 = dumper.dump_media(event.action.new_photo)
                    media_id2 = dumper.dump_media(event.action.prev_photo)
                    entity_downloader.download_profile_photo(
                        event.action.new_photo, target, event.id
                    )
                    entity_downloader.download_profile_photo(
                        event.action.prev_photo, target, event.id
                    )
                else:
                    media_id1 = None
                    media_id2 = None
                dumper.dump_admin_log_event(event, target_id,
                                            media_id1=media_id1,
                                            media_id2=media_id2)

            req.max_id = min(e.id for e in result.events)
            time.sleep(max(1 - (time.time() - start), 0))
            chunks_left -= 1
            if chunks_left <= 0:
                break

        while entity_downloader:
            start = time.time()
            needed_sleep = entity_downloader.pop_pending()
            dumper.commit()
            time.sleep(max(needed_sleep - (time.time() - start), 0))

        __log__.info('Admin log from %s dumped',
                     utils.get_display_name(target))

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
            who = who.strip().split(':', 1)[0]  # Ignore anything after ':'
            if (not who.startswith('+') and who.isdigit()) or who.startswith('-'):
                yield self.client.get_input_entity(int(who))
            else:
                yield self.client.get_input_entity(who)
