#!/bin/env python3
import logging
import os
import time
from collections import deque

from telethon import utils
from telethon.extensions import BinaryReader
from telethon.tl import types, functions

__log__ = logging.getLogger(__name__)


VALID_TYPES = {
    'photo', 'document', 'video', 'audio', 'sticker', 'voice'
}


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

        req.offset_id, req.offset_date, stop_at = dumper.get_resume(target_id)
        if req.offset_id:
            __log__.info('Resuming at %s (%s)', req.offset_date, req.offset_id)

        found = dumper.get_message_count(target_id)

        pending_entities = deque()
        pending_entity_ids = set()
        dumped_entity_ids = set()
        while True:
            # TODO How should edits be handled? Always read first two days?
            start = time.time()
            history = self.client(req)

            # Queue users and chats for dumping
            for user in history.users:
                if not isinstance(user, types.User):
                    # Ignore UserEmpty
                    continue
                i = utils.get_peer_id(user)
                if i not in dumped_entity_ids and not i in pending_entity_ids:
                    pending_entity_ids.add(i)
                    pending_entities.append(user)

            for chat in history.chats:
                if not isinstance(chat, (types.Chat, types.Channel)):
                    # Ignore ChatEmpty, ChatForbidden and ChannelForbidden
                    continue
                if isinstance(chat, types.Chat):
                    photo_id = dumper.dump_media(chat.photo)
                    dumper.dump_chat(chat, photo_id=photo_id)
                    continue
                i = utils.get_peer_id(chat)
                if i not in dumped_entity_ids and not i in pending_entity_ids:
                    pending_entity_ids.add(i)
                    pending_entities.append(chat)

            # Since the flood waits to get full and get history are the same,
            # we can interlace them to "double" the speed (are independent).
            if pending_entities:
                ent = pending_entities.popleft()
                i = utils.get_peer_id(ent)
                pending_entity_ids.remove(i)

                __log__.debug('Dumping entity %s', utils.get_display_name(ent))
                if isinstance(ent, types.User) and not (ent.deleted or ent.min):
                    # Otherwise, empty first name causes an IntegrityError
                    full_user = self.client(
                        functions.users.GetFullUserRequest(ent))
                    photo_id = dumper.dump_media(full_user.profile_photo)
                    dumper.dump_user(full_user, photo_id=photo_id)

                elif isinstance(ent, types.Channel):
                    full = self.client(
                        functions.channels.GetFullChannelRequest(ent))
                    # TODO Maybe just pass messages.ChatFull to dumper...
                    photo_id = dumper.dump_media(full.full_chat.chat_photo)
                    if ent.megagroup:
                        dumper.dump_supergroup(full.full_chat, ent, photo_id)
                    else:
                        dumper.dump_channel(full.full_chat, ent, photo_id)

                dumped_entity_ids.add(i)

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
                max_msg = dumper.get_message(target_id, 'MAX')
                dumper.save_resume(target_id, stop_at=max_msg.id)
                break

            # We dump forward (message ID going towards 0), so as soon
            # as the minimum message ID (now in offset ID) is less than
            # the highest ID ("closest" bound we need to reach), stop.
            if req.offset_id <= stop_at:
                __log__.info('Reached already-dumped messages, done.')
                max_msg = dumper.get_message(target_id, 'MAX')
                dumper.save_resume(target_id, stop_at=max_msg.id)
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
            .format(len(pending_entities))
        )
        # TODO Save their profile picture
        while pending_entities:
            start = time.time()
            entity = pending_entities.popleft()
            __log__.debug('Dumping entity %s', utils.get_display_name(entity))
            if isinstance(entity, types.User) and not (entity.deleted or entity.min):
                full_user = self.client(functions.users.GetFullUserRequest(entity))
                photo_id = dumper.dump_media(full_user.profile_photo)
                dumper.dump_user(full_user, photo_id=photo_id)

            elif isinstance(entity, types.Channel):
                full = self.client(functions.channels.GetFullChannelRequest(entity))
                photo_id = dumper.dump_media(full.full_chat.chat_photo)
                if entity.megagroup:
                    dumper.dump_supergroup(full.full_chat, entity, photo_id=photo_id)
                else:
                    dumper.dump_channel(full.full_chat, entity, photo_id=photo_id)

            else:
                # Note that Chats are always dumped inmediatly as
                # there's no relevant "full" information about them.
                __log__.info('Ignoring entity %s', entity)

            dumper.commit()
            if start:
                # 30 request in 30 seconds (sleep a second *between* requests).
                # Not optimal since get full user/channel are independent,
                # but the users and channels are saved ordered.
                time.sleep(max(1 - (time.time() - start), 0))

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
