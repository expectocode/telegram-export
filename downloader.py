#!/bin/env python3
import logging
import os
from time import sleep

from telethon.extensions import BinaryReader
from telethon.tl import types as tl, functions as rpc
from telethon.utils import get_peer_id, resolve_id

__log__ = logging.getLogger(__name__)


def get_file_location(obj):
    if isinstance(obj, tl.Message):
        if obj.media:
            if isinstance(obj.media, tl.MessageMediaDocument):
                return get_file_location(obj.media.document)
            elif isinstance(obj.media, tl.MessageMediaPhoto):
                return get_file_location(obj.media.photo)

    elif isinstance(obj, tl.MessageService):
        if isinstance(obj.action, tl.MessageActionChatEditPhoto):
            return get_file_location(obj.action.photo)

    elif isinstance(obj, (tl.User, tl.Chat, tl.Channel)):
        return get_file_location(obj.photo)

    elif isinstance(obj, tl.Photo):  # PhotoEmpty are ignored
        # FileLocation or FileLocationUnavailable
        return obj.sizes[-1].location

    elif isinstance(obj, (tl.UserProfilePhoto, tl.ChatPhoto)):
        # FileLocation or FileLocationUnavailable
        # If the latter we could test whether obj.photo_small is more worthy
        return obj.photo_big

    elif isinstance(obj, tl.Document):  # DocumentEmpty are ignored
        return tl.InputDocumentFileLocation(
            id=obj.id,
            access_hash=obj.access_hash,
            version=obj.version
        )


def save_messages(client, dumper, target):
    target = client.get_input_entity(target)
    request = rpc.messages.GetHistoryRequest(
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

    target_id = get_peer_id(target)
    chunks_left = dumper.max_chunks

    # Resume from the last dumped message. It's important to
    # remember that we go -> 0, although it can be confusing.
    latest = dumper.get_last_dumped_message(target_id)
    if latest:
        __log__.info('Resuming at %s (%s)', latest.date, latest.id)
        # Offset is exclusive, which makes it easier
        request.offset_id = latest.id
        request.offset_date = latest.date

    # Stop as soon as we reach the highest ID we already have.
    # If we don't have such ID, we must reach the end or until
    # we don't receive any more messages.
    stop_at = getattr(dumper.get_message(target_id, 'MAX'), 'id', 0)

    found = dumper.get_message_count(target_id)
    entities = {}
    while True:
        # TODO How should edits be handled? Always read first two days?
        history = client(request)
        entities.update({get_peer_id(c): c for c in history.chats})
        entities.update({get_peer_id(u): u for u in history.users})

        for m in history.messages:
            media_id = dumper.dump_filelocation(get_file_location(m))

            if isinstance(m, tl.Message):
                fwd_id = dumper.dump_forward(m.fwd_from)
                dumper.dump_message(m, target_id,
                                    forward_id=fwd_id, media_id=media_id)

            elif isinstance(m, tl.MessageService):
                dumper.dump_message_service(m, media_id=media_id)

            else:
                __log__.warning('Skipping message %s', m)
                continue

        total_messages = getattr(history, 'count', len(history.messages))
        if history.messages:
            # We may reinsert some we already have (so found > total)
            found = min(found + len(history.messages), total_messages)
            request.offset_id = min(m.id for m in history.messages)
            request.offset_date = min(m.date for m in history.messages)

        __log__.debug('Downloaded {}/{} ({:.1%})'.format(
            found, total_messages, found / total_messages
        ))

        if len(history.messages) < request.limit:
            __log__.info('Received less messages than limit, done.')
            # Receiving less messages than the limit means we have reached
            # the end, so we need to exit. Next time we'll start from offset
            # 0 again so we can check for new messages.
            dumper.update_last_dumped_message(target_id, 0)
            break

        # We dump forward (message ID going towards 0), so as soon
        # as the minimum message ID (now in offset ID) is less than
        # the highest ID ("closest" bound we need to reach), stop.
        if request.offset_id <= stop_at:
            __log__.info('Already have the rest of messages, done.')
            dumper.update_last_dumped_message(target_id, 0)
            break

        # Keep track of the last target ID (smallest one),
        # so we can resume from here in case of interruption.
        dumper.update_last_dumped_message(target_id, request.offset_id)

        chunks_left -= 1  # 0 means infinite, will reach -1 and never 0
        if chunks_left == 0:
            __log__.info('Reached maximum amount of chunks, done.')
            break

        sleep(1)

    __log__.info('Done. Retrieving full information about entities.')
    # TODO Save their profile picture
    for mid, entity in entities.items():
        file_location = get_file_location(entity)
        if file_location:
            photo_id = dumper.dump_filelocation(file_location)
        else:
            photo_id = None

        eid, etype = resolve_id(mid)
        if etype == tl.PeerUser:
            if entity.deleted:
                continue
                # Otherwise, the empty first name causes an IntegrityError
            full_user = client(rpc.users.GetFullUserRequest(entity))
            sleep(1)
            dumper.dump_user(full_user, photo_id=photo_id)

        elif etype == tl.PeerChat:
            dumper.dump_chat(entity, photo_id=photo_id)

        elif etype == tl.PeerChannel:
            full_channel = client(rpc.channels.GetFullChannelRequest(entity))
            sleep(1)
            if entity.megagroup:
                dumper.dump_supergroup(full_channel, entity, photo_id=photo_id)
            else:
                dumper.dump_channel(full_channel.full_chat, entity,
                                    photo_id=photo_id)

    __log__.info('Dump with %s finished', target)


def fetch_dialogs(client, cache_file='dialogs.tl', force=False):
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
        entities = [d.entity for d in client.get_dialogs(limit=None)]
        for entity in entities:
            f.write(bytes(entity))

    return entities


def load_entities_from_str(client, string):
    for who in string.split(','):
        who = who.strip()
        if (not who.startswith('+') and who.isdigit()) or who.startswith('-'):
            yield client.get_input_entity(int(who))
        else:
            yield client.get_input_entity(who)
