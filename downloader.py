#!/bin/env python3
import logging
import os
from time import sleep

from telethon.extensions import BinaryReader
from telethon.tl import types as tl, functions as rpc
from telethon.utils import get_peer_id, resolve_id

__log__ = logging.getLogger(__name__)


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
    if stop_at > request.offset_id:
        # 987654321
        # stop ^ ^
        # resume |
        # If we resume after the maximum ID  we have we need to reach the end.
        stop_at = 0

    found = dumper.get_message_count(target_id)
    entities = {}
    while True:
        # TODO How should edits be handled? Always read first two days?
        history = client(request)
        entities.update({get_peer_id(c): c for c in history.chats})
        entities.update({get_peer_id(u): u for u in history.users})

        for m in history.messages:
            if isinstance(m, tl.Message):
                fwd_id = dumper.dump_forward(m.fwd_from)
                media_id = dumper.dump_media(m.media)
                dumper.dump_message(m, target_id,
                                    forward_id=fwd_id, media_id=media_id)

            elif isinstance(m, tl.MessageService):
                dumper.dump_message_service(m, media_id=None)

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
        eid, etype = resolve_id(mid)
        if etype == tl.PeerUser:
            if entity.deleted or entity.min:
                continue
                # Otherwise, the empty first name causes an IntegrityError
            full_user = client(rpc.users.GetFullUserRequest(entity))
            sleep(1)
            photo_id = dumper.dump_media(full_user.profile_photo)
            dumper.dump_user(full_user, photo_id=photo_id)

        elif etype == tl.PeerChat:
            if isinstance(entity, tl.Chat):
                photo_id = dumper.dump_media(entity.photo)
            else:
                photo_id = None
            dumper.dump_chat(entity, photo_id=photo_id)

        elif etype == tl.PeerChannel:
            if hasattr(entity, 'left') and entity.left:
                continue
            full_channel = client(rpc.channels.GetFullChannelRequest(entity))
            sleep(1)
            photo_id = dumper.dump_media(full_channel.chat_photo)
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
