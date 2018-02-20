#!/bin/env python3
import logging
import os
from time import sleep

from telethon.extensions import BinaryReader
from telethon.tl import types, functions
from telethon.utils import get_peer_id, resolve_id

__log__ = logging.getLogger(__name__)


def download_media(client, msg, target_id):
    if isinstance(msg, types.Message):
        media = msg.media
    else:
        media = msg
    os.makedirs('usermedia', exist_ok=True)
    file_name_prefix = 'usermedia/{}-{}-'.format(target_id, msg.id)
    if isinstance(media, types.MessageMediaDocument) and not hasattr(media.document, 'stickerset'):
        file_name = file_name_prefix + next(
            a for a in media.document.attributes
            if isinstance(a, types.DocumentAttributeFilename)
        ).file_name
        return client.download_media(media, file=file_name)
    elif isinstance(media, types.MessageMediaPhoto):
        file_name = file_name_prefix + media.photo.date.strftime('photo_%Y-%m-%d_%H-%M-%S.jpg')
        return client.download_media(media, file=file_name)
    else:
        return None


def save_messages(client, dumper, target):
    target = client.get_input_entity(target)
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
        target_id = client.get_me().id
    else:
        target_id = get_peer_id(target)

    req.offset_id, req.offset_date, stop_at = dumper.get_resume(target_id)
    if req.offset_id:
        __log__.info('Resuming at %s (%s)', req.offset_date, req.offset_id)

    found = dumper.get_message_count(target_id)
    entities = {}
    while True:
        # TODO How should edits be handled? Always read first two days?
        history = client(req)
        entities.update({get_peer_id(c): c for c in history.chats})
        entities.update({get_peer_id(u): u for u in history.users})

        for m in history.messages:
            if isinstance(m, types.Message):
                if m.media:
                    download_media(client, msg=m, target_id=target_id)
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

        sleep(1)
        dumper.commit()
    dumper.commit()

    __log__.info('Done. Retrieving full information about entities.')
    # TODO Save their profile picture
    for mid, entity in entities.items():
        eid, etype = resolve_id(mid)
        if etype == types.PeerUser:
            if entity.deleted or entity.min:
                continue
                # Otherwise, the empty first name causes an IntegrityError
            full_user = client(functions.users.GetFullUserRequest(entity))
            sleep(1)
            photo_id = dumper.dump_media(full_user.profile_photo)
            dumper.dump_user(full_user, photo_id=photo_id)

        elif etype == types.PeerChat:
            if isinstance(entity, types.Chat):
                photo_id = dumper.dump_media(entity.photo)
            else:
                photo_id = None
            dumper.dump_chat(entity, photo_id=photo_id)

        elif etype == types.PeerChannel:
            if hasattr(entity, 'left') and entity.left:
                continue
            full = client(functions.channels.GetFullChannelRequest(entity))
            assert isinstance(full, types.messages.ChatFull)
            sleep(1)
            photo_id = dumper.dump_media(full.full_chat.chat_photo)
            if entity.megagroup:
                dumper.dump_supergroup(full, entity, photo_id=photo_id)
            else:
                dumper.dump_channel(full, entity, photo_id=photo_id)

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
