#!/bin/env python3
import os
from getpass import getpass
from time import sleep
import configparser
import logging

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.extensions import BinaryReader
from telethon.tl import types as tl, functions as rpc
from telethon.utils import get_peer_id, resolve_id, get_display_name

from dumper import Dumper

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    request = rpc.messages.GetHistoryRequest(
        peer=target,
        offset_id=0,
        offset_date=None,
        add_offset=0,
        limit=100,
        max_id=0,
        min_id=0
    )
    print('Starting with', get_display_name(target))

    target_id = get_peer_id(target)

    # Resume from the last dumped message. It's important to
    # remember that we go -> 0, although it can be confusing.
    latest = dumper.get_last_dumped_message(target_id)
    if latest:
        print('Resuming at', latest.date, '(', latest.id, ')')
        # Offset is exclusive, which makes it easier
        request.offset_id = latest.id
        request.offset_date = latest.date

    # Stop as soon as we reach the highest ID we already have.
    # If we don't have such ID, we must reach the end or until
    # we don't receive any more messages.
    stop_at = getattr(dumper.get_message(target_id, 'MAX'), 'id', 0)

    # OR if the offset is SMALLER than which we should stop at, it
    # means we're AFTER that limit, which means we haven't finished
    # reaching the end. If this is the case, we should stop at 0.
    if latest and latest.id <= stop_at:
        stop_at = 0

    found = 0
    entities = {}
    while True:
        # TODO How should edits be handled? Always read first two days?
        history = client(request)
        entities.update({get_peer_id(c): c for c in history.chats})
        entities.update({get_peer_id(u): u for u in history.users})
        if not history.messages:
            # TODO Once we reach the end, restart looking for new.
            # If the first message we return is already in the database,
            # it means there's nothing new and we have fully finished.
            break

        for m in history.messages:
            file_location = get_file_location(m)
            if file_location:
                media_id = dumper.dump_filelocation(file_location)
            else:
                media_id = None

            if isinstance(m, tl.Message):
                m.to_id = get_peer_id(m.to_id)
                if m.fwd_from:
                    fwd_id = dumper.dump_forward(m.fwd_from)
                else:
                    fwd_id = None

                dumper.dump_message(m, forward_id=fwd_id, media_id=media_id)

            elif isinstance(m, tl.MessageService):
                m.to_id = get_peer_id(m.to_id)
                dumper.dump_message_service(m, media_id=media_id)

            else:
                print('Skipping message', type(m).__name__)
                continue

        found += len(history.messages)
        total_messages = getattr(history, 'count', len(history.messages))
        request.offset_id = min(m.id for m in history.messages)
        request.offset_date = min(m.date for m in history.messages)

        # Keep track of the last target ID (smallest one),
        # so we can resume from here in case of interruption.
        dumper.update_last_dumped_message(target_id, request.offset_id)

        # We dump forward (message ID going towards 0), so as soon
        # as the minimum message ID (now in offset ID) is less than
        # the highest ID ("closest" bound we need to reach), stop.
        if request.offset_id <= stop_at:
            print('Already have the rest of messages, done.')
            break

        print('Downloaded {}/{} ({:.1%})'.format(
            found, total_messages, found / total_messages
        ))
        sleep(1)

    print('Done. Retrieving full information about entities.')
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
                dumper.dump_channel(full_channel.full_chat, entity, photo_id=photo_id)
    print('Done!\n')


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

def load_config():
    # Load from file
    defaults = {'ForceNoChangeDumpAfter': 7200,'DBFileName': 'export'}
    config = configparser.ConfigParser(defaults)
    config.read('config.ini')

    # Convert minutes to seconds
    config['Dumper']['ForceNoChangeDumpAfter'] = str(
        config.getint('Dumper', 'ForceNoChangeDumpAfter') * 60)

    return config


if __name__ == '__main__':
    config = load_config()
    dumper = Dumper(config['Dumper'])
    config = config['TelegramAPI']

    client = TelegramClient(
        config['SessionName'], config['ApiId'], config['ApiHash']
    )
    try:
        client.connect()
        if not client.is_user_authorized():
            client.sign_in(config['PhoneNumber'])
            try:
                client.sign_in(code=input('Enter code: '))
            except SessionPasswordNeededError:
                client.sign_in(password=getpass())

        for entity in fetch_dialogs(client):
            save_messages(client, dumper, entity)
    except KeyboardInterrupt:
        pass
    finally:
        print('Done, disconnecting...')
        client.disconnect()
