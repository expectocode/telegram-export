#!/bin/env python3
import os
from getpass import getpass
from time import sleep
import configparser

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.extensions import BinaryReader
from telethon.tl import types as tl, functions as rpc
from telethon.utils import get_peer_id

from dumper import Dumper


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

    found = 0
    while True:
        # TODO Actually save the the users
        # TODO Allow resuming
        # TODO How should edits be handled? Always read first two days?
        history = client(request)
        if not history.messages:
            break

        for m in history.messages:
            fwd_id = None
            if isinstance(m, tl.Message):
                m.to_id = get_peer_id(m.to_id, add_mark=True)
                if m.fwd_from:
                    fwd_id = dumper.dump_forward(m.fwd_from)
            elif isinstance(m, tl.MessageService):
                m.to_id = get_peer_id(m.to_id, add_mark=True)
                continue  # TODO Don't skip messageService's
            else:
                print('Skipping message', type(m).__name__)
                continue

            # TODO Handle media
            dumper.dump_message(m, forward_id=fwd_id, media_id=None)

        found += len(history.messages)
        total_messages = getattr(history, 'count', len(history.messages))
        request.offset_id = min(m.id for m in history.messages)
        request.offset_date = min(m.date for m in history.messages)
        print('Downloaded {}/{} ({:.1%})'.format(
            found, total_messages, found / total_messages
        ))
        sleep(1)
    print('Done.')


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
        entities = client.get_dialogs(limit=None)[1]
        for entity in entities:
            f.write(bytes(entity))

    return entities

def load_config():
    # Load from file
    defaults = {'ForceNoChangeDumpAfter':7200,'DBFileName':'export'}
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
