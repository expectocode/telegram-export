#!/bin/env python3
import os
import json

from telethon import TelegramClient
from telethon.extensions import BinaryReader
from telethon.errors import SessionPasswordNeededError, UsernameNotOccupiedError
from json.decoder import JSONDecodeError
from getpass import getpass
from time import sleep

from telethon.tl.functions.messages import GetHistoryRequest


def save_messages(client, target):
    params = {
        'peer': target,
        'offset_id': 0,
        'offset_date': None,
        'add_offset': 0,
        'limit': 100,
        'max_id': 0,
        'min_id': 0
    }
    found = 0
    total_messages = 0
    while True:
        # TODO Actually save the messages, and the users
        # TODO Allow resuming
        # TODO How shold edits be handled? Always read first two days?
        history = client(GetHistoryRequest(**params))
        if not history.messages:
            break
        found += len(history.messages)
        total_messages = getattr(history, 'count', len(history.messages))
        params['offset_id'] = min(m.id for m in history.messages)
        params['offset_date'] = min(m.date for m in history.messages)
        print('Downloaded {}/{} ({:.1%})'.format(
            found, total_messages, found / total_messages
        ))
        sleep(0.5)  # TODO Smarter sleep, except FloodWait
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


if __name__ == '__main__':
    try:
        with open('client.conf') as f:
            conf = json.load(f)
    except (FileNotFoundError, JSONDecodeError, KeyError) as e:
        print('Failed to load configuration:', e)

    client = TelegramClient(conf['name'], conf['api_id'], conf['api_hash'])
    try:
        client.connect()
        if not client.is_user_authorized():
            client.sign_in(conf['phone'])
            try:
                client.sign_in(code=input('Enter code: '))
            except SessionPasswordNeededError:
                client.sign_in(password=getpass())

        for entity in fetch_dialogs(client):
            save_messages(client, entity)
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
