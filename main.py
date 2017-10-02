#!/bin/env python3
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, UsernameNotOccupiedError
from json.decoder import JSONDecodeError
from getpass import getpass
from time import sleep
import json

from telethon.tl.functions.messages import GetHistoryRequest


def save_messages(client):
    try:
        target = client.get_entity(input("Enter target's username: "))
        if not target:
            raise UsernameNotOccupiedError()
    except UsernameNotOccupiedError:
        print('Nobody uses such username.')
        return

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

        save_messages(client)
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
