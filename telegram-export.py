#!/usr/bin/env python3
import configparser
import difflib
import logging
import re
import argparse

from telethon import TelegramClient, utils
from telethon.tl.types import Channel

from dumper import Dumper
from downloader import Downloader

# TODO make log level a config option
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG)
logging.getLogger('telethon').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_config(filename):
    # Load from file
    config = configparser.ConfigParser()
    config.read(filename)

    # Convert minutes to seconds
    config['Dumper']['ForceNoChangeDumpAfter'] = str(
        config['Dumper'].getint('ForceNoChangeDumpAfter', 7200) * 60)

    # Convert size to bytes
    max_size = config['Downloader'].get('MaxSize') or '1MB'
    m = re.match(r'\s*(\d+(?:\.\d*)?)\s*([kmg]?b)?\s*', max_size, re.IGNORECASE)
    if not m:
        raise ValueError('Invalid file size given for MaxSize')

    max_size = int(float(m.group(1)) * {
        'B': 1024**0,
        'KB': 1024**1,
        'MB': 1024**2,
        'GB': 1024**3,
    }.get((m.group(2) or 'MB').upper()))
    config['Downloader']['MaxSize'] = str(max_size)
    return config


def parse_args():
    parser = argparse.ArgumentParser(description="export Telegram data")
    parser.add_argument('--list-dialogs', action='store_true',
                        help='list dialogs and exit')

    parser.add_argument('--search', type=str,
                        help='searches for a dialog via name/username/phone')

    parser.add_argument('--config-file', default='config.ini',
                        help='specify a config file. Default config.ini')
    return parser.parse_args()


def fmt_dialog(dialog):
    username = getattr(dialog.entity, 'username', None)
    username = '@' + username if username else '<no username>'
    return '{} | {} | {}'.format(
        utils.get_peer_id(dialog.entity), username, dialog.name
    )


def find_dialog(dialogs, query, top=5, threshold=0.5):
    seq = difflib.SequenceMatcher(b=query, autojunk=False)
    scores = []
    for dialog in dialogs:
        seq.set_seq1(dialog.name)
        name_score = seq.ratio()
        if getattr(dialog.entity, 'username', None):
            seq.set_seq1(dialog.entity.username)
            username_score = seq.ratio()
        else:
            username_score = 0
        if getattr(dialog.entity, 'phone', None):
            seq.set_seq1(dialog.entity.phone)
            phone_score = seq.ratio()
        else:
            phone_score = 0

        scores.append((dialog, max(name_score, username_score, phone_score)))
    scores.sort(key=lambda t: t[1], reverse=True)
    return tuple(score[0] for score in scores[:top] if score[1] > threshold)


def main():
    args = parse_args()
    config = load_config(args.config_file)
    client = TelegramClient(
        config['TelegramAPI']['SessionName'], config['TelegramAPI']['ApiId'], config['TelegramAPI']['ApiHash']
    ).start(config['TelegramAPI']['PhoneNumber'])

    if args.list_dialogs or args.search:
        dialogs = client.get_dialogs(limit=None)[::-1]  # Oldest to newest
        if args.list_dialogs:
            for dialog in dialogs:
                print(fmt_dialog(dialog))

        if args.search:
            print('Searching for "{}"...'.format(args.search))
            found = find_dialog(dialogs, args.search)
            if not found:
                print('Found no good results with "{}".'.format(args.search))
            elif len(found) == 1:
                print('Top match:', fmt_dialog(found[0]), sep='\n')
            else:
                print('Showing top {} matches:'.format(len(found)))
                for dialog in found:
                    print(fmt_dialog(dialog))

        return

    downloader = Downloader(client, config['Downloader'])
    dumper = Dumper(config['Dumper'])
    with dumper.conn:
        dumper.conn.execute(
                "INSERT INTO SelfInformation VALUES (?)",
                (client.get_me(input_peer=True).user_id,))
    config = config['TelegramAPI']
    cache_file = config['SessionName'] + '.tl'
    try:
        if 'Whitelist' in dumper.config:
            # Only whitelist, don't even get the dialogs
            entities = downloader.load_entities_from_str(
                dumper.config['Whitelist']
            )
            for who in entities:
                downloader.save_messages(dumper, who)

        elif 'Blacklist' in dumper.config:
            # May be blacklist, so save the IDs on who to avoid
            entities = downloader.load_entities_from_str(
                dumper.config['Blacklist']
            )
            avoid = set(utils.get_peer_id(x) for x in entities)
            for entity in downloader.fetch_dialogs(cache_file=cache_file):
                if utils.get_peer_id(entity) not in avoid:
                    downloader.save_messages(dumper, entity)
        else:
            # Neither blacklist nor whitelist - get all
            for entity in downloader.fetch_dialogs(cache_file=cache_file):
                downloader.save_messages(dumper, entity)

    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()


if __name__ == '__main__':
    main()
