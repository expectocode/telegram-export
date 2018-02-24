#!/usr/bin/env python3
import configparser
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
    parser.add_argument('--config-file', default='config.ini',
                        help='specify a config file. Default config.ini')
    return parser.parse_args()


def print_dialogs(client):
    for dialog in client.get_dialogs(limit=None)[::-1]:  # Oldest to newest
        ent = dialog.entity
        try:
            username = '@' + ent.username
        except (AttributeError, TypeError):  # If no username or it is None
            username = '<no username>'
        if isinstance(ent, Channel):
            contextid = '-100{}'.format(ent.id)
        else:
            contextid = ent.id
        print('{} | {} | {}'.format(contextid, username, dialog.name))


def main():
    args = parse_args()
    config = load_config(args.config_file)
    client = TelegramClient(
        config['TelegramAPI']['SessionName'], config['TelegramAPI']['ApiId'], config['TelegramAPI']['ApiHash']
    ).start(config['TelegramAPI']['PhoneNumber'])
    if args.list_dialogs:
        print_dialogs(client)
        return
    downloader = Downloader(client, config['Downloader'])
    dumper = Dumper(config['Dumper'])
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
