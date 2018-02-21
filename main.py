import configparser
import logging
import re

from telethon import TelegramClient, utils

import downloader
from dumper import Dumper

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG)
logging.getLogger('telethon').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_config():
    # Load from file
    config = configparser.ConfigParser()
    config.read('config.ini')

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


def main():
    config = load_config()
    dumper = Dumper(config['Dumper'])
    dl_config = config['Downloader']
    config = config['TelegramAPI']
    cache_file = config['SessionName'] + '.tl'

    client = TelegramClient(
        config['SessionName'], config['ApiId'], config['ApiHash']
    ).start(config['PhoneNumber'])
    try:
        if 'Whitelist' in dumper.config:
            # Only whitelist, don't even get the dialogs
            entities = downloader.load_entities_from_str(
                client, dumper.config['Whitelist']
            )
            for who in entities:
                downloader.save_messages(client, dumper, who,
                                         config=dl_config)

        elif 'Blacklist' in dumper.config:
            # May be blacklist, so save the IDs on who to avoid
            entities = downloader.load_entities_from_str(
                client, dumper.config['Blacklist']
            )
            avoid = set(utils.get_peer_id(x) for x in entities)
            for entity in downloader.fetch_dialogs(client, cache_file=cache_file):
                if utils.get_peer_id(entity) not in avoid:
                    downloader.save_messages(client, dumper, entity,
                                             config=dl_config)
        else:
            # Neither blacklist nor whitelist - get all
            for entity in downloader.fetch_dialogs(client):
                downloader.save_messages(client, dumper, entity,
                                         config=dl_config)

    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()


if __name__ == '__main__':
    main()
