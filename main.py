import configparser
import logging
from getpass import getpass

from telethon import TelegramClient, utils
from telethon.errors import SessionPasswordNeededError

import downloader
from dumper import Dumper

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    cache_file = config['SessionName'] + '.tl'

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

        if 'Whitelist' in dumper.config:
            # Only whitelist, don't even get the dialogs
            entities = downloader.load_entities_from_str(client, dumper.config['Whitelist'])
            entities = client.get_entity(entities)  # Into full, to show name
            for who in entities:
                downloader.save_messages(client, dumper, who)
        else:
            # May be blacklist, so save the IDs on who to avoid
            entities = downloader.load_entities_from_str(client, dumper.config['Blacklist'])
            avoid = set(utils.get_peer_id(x) for x in entities)
            for entity in downloader.fetch_dialogs(client, cache_file=cache_file):
                if utils.get_peer_id(entity) not in avoid:
                    downloader.save_messages(client, dumper, entity)
    except KeyboardInterrupt:
        pass
    finally:
        print('Done, disconnecting...')
        client.disconnect()
