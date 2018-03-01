#!/usr/bin/env python3
"""The main telegram-export program"""
import configparser
import difflib
import logging
import re
import argparse
import os

import sys
from telethon import TelegramClient, utils
import tqdm

from dumper import Dumper
from downloader import Downloader
from formatters import NAME_TO_FORMATTER

logger = logging.getLogger('')  # Root logger


NO_USERNAME = '<no username>'
SCRIPT_DIR = os.path.dirname(__file__)


class TqdmLoggingHandler (logging.Handler):
    def __init__ (self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def load_config(filename):
    """Load config from the specified file and return the parsed config"""
    # Get a path to the file. If it was specified, it should be fine.
    # If it was not specified, assume it's config.ini in the script's dir.
    if not filename:
        filename = os.path.join(SCRIPT_DIR, 'config.ini')
    # Load from file
    config = configparser.ConfigParser()
    config.read(filename)

    # Check logging level (let it raise on invalid)
    level = (config['Dumper'].get('LogLevel') or 'DEBUG').upper()
    handler = TqdmLoggingHandler(level)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    handler.setLevel(getattr(logging, level))
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level))
    # Library loggers
    level = (config['Dumper'].get('LibraryLogLevel') or 'WARNING').upper()
    telethon_logger = logging.getLogger('telethon')
    telethon_logger.setLevel(getattr(logging, level))
    telethon_logger.addHandler(handler)
    # Convert default output dir '.' to script dir
    if config['Dumper']['OutputDirectory'] == '.':
        config['Dumper']['OutputDirectory'] = SCRIPT_DIR
    os.makedirs(config['Dumper']['OutputDirectory'], exist_ok=True)

    # Ensure that we have a format for the media filename
    if 'MediaFilenameFmt' not in config['Dumper']:
        config['Dumper']['MediaFilenameFmt'] = \
            'usermedia/{name}/{type}/{filename}{ext}'

    # Convert minutes to seconds
    config['Dumper']['ForceNoChangeDumpAfter'] = str(
        config['Dumper'].getint('ForceNoChangeDumpAfter', 7200) * 60)

    # Convert size to bytes
    max_size = config['Dumper'].get('MaxSize') or '1MB'
    m = re.match(r'(\d+(?:\.\d*)?)\s*([kmg]?b)?', max_size, re.IGNORECASE)
    if not m:
        raise ValueError('Invalid file size given for MaxSize')

    max_size = int(float(m.group(1)) * {
        'B': 1024**0,
        'KB': 1024**1,
        'MB': 1024**2,
        'GB': 1024**3,
    }.get((m.group(2) or 'MB').upper()))
    config['Dumper']['MaxSize'] = str(max_size)
    return config


def parse_args():
    """Parse command-line arguments to the script"""
    parser = argparse.ArgumentParser(description="export Telegram data")
    parser.add_argument('--list-dialogs', action='store_true',
                        help='list dialogs and exit')

    parser.add_argument('--search-dialogs', type=str, dest='search_string',
                        help='like --list-dialogs but searches for a dialog '
                             'by name/username/phone')

    parser.add_argument('--config-file', default=None,
                        help='specify a config file. Default config.ini')

    # TODO Selectable context ID? They're hard to remember though.
    #      Would it be a different argument? Format but comma separated?
    parser.add_argument('--format', type=str,
                        help='formats the dumped messages with the specified '
                             'formatter and exits. Valid options are: {}'
                        .format(', '.join(NAME_TO_FORMATTER)))

    parser.add_argument('--download-past-media', type=int,
                        help='downloads past media (i.e. dumped files but'
                             'not downloaded) from the given context ID')
    return parser.parse_args()


def fmt_dialog(dialog, id_pad=0, username_pad=0):
    """
    Space-fill a row with given padding values to ensure alignment when printing dialogs
    """
    username = getattr(dialog.entity, 'username', None)
    username = '@' + username if username else NO_USERNAME
    return '{:<{id_pad}} | {:<{username_pad}} | {}'.format(
        utils.get_peer_id(dialog.entity), username, dialog.name,
        id_pad=id_pad, username_pad=username_pad
    )


def find_fmt_dialog_padding(dialogs):
    """Find the correct amount of space padding to give dialogs when printing them"""
    no_username = NO_USERNAME[:-1]  # Account for the added '@' if username
    return (
        max(len(str(utils.get_peer_id(dialog.entity))) for dialog in dialogs),
        max(len(getattr(dialog.entity, 'username', no_username) or no_username)
            for dialog in dialogs) + 1
    )


def find_dialog(dialogs, query, top=25, threshold=0.7):
    """Iterate through dialogs and return, sorted, the best matches for a given query"""
    seq = difflib.SequenceMatcher(b=query, autojunk=False)
    scores = []
    for index, dialog in enumerate(dialogs):
        seq.set_seq1(dialog.name)
        name_score = seq.ratio()
        if query.lower() in dialog.name.lower():
            # If query is a substring of the name, make it a good match.
            # Slightly boost dialogs which were recently active, so not
            # all substring-matched dialogs have exactly the same score.
            boost = (index/len(dialogs))/25
            name_score = max(name_score, 0.75 + boost)
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
    matches = tuple(score[0] for score in scores if score[1] > threshold)
    num_not_shown = 0 if len(matches) <= top else len(matches) - top
    return matches[:top], num_not_shown


def list_or_search_dialogs(args, client):
    """List the user's dialogs and/or search them for a query"""
    dialogs = client.get_dialogs(limit=None)[::-1]  # Oldest to newest
    if args.list_dialogs:
        id_pad, username_pad = find_fmt_dialog_padding(dialogs)
        for dialog in dialogs:
            print(fmt_dialog(dialog, id_pad, username_pad))

    if args.search_string:
        print('Searching for "{}"...'.format(args.search_string))
        found, num_not_shown = find_dialog(dialogs, args.search_string)
        if not found:
            print('Found no good results with "{}".'.format(args.search_string))
        elif len(found) == 1:
            print('Top match:', fmt_dialog(found[0]), sep='\n')
        else:
            if num_not_shown > 0:
                print('Showing top {} matches of {}:'.format(
                    len(found), len(found) + num_not_shown))
            else:
                print('Showing top {} matches:'.format(len(found)))
            id_pad, username_pad = find_fmt_dialog_padding(found)
            for dialog in found:
                print(fmt_dialog(dialog, id_pad, username_pad))

    client.disconnect()


def main():
    """The main telegram-export program.
       Goes through the configured dialogs and dumps them into the database"""
    args = parse_args()
    config = load_config(args.config_file)
    dumper = Dumper(config['Dumper'])

    if args.format:
        if args.format not in NAME_TO_FORMATTER:
            print('Format name "{}" not available"'.format(args.format),
                  file=sys.stderr)
            return 1

        formatter = NAME_TO_FORMATTER[args.format](dumper.conn)
        for cid in formatter.iter_context_ids():
            formatter.format(cid, config['Dumper']['OutputDirectory'])
        return

    absolute_session_name = os.path.join(
        config['Dumper']['OutputDirectory'],
        config['TelegramAPI']['SessionName']
    )
    client = TelegramClient(
        absolute_session_name,
        config['TelegramAPI']['ApiId'],
        config['TelegramAPI']['ApiHash']
    ).start(config['TelegramAPI']['PhoneNumber'])

    if args.list_dialogs or args.search_string:
        return list_or_search_dialogs(args, client)

    downloader = Downloader(client, config['Dumper'])
    cache_file = os.path.join(absolute_session_name + '.tl')
    try:
        if args.download_past_media:
            downloader.download_past_media(dumper, args.download_past_media)
            return

        dumper.check_self_user(client.get_me(input_peer=True).user_id)
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
    exit(main() or 0)
