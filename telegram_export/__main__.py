#!/usr/bin/env python3
"""The main telegram-export script.
Handles arguments and config, then calls the Exporter.
"""
import argparse
import asyncio
import configparser
import difflib
import logging
import os
import re
from contextlib import suppress

import tqdm
import appdirs
from telethon import TelegramClient, utils
from telegram_export.utils import parse_proxy_str

from telegram_export.dumper import Dumper
from telegram_export.exporter import Exporter
from telegram_export.formatters import NAME_TO_FORMATTER

logger = logging.getLogger('')  # Root logger


NO_USERNAME = '<no username>'


class TqdmLoggingHandler(logging.Handler):
    """Redirect all logging messages through tqdm.write()"""
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
    config_dir = appdirs.user_config_dir("telegram-export")

    if not filename:
        filename = os.path.join(config_dir, 'config.ini')

    if not os.path.isfile(filename):
        logger.warning("No config file! Make one in {} and find an example "
                       "config at https://github.com/expectocode/"
                       "telegram-export/blob/master/config.ini.example."
                       "Alternatively, use --config-file FILE".format(filename))
        exit(1)

    defaults = {
        'SessionName': 'exporter',
        'OutputDirectory': '.',
        'MediaWhitelist': 'chatphoto, photo, sticker',
        'MaxSize': '1MB',
        'LogLevel': 'INFO',
        'DBFileName': 'export',
        'InvalidationTime': '7200',
        'ChunkSize': '100',
        'MaxChunks': '0',
        'LibraryLogLevel': 'WARNING',
        'MediaFilenameFmt': 'usermedia/{name}-{context_id}/{type}-{filename}'
    }

    # Load from file
    config = configparser.ConfigParser(defaults)
    config.read(filename)

    # Check logging level (let it raise on invalid)
    level = config['Dumper'].get('LogLevel').upper()
    handler = TqdmLoggingHandler(level)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    handler.setLevel(getattr(logging, level))
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level))
    # Library loggers
    level = config['Dumper'].get('LibraryLogLevel').upper()
    telethon_logger = logging.getLogger('telethon')
    telethon_logger.setLevel(getattr(logging, level))
    telethon_logger.addHandler(handler)

    # Convert relative paths and paths with ~
    config['Dumper']['OutputDirectory'] = os.path.abspath(os.path.expanduser(
        config['Dumper']['OutputDirectory']))
    os.makedirs(config['Dumper']['OutputDirectory'], exist_ok=True)

    # Convert minutes to seconds
    config['Dumper']['InvalidationTime'] = str(
        config['Dumper'].getint('InvalidationTime', 7200) * 60)

    # Convert size to bytes
    max_size = config['Dumper'].get('MaxSize')
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
    parser = argparse.ArgumentParser(description="Download Telegram data (users, chats, messages, and media) into a database (and display the saved data)")
    parser.add_argument('--list-dialogs', action='store_true',
                        help='list dialogs and exit')

    parser.add_argument('--search-dialogs', type=str, dest='search_string',
                        help='like --list-dialogs but searches for a dialog '
                             'by name/username/phone')

    parser.add_argument('--config-file', default=None,
                        help='specify a config file. Default config.ini')
                        # This None is handled in read_config.

    parser.add_argument('--contexts', type=str,
                        help='list of contexts to act on eg --contexts=12345, '
                             '@username (see example config whitelist for '
                             'full rules). Overrides whitelist/blacklist. '
                             'The = is required when providing multiple values.')

    parser.add_argument('--format-contexts', type=int, nargs='+',
                        help='list of contexts to format eg --format-contexts='
                             '12345 -1006789. Only ContextIDs are accepted, '
                             'not usernames or phone numbers.')

    parser.add_argument('--format', type=str,
                        help='formats the dumped messages with the specified '
                             'formatter and exits. You probably want to use '
                             'this in conjunction with --format-contexts.',
                             choices=NAME_TO_FORMATTER)

    parser.add_argument('--download-past-media', action='store_true',
                        help='download past media instead of dumping '
                             'new data (files that were seen before '
                             'but not downloaded).')

    parser.add_argument('--proxy', type=str, dest='proxy_string',
                        help='set proxy string. '
                             'Examples: socks5://user:password@127.0.0.1:1080. '
                             'http://localhost:8080')
    return parser.parse_args()


def fmt_dialog(dialog, id_pad=0, username_pad=0):
    """
    Space-fill a row with given padding values
    to ensure alignment when printing dialogs.
    """
    username = getattr(dialog.entity, 'username', None)
    username = '@' + username if username else NO_USERNAME
    return '{:<{id_pad}} | {:<{username_pad}} | {}'.format(
        utils.get_peer_id(dialog.entity), username, dialog.name,
        id_pad=id_pad, username_pad=username_pad
    )


def find_fmt_dialog_padding(dialogs):
    """
    Find the correct amount of space padding
    to give dialogs when printing them.
    """
    no_username = NO_USERNAME[:-1]  # Account for the added '@' if username
    return (
        max(len(str(utils.get_peer_id(dialog.entity))) for dialog in dialogs),
        max(len(getattr(dialog.entity, 'username', no_username) or no_username)
            for dialog in dialogs) + 1
    )


def find_dialog(dialogs, query, top=25, threshold=0.7):
    """
    Iterate through dialogs and return, sorted,
    the best matches for a given query.
    """
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


async def list_or_search_dialogs(args, client):
    """List the user's dialogs and/or search them for a query"""
    dialogs = (await client.get_dialogs(limit=None))[::-1]  # Oldest to newest
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

    await client.disconnect()


async def main(loop):
    """
    The main telegram-export program. Goes through the
    configured dialogs and dumps them into the database.
    """
    args = parse_args()
    config = load_config(args.config_file)
    dumper = Dumper(config['Dumper'])

    if args.contexts:
        dumper.config['Whitelist'] = args.contexts

    if args.format:
        formatter = NAME_TO_FORMATTER[args.format](dumper.conn)
        fmt_contexts = args.format_contexts or formatter.iter_context_ids()
        for cid in fmt_contexts:
            formatter.format(cid, config['Dumper']['OutputDirectory'])
        return

    proxy = args.proxy_string or dumper.config.get('Proxy')
    if proxy:
        proxy = parse_proxy_str(proxy)

    absolute_session_name = os.path.join(
        config['Dumper']['OutputDirectory'],
        config['TelegramAPI']['SessionName']
    )
    if config.has_option('TelegramAPI', 'SecondFactorPassword'):
        client = await (TelegramClient(
                absolute_session_name,
                config['TelegramAPI']['ApiId'],
                config['TelegramAPI']['ApiHash'],
                loop=loop,
                proxy=proxy
            ).start(config['TelegramAPI']['PhoneNumber'], password=config['TelegramAPI']['SecondFactorPassword']))
    else:
        client = await (TelegramClient(
            absolute_session_name,
            config['TelegramAPI']['ApiId'],
            config['TelegramAPI']['ApiHash'],
            loop=loop,
            proxy=proxy
        ).start(config['TelegramAPI']['PhoneNumber']))

    if args.list_dialogs or args.search_string:
        return await list_or_search_dialogs(args, client)

    exporter = Exporter(client, config, dumper, loop)

    try:
        if args.download_past_media:
            await exporter.download_past_media()
        else:
            await exporter.start()
    except asyncio.CancelledError:
        # This should be triggered on KeyboardInterrupt's to prevent ugly
        # traceback from reaching the user. Important code that always
        # must run (such as the Downloader saving resume info) should go
        # in their respective `finally:` blocks to ensure it gets called.
        pass
    finally:
        await exporter.close()

    exporter.logger.info("Finished!")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        ret = loop.run_until_complete(main(loop)) or 0
    except KeyboardInterrupt:
        ret = 1
    for task in asyncio.Task.all_tasks():
        task.cancel()
        # Now we should await task to execute it's cancellation.
        # Cancelled task raises asyncio.CancelledError that we can suppress:
        if hasattr(task._coro, '__name__') and task._coro.__name__ == 'main':
            continue
        with suppress(asyncio.CancelledError):
            loop.run_until_complete(task)
    loop.stop()
    loop.close()
    exit(ret)
