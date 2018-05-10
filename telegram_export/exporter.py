"""A class to iterate through dialogs and dump them, or save past media"""

import logging
import re

from async_generator import yield_, async_generator
from telethon import utils

from .downloader import Downloader


@async_generator
async def entities_from_str(client, string):
    """Helper function to load entities from the config file"""
    for who in string.split(','):
        if not who.strip():
            continue
        who = who.split(':', 1)[0].strip()  # Ignore anything after ':'
        if re.match(r'[^+]-?\d+', who):
            await yield_(await client.get_input_entity(int(who)))
        else:
            await yield_(await client.get_input_entity(who))


@async_generator
async def get_entities_iter(mode, in_list, client):
    """
    Get a generator of entities to act on given a mode ('blacklist',
    'whitelist') and an input from that mode. If whitelist, generator
    will be asynchronous.
    """
    # TODO change None to empty blacklist?
    mode = mode.lower()
    if mode == 'whitelist':
        assert client is not None
        async for ent in entities_from_str(client, in_list):
            await yield_(ent)
    if mode == 'blacklist':
        assert client is not None
        blacklist = entities_from_str(client, in_list)
        avoid = set()
        async for entity in blacklist:
            avoid.add(utils.get_peer_id(entity))
        # TODO Should this get_dialogs call be cached? How?
        for dialog in await client.get_dialogs(limit=None):
            if utils.get_peer_id(dialog.entity) not in avoid:
                await yield_(dialog.entity)
        return


class Exporter:
    """A class to iterate through dialogs and dump them, or save past media"""
    def __init__(self, client, config, dumper, loop):
        self.client = client
        self.dumper = dumper
        self.downloader = Downloader(client, config['Dumper'], dumper, loop)
        self.logger = logging.getLogger("exporter")

    def close(self):
        """Gracefully close the exporter"""
        # Downloader handles its own graceful exit
        self.logger.info("Closing exporter")
        self.client.disconnect()
        self.dumper.conn.close()

    async def start(self):
        """Perform a dump of the dialogs we've been told to act on"""
        self.logger.info("Saving to %s", self.dumper.config['OutputDirectory'])
        self.dumper.check_self_user((await self.client.get_me(input_peer=True)).user_id)
        if 'Whitelist' in self.dumper.config:
            # Only whitelist, don't even get the dialogs
            async for entity in get_entities_iter('whitelist',
                                                  self.dumper.config['Whitelist'],
                                                  self.client):
                await self.downloader.start(entity)
        elif 'Blacklist' in self.dumper.config:
            # May be blacklist, so save the IDs on who to avoid
            async for entity in get_entities_iter('blacklist',
                                                  self.dumper.config['Blacklist'],
                                                  self.client):
                await self.downloader.start(entity)
        else:
            # Neither blacklist nor whitelist - get all
            for dialog in await self.client.get_dialogs(limit=None):
                await self.downloader.start(dialog.entity)

    async def download_past_media(self):
        """
        Download past media (media we saw but didn't download before) of the
        dialogs we've been told to act on
        """
        self.logger.info("Saving to %s", self.dumper.config['OutputDirectory'])
        self.dumper.check_self_user((await self.client.get_me(input_peer=True)).user_id)

        if 'Whitelist' in self.dumper.config:
            # Only whitelist, don't even get the dialogs
            async for entity in get_entities_iter('whitelist',
                                                  self.dumper.config['Whitelist'],
                                                  self.client):
                await self.downloader.download_past_media(self.dumper, entity)
        elif 'Blacklist' in self.dumper.config:
            # May be blacklist, so save the IDs on who to avoid
            async for entity in get_entities_iter('blacklist',
                                                  self.dumper.config['Blacklist'],
                                                  self.client):
                await self.downloader.download_past_media(self.dumper, entity)
        else:
            # Neither blacklist nor whitelist - get all
            for dialog in await self.client.get_dialogs(limit=None):
                await self.downloader.download_past_media(self.dumper, dialog.entity)
