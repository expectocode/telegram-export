#!/usr/bin/env python3
"""A module for dumping export data into the database"""
import json
import logging
import sqlite3
import time
from base64 import b64encode
from datetime import datetime
from enum import Enum

from telethon.tl import types as tl
from telethon.utils import resolve_id

logger = logging.getLogger(__name__)

DB_VERSION = 1  # database version


class InputFileType(Enum):
    NORMAL = 0
    DOCUMENT = 1


def sanitize_dict(dictionary):
    """
    Sanitizes a dictionary, encoding all bytes as
    Base64 so that it can be serialized as JSON.

    Assumes that there are no containers with bytes inside,
    and that the dictionary doesn't contain self-references.
    """
    for k, v in dictionary.items():
        if isinstance(v, bytes):
            dictionary[k] = str(b64encode(v), encoding='ascii')
        elif isinstance(v, datetime):
            dictionary[k] = v.timestamp()
        elif isinstance(v, dict):
            sanitize_dict(v)
        elif isinstance(v, list):
            for d in v:
                if isinstance(d, dict):
                    sanitize_dict(d)


class Dumper:
    """Class to interface with the database for exports"""

    def __init__(self, config):
        """Initialise the dumper.
        `config` should be a dict-like object with key "DBFileName"
        """
        self.config = config
        if 'DBFileName' in self.config:
            if self.config["DBFileName"] == ':memory:':
                self.conn = sqlite3.connect(':memory:')
            else:
                self.conn = sqlite3.connect('{}.db'.format(self.config['DBFileName']))
        else:
            logger.error("A database filename is required!")
            exit()
        c = self.conn.cursor()

        self.chunk_size = max(int(config.get('ChunkSize', 100)), 1)
        self.max_chunks = max(int(config.get('MaxChunks', 0)), 0)
        self.force_no_change_dump_after = \
            max(int(config.get('ForceNoChangeDumpAfter')), -1)

        c.execute("SELECT name FROM sqlite_master "
                         "WHERE type='table' AND name='Version'")

        if c.fetchone():
            # Tables already exist, check for the version
            c.execute("SELECT Version FROM Version")
            version = c.fetchone()[0]
            if version != DB_VERSION:
                self._upgrade_database(old=version)
                self.conn.commit()
        else:
            # Tables don't exist, create new ones
            c.execute("CREATE TABLE Version (Version INTEGER)")
            c.execute("INSERT INTO Version VALUES (?)", (DB_VERSION,))

            c.execute("CREATE TABLE Forward("
                      "ID INTEGER PRIMARY KEY AUTOINCREMENT,"
                      "OriginalDate INT NOT NULL,"
                      "FromID INT,"  # User or Channel ID
                      "ChannelPost INT,"
                      "PostAuthor TEXT)")

            # For InputFileLocation:
            #   local_id -> LocalID
            #   volume_id -> VolumeID
            #   secret -> Secret
            #
            # For InputDocumentFileLocation:
            #   id -> LocalID
            #   access_hash -> Secret
            #   version -> VolumeID
            c.execute("CREATE TABLE Media("
                      "ID INTEGER PRIMARY KEY AUTOINCREMENT,"
                      # Basic useful information, if available
                      "Name TEXT,"
                      "MimeType TEXT,"
                      "Size INT,"
                      "ThumbnailID INT,"
                      "Type TEXT,"
                      # Fields required to download the file
                      "LocalID INT,"
                      "VolumeID INT,"
                      "Secret INT,"
                      # Whatever else as JSON here
                      "Extra TEXT,"
                      "FOREIGN KEY (ThumbnailID) REFERENCES Media(ID))")

            c.execute("CREATE TABLE User("
                      "ID INT NOT NULL,"
                      "DateUpdated INT NOT NULL,"
                      "FirstName TEXT NOT NULL,"
                      "LastName TEXT,"
                      "Username TEXT,"
                      "Phone TEXT,"
                      "Bio TEXT,"
                      "Bot INTEGER,"
                      "CommonChatsCount INT NOT NULL,"
                      "PictureID INT,"
                      "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                      "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            c.execute("CREATE TABLE Channel("
                      "ID INT NOT NULL,"
                      "DateUpdated INT NOT NULL,"
                      # "CreatorID INT,"
                      "About TEXT,"
                      # "Signatures INT,"
                      "Title TEXT NOT NULL,"
                      "Username TEXT,"
                      "PictureID INT,"
                      "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                      "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            c.execute("CREATE TABLE Supergroup("
                      "ID INT NOT NULL,"
                      "DateUpdated INT NOT NULL,"
                      # "CreatorID INT,"
                      "About TEXT,"
                      "Title TEXT NOT NULL,"
                      "Username TEXT,"
                      "PictureID INT,"
                      "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                      "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            c.execute("CREATE TABLE Chat("
                       "ID INT NOT NULL,"
                       "DateUpdated INT NOT NULL,"
                       # "CreatorID INT,"
                       "Title TEXT NOT NULL,"
                       "MigratedToID INT,"
                       "PictureID INT,"
                       "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                       "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            c.execute("CREATE TABLE Message("
                      "ID INT NOT NULL,"
                      "ContextID INT NOT NULL,"
                      "Date INT NOT NULL,"
                      "FromID INT,"
                      "Message TEXT,"
                      "ReplyMessageID INT,"
                      "ForwardID INT,"
                      "PostAuthor TEXT,"
                      "ViewCount INT,"
                      "MediaID INT,"
                      "FOREIGN KEY (ForwardID) REFERENCES Forward(ID),"
                      "FOREIGN KEY (MediaID) REFERENCES Media(ID),"
                      "PRIMARY KEY (ID, ContextID)) WITHOUT ROWID")

            c.execute("CREATE TABLE LastMessage("
                       "ContextID INT NOT NULL,"
                       "ID INT NOT NULL,"
                       "PRIMARY KEY (ContextID)) WITHOUT ROWID")
            self.conn.commit()

    def _upgrade_database(self, old):
        """
        This method knows how to migrate from old -> DB_VERSION.

        Currently it performs no operation because this is the
        first version of the tables, in the future it should alter
        tables or somehow transfer the data between what canged.
        """

    def dump_message(self, message, context_id, forward_id, media_id):
        # TODO handle edits/deletes (fundamental problems with non-long-running exporter)
        """Dump a Message into the Message table
        The caller is responsible for ensuring to_id is a unique and correct contextID
        Params:
        - Message to dump,
        - ID of the chat dumping,
        - ID of Forward in the DB (or None),
        - ID of message Media in the DB (or None)
        Returns: -"""
        if not message.message and message.media:
            message.message = getattr(message.media, 'caption', '')

        return self._insert('Message',
                            (message.id,
                             context_id,
                             message.date.timestamp(),
                             message.from_id,
                             message.message,
                             message.reply_to_msg_id,
                             forward_id,
                             message.post_author,
                             message.views,
                             media_id)
                            )

    def dump_message_service(self, message, media_id):
        """Dump a MessageService into the ??? table"""
        # ddg.gg/%68%61%68%61%20%79%65%73?ia=images

    def dump_user(self, user_full, photo_id):
        # TODO: Use invalidation time
        """Dump a UserFull into the User table
        Params: UserFull to dump, MediaID of the profile photo in the DB
        Returns -, or False if not added"""
        # Rationale for UserFull rather than User is to get bio
        timestamp = round(time.time())
        values = (user_full.user.id,
                  timestamp,
                  user_full.user.first_name,
                  user_full.user.last_name,
                  user_full.user.username,
                  user_full.user.phone,
                  user_full.about,
                  user_full.user.bot,
                  user_full.common_chats_count,
                  photo_id)

        last = self.conn.execute(
            'SELECT * FROM User ORDER BY DateUpdated DESC').fetchone()
        if (self.rows_are_same(values, last, ignore_column=1)
                and values[1] - last[1] < int(self.force_no_change_dump_after)):
            return False

        return self._insert('User', values)

    def dump_channel(self, channel_full, channel, photo_id):
        # TODO: Use invalidation time
        """Dump a Channel into the Channel table
        Params: ChannelFull, Channel to dump, MediaID of the profile photo in the DB
        Returns -"""
        # Need to get the full object too for 'about' info
        timestamp = round(time.time())
        return self._insert('Channel',
                            (channel.id,
                             timestamp,
                             channel_full.about,
                             channel.title,
                             channel.username,
                             photo_id)
                            )

    def dump_supergroup(self, supergroup_full, supergroup, photo_id):
        # TODO: Use invalidation time
        """Dump a Supergroup into the Supergroup table
        Params: ChannelFull, Channel to dump, MediaID of the profile photo in the DB
        Returns -"""
        # Need to get the full object too for 'about' info
        timestamp = round(time.time())
        return self._insert('Supergroup',
                            (supergroup.id,
                             timestamp,
                             supergroup_full.about if hasattr(supergroup_full, 'about') else '',
                             supergroup.title,
                             supergroup.username,
                             photo_id)
                            )

    def dump_chat(self, chat, photo_id):
        # TODO: Use invalidation time
        """Dump a Chat into the Chat table
        Params: Chat to dump, MediaID of the profile photo in the DB
        Returns -"""
        timestamp = round(time.time())
        return self._insert('Chat',
                            (chat.id,
                             timestamp,
                             chat.title,
                             chat.migrated_to,
                             photo_id)
                            )

    def dump_media(self, media, media_type=None):
        """Dump a MessageMedia into the Media table
        Params: media Telethon object
        Returns: ID of inserted row"""
        if not media:
            return

        row = {x: None for x in (
            'name', 'mime_type', 'size', 'thumbnail_id',
            'local_id', 'volume_id', 'secret'
        )}
        row['type'] = media_type
        row['extra'] = media.to_dict()
        sanitize_dict(row['extra'])
        row['extra'] = json.dumps(row['extra'])

        if isinstance(media, tl.MessageMediaContact):
            row['type'] = 'contact'
            row['name'] = '{} {}'.format(media.first_name, media.last_name)
            row['local_id'] = media.user_id
            try:
                row['secret'] = int(media.phone_number or '0')
            except ValueError:
                row['secret'] = 0

        elif isinstance(media, tl.MessageMediaDocument):
            row['type'] = 'document'
            doc = media.document
            if isinstance(doc, tl.Document):
                row['mime_type'] = doc.mime_type
                row['size'] = doc.size
                row['thumbnail_id'] = self.dump_media(doc.thumb)
                for attr in doc.attributes:
                    if isinstance(attr, tl.DocumentAttributeFilename):
                        row['name'] = attr.file_name
                        break
                row['local_id'] = doc.id
                row['volume_id'] = doc.version
                row['secret'] = doc.access_hash

        elif isinstance(media, tl.MessageMediaEmpty):
            row['type'] = 'empty'
            return

        elif isinstance(media, tl.MessageMediaGame):
            row['type'] = 'game'
            game = media.game
            if isinstance(game, tl.Game):
                row['name'] = game.short_name
                row['thumbnail_id'] = self.dump_media(game.photo)
                row['local_id'] = game.id
                row['secret'] = game.access_hash

        elif isinstance(media, tl.MessageMediaGeo):
            row['type'] = 'geo'
            geo = media.geo
            if isinstance(geo, tl.GeoPoint):
                row['name'] = '({}, {})'.format(repr(geo.lat), repr(geo.long))

        elif isinstance(media, tl.MessageMediaGeoLive):
            row['type'] = 'geolive'
            geo = media.geo
            if isinstance(geo, tl.GeoPoint):
                row['name'] = '({}, {})'.format(repr(geo.lat), repr(geo.long))

        elif isinstance(media, tl.MessageMediaInvoice):
            row['type'] = 'invoice'
            row['name'] = media.title
            row['thumbnail_id'] = self.dump_media(media.photo)

        elif isinstance(media, tl.MessageMediaPhoto):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            media = media.photo

        elif isinstance(media, tl.MessageMediaUnsupported):
            row['type'] = 'unsupported'
            return

        elif isinstance(media, tl.MessageMediaVenue):
            row['type'] = 'venue'
            row['name'] = '{} - {} ({}, {} {})'.format(
                media.title, media.address,
                media.provider, media.venue_id, media.venue_type
            )
            geo = media.geo
            if isinstance(geo, tl.GeoPoint):
                row['name'] += ' at ({}, {})'.format(
                    repr(geo.lat), repr(geo.long)
                )

        elif isinstance(media, tl.MessageMediaWebPage):
            row['type'] = 'webpage'
            web = media.webpage
            if isinstance(web, tl.WebPage):
                row['name'] = web.title
                row['thumbnail_id'] = self.dump_media(web.photo, 'thumbnail')
                row['local_id'] = web.id
                row['secret'] = web.hash

        if isinstance(media, tl.Photo):
            # Extra fallback cases for common parts
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            row['name'] = str(media.date)
            sizes = [x for x in media.sizes
                     if isinstance(x, (tl.PhotoSize, tl.PhotoCachedSize))]
            if sizes:
                small = min(sizes, key=lambda s: s.w * s.h)
                large = max(sizes, key=lambda s: s.w * s.h)
                media = large
                if small != large:
                    row['thumbnail_id'] = self.dump_media(small, 'thumbnail')

        if isinstance(media, (tl.PhotoSize, tl.PhotoCachedSize)):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            if isinstance(media.location, tl.FileLocation):
                media = media.location

        if isinstance(media, (tl.UserProfilePhoto, tl.ChatPhoto)):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            row['thumbnail_id'] = self.dump_media(
                media.photo_small, 'thumbnail'
            )
            media = media.photo_big

        if isinstance(media, tl.FileLocation):
            row['local_id'] = media.local_id
            row['volume_id'] = media.volume_id
            row['secret'] = media.secret

        if row['type']:
            return self._insert('Media', (
                None,
                row['name'], row['mime_type'], row['size'],
                row['thumbnail_id'], row['type'],
                row['local_id'], row['volume_id'], row['secret'],
                row['extra']
            ))

    def dump_forward(self, forward):
        """Dump a message forward relationship into the Forward table
        The caller is responsible for ensuring from_id is a unique and correct ID
        Params: MessageFwdHeader Telethon object
        Returns: ID of inserted row"""
        if not forward:
            return None

        return self._insert('Forward',
                            (None,  # Database will handle this
                             forward.date.timestamp(),
                             forward.from_id,
                             forward.channel_post,
                             forward.post_author))

    def get_message(self, context_id, which):
        """Returns MAX or MIN message available for context_id.
        Used to determine at which point a backup should stop."""
        if which not in ('MIN', 'MAX'):
            raise ValueError('Parameter', which, 'must be MIN or MAX.')

        return self.message_from_tuple(self.conn.execute(
            """SELECT * FROM Message WHERE ID = (
                    SELECT {which}(ID) FROM Message
                    WHERE ContextID = ?
                )
            """.format(which=which), (context_id,)).fetchone())

    def iter_messages(self, context_id):
        """Iterates over the messages on context_id, in ascending order"""
        c = self.conn.execute(
            """SELECT * FROM Message WHERE ContextID = ? ORDER BY ID ASC""",
            (context_id,)
        )
        msg = c.fetchone()
        while msg:
            yield self.message_from_tuple(msg)
            msg = c.fetchone()

    def get_message_count(self, context_id):
        """Gets the message count for the given context"""
        tuple_ = self.conn.execute(
            "SELECT COUNT(*) FROM MESSAGE WHERE ContextID = ?", (context_id,)
        ).fetchone()
        return tuple_[0] if tuple_ else 0

    def update_last_dumped_message(self, context_id, msg_id):
        """Updates the last dumped message"""

        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO LastMessage VALUES (?,?)",
                (context_id, msg_id)
            )
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def get_last_dumped_message(self, context_id):
        """Returns the last dumped message for a context iD.
        Used to determine from where a backup should resume."""
        c = self.conn.execute("SELECT ID FROM LastMessage WHERE ContextID = ?",
                              (context_id,))
        tuple_ = c.fetchone()
        if tuple_:
            c.execute("""SELECT * FROM Message WHERE
                         ID = ? AND ContextID = ?""",
                      (tuple_[0], context_id))
            return self.message_from_tuple(c.fetchone())

    def _insert(self, into, values):
        """
        Helper method to insert or replace the
        given tuple of values into the given table.
        """
        try:
            fmt = ','.join('?' * len(values))
            c = self.conn.execute("INSERT OR REPLACE INTO {} VALUES ({})"
                                  .format(into, fmt), values)
            self.conn.commit()
            return c.lastrowid
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def message_from_tuple(self, message_tuple):
        if not message_tuple:
            return

        c = self.conn.cursor()
        c.execute("SELECT * FROM Forward WHERE ID = ?",
                  (message_tuple[6],))
        fwd = Dumper.fwd_from_tuple(c.fetchone())

        c.execute("SELECT * FROM Media WHERE ID = ?",
                  (message_tuple[9],))
        loc = Dumper.location_from_tuple(c.fetchone())
        if loc == tl.InputFileLocation:
            media = tl.MessageMediaPhoto(
                caption=message_tuple[4],
                photo=tl.Photo(
                    id=0,
                    access_hash=0,
                    date=None,
                    sizes=[tl.PhotoSize(
                        type='',
                        location=loc,
                        w=0,
                        h=0,
                        size=0
                    )]
                )
            )
        elif loc == tl.InputDocumentFileLocation:
            media = tl.MessageMediaDocument(
                caption=message_tuple[4],
                document=tl.Document(
                    id=loc.id,
                    access_hash=loc.access_hash,
                    version=loc.version,
                    dc_id=0,
                    mime_type='',
                    date=None,
                    size=0,
                    thumb=None,
                    attributes=[]
                )
            )
        else:
            media = None

        # ContextID often matches with to_id, except for incoming PMs
        to_id, to_type = resolve_id(message_tuple[1])
        return tl.Message(
            id=message_tuple[0],
            to_id=to_type(to_id),
            date=datetime.fromtimestamp(message_tuple[2]),
            from_id=message_tuple[3],
            message=message_tuple[4],
            reply_to_msg_id=message_tuple[5],
            fwd_from=fwd,
            post_author=message_tuple[7],
            views=message_tuple[8],
            media=media  # Cannot exactly reconstruct it
        )

    @staticmethod
    def fwd_from_tuple(fwd_tuple):
        if not fwd_tuple:
            return

        return tl.MessageFwdHeader(
            date=datetime.fromtimestamp(fwd_tuple[1]),
            from_id=fwd_tuple[2],
            channel_post=fwd_tuple[3],
            post_author=fwd_tuple[4]
        )

    @staticmethod
    def location_from_tuple(loc_tuple):
        if not loc_tuple:
            return

        if loc_tuple[4] == InputFileType.NORMAL.value:
            return tl.InputFileLocation(
                local_id=loc_tuple[1],
                volume_id=loc_tuple[2],
                secret=loc_tuple[3]
            )
        elif loc_tuple[4] == InputFileType.DOCUMENT.value:
            return tl.InputDocumentFileLocation(
                id=loc_tuple[1],
                version=loc_tuple[2],
                access_hash=loc_tuple[3]
            )

    @staticmethod
    def rows_are_same(row2, row1, ignore_column):
        """Compare two records, ignoring the DateUpdated"""
        # Note that sqlite stores True as 1 and False as 0
        # but python handles this fine anyway (probably)
        if not row1 or not row2:
            return False
        if len(row1) != len(row2):
            return False
        for i, x in enumerate(row1):
            if (i != ignore_column) and x != row2[i]:
                return False
        return True
