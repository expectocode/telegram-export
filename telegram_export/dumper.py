#!/usr/bin/env python3
"""A module for dumping export data into the database"""
import json
import logging
import sqlite3
import sys
import time
from base64 import b64encode
from datetime import datetime
from enum import Enum
import os.path

from telethon.tl import types
from telethon.utils import get_peer_id, resolve_id, get_input_peer

from . import utils

logger = logging.getLogger(__name__)

DB_VERSION = 1  # database version


class InputFileType(Enum):
    """An enum to specify the type of an InputFile"""
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
        """
        Initialise the dumper. `config` should be a dict-like
        object from the config file's Dumper section".
        """
        self.config = config
        if 'DBFileName' in self.config:
            where = self.config["DBFileName"]
            if where != ':memory:':
                where = '{}.db'.format(os.path.join(
                    self.config['OutputDirectory'], self.config['DBFileName']
                ))
            self.conn = sqlite3.connect(where, check_same_thread=False)
        else:
            logger.error("A database filename is required!")
            exit()
        c = self.conn.cursor()

        self.chunk_size = max(int(config.get('ChunkSize', 100)), 1)
        self.max_chunks = max(int(config.get('MaxChunks', 0)), 0)
        self.invalidation_time = max(config.getint('InvalidationTime', 0), -1)

        self.dump_methods = ('message', 'user', 'message_service', 'channel',
                             'supergroup', 'chat', 'adminlog_event', 'media',
                             'participants_delta', 'media', 'forward')

        self._dump_callbacks = {method: set() for method in self.dump_methods}

        c.execute("SELECT name FROM sqlite_master "
                  "WHERE type='table' AND name='Version'")

        exists = bool(c.fetchone())
        if exists:
            # Tables already exist, check for the version
            c.execute("SELECT Version FROM Version")
            version = c.fetchone()
            if not version:
                # Sometimes there may be a table without values (see #55)
                c.execute("DROP TABLE IF EXISTS Version")
                exists = False
            elif version[0] != DB_VERSION:
                self._upgrade_database(old=version[0])
                self.conn.commit()
        if not exists:
            # Tables don't exist, create new ones
            c.execute("CREATE TABLE Version (Version INTEGER)")
            c.execute("CREATE TABLE SelfInformation (UserID INTEGER)")
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
                      "PRIMARY KEY (ID, DateUpdated))")

            c.execute("CREATE TABLE Channel("
                      "ID INT NOT NULL,"
                      "DateUpdated INT NOT NULL,"
                      "About TEXT,"
                      "Title TEXT NOT NULL,"
                      "Username TEXT,"
                      "PictureID INT,"
                      "PinMessageID INT,"
                      "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                      "PRIMARY KEY (ID, DateUpdated))")

            c.execute("CREATE TABLE Supergroup("
                      "ID INT NOT NULL,"
                      "DateUpdated INT NOT NULL,"
                      "About TEXT,"
                      "Title TEXT NOT NULL,"
                      "Username TEXT,"
                      "PictureID INT,"
                      "PinMessageID INT,"
                      "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                      "PRIMARY KEY (ID, DateUpdated))")

            c.execute("CREATE TABLE Chat("
                      "ID INT NOT NULL,"
                      "DateUpdated INT NOT NULL,"
                      "Title TEXT NOT NULL,"
                      "MigratedToID INT,"
                      "PictureID INT,"
                      "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                      "PRIMARY KEY (ID, DateUpdated))")

            c.execute("CREATE TABLE ChatParticipants("
                      "ContextID INT NOT NULL,"
                      "DateUpdated INT NOT NULL,"
                      "Added TEXT NOT NULL,"
                      "Removed TEXT NOT NULL,"
                      "PRIMARY KEY (ContextID, DateUpdated))")

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
                      "Formatting TEXT,"  # e.g. bold, italic, etc.
                      "ServiceAction TEXT,"  # friendly name of action if it is
                      # a MessageService
                      "FOREIGN KEY (ForwardID) REFERENCES Forward(ID),"
                      "FOREIGN KEY (MediaID) REFERENCES Media(ID),"
                      "PRIMARY KEY (ID, ContextID))")

            c.execute("CREATE TABLE AdminLog("
                      "ID INT NOT NULL,"
                      "ContextID INT NOT NULL,"
                      "Date INT NOT NULL,"
                      "UserID INT,"
                      "MediaID1 INT,"  # e.g. new photo
                      "MediaID2 INT,"  # e.g. old photo
                      "Action TEXT,"  # Friendly name for the action
                      "Data TEXT,"  # JSON data of the entire action
                      "FOREIGN KEY (MediaID1) REFERENCES Media(ID),"
                      "FOREIGN KEY (MediaID2) REFERENCES Media(ID),"
                      "PRIMARY KEY (ID, ContextID))")

            c.execute("CREATE TABLE Resume("
                      "ContextID INT NOT NULL,"
                      "ID INT NOT NULL,"
                      "Date INT NOT NULL,"
                      "StopAt INT NOT NULL,"
                      "PRIMARY KEY (ContextID))")

            c.execute("CREATE TABLE ResumeEntity("
                      "ContextID INT NOT NULL,"
                      "ID INT NOT NULL,"
                      "AccessHash INT,"
                      "PRIMARY KEY (ContextID, ID))")

            c.execute("CREATE TABLE ResumeMedia("
                      "MediaID INT NOT NULL,"
                      "ContextID INT NOT NULL,"
                      "SenderID INT,"
                      "Date INT,"
                      "PRIMARY KEY (MediaID))")
            self.conn.commit()

    def _upgrade_database(self, old):
        """
        This method knows how to migrate from old -> DB_VERSION.

        Currently it performs no operation because this is the
        first version of the tables, in the future it should alter
        tables or somehow transfer the data between what changed.
        """

    # TODO make these callback functions less repetitive.
    # For the most friendly API, we should  have different methods for each
    # kind of callback, but there could be a way to make this cleaner.
    # Perhaps a dictionary mapping 'message' to the message callback set.

    def add_callback(self, dump_method, callback):
        """
        Add the callback function to the set of callbacks for the given
        dump method. dump_method should be a string, and callback should be a
        function which takes one argument - a tuple which will be dumped into
        the database. The list of valid dump methods is dumper.dump_methods.
        If the dumper does not dump a row due to the invalidation_time, the
        callback will still be called.
        """
        if dump_method not in self.dump_methods:
            raise ValueError("Cannot attach callback to method {}. Available "
                             "methods are {}".format(dump_method, self.dump_methods))

        self._dump_callbacks[dump_method].add(callback)

    def remove_callback(self, dump_method, callback):
        """
        Remove the callback function from the set of callbacks for the given
        dump method. Will raise KeyError if the callback is not in the set of
        callbacks for that method
        """
        if dump_method not in self.dump_methods:
            raise ValueError("Cannot remove callback from method {}. Available "
                             "methods are {}".format(dump_method, self.dump_methods))

        self._dump_callbacks[dump_method].remove(callback)

    def check_self_user(self, self_id):
        """
        Checks the self ID. If there is a stored ID and it doesn't match the
        given one, an error message is printed and the application exits.
        """
        cur = self.conn.cursor()
        cur.execute("SELECT UserID FROM SelfInformation")
        result = cur.fetchone()
        if result:
            if result[0] != self_id:
                print('This export database belongs to another user!',
                      file=sys.stderr)
                exit(1)
        else:
            cur.execute("INSERT INTO SelfInformation VALUES (?)", (self_id,))
            self.commit()

    def dump_message(self, message, context_id, forward_id, media_id):
        """
        Dump a Message into the Message table.

        Params:
            Message to dump,
            ID of the chat dumping,
            ID of Forward in the DB (or None),
            ID of message Media in the DB (or None)

        Returns:
            Inserted row ID.
        """
        if not message.message and message.media:
            message.message = getattr(message.media, 'caption', '')

        row = (message.id,
               context_id,
               message.date.timestamp(),
               message.from_id,
               message.message,
               message.reply_to_msg_id,
               forward_id,
               message.post_author,
               message.views,
               media_id,
               utils.encode_msg_entities(message.entities),
               None)  # No MessageAction

        for callback in self._dump_callbacks['message']:
            callback(row)

        return self._insert('Message', row)

    def dump_message_service(self, message, context_id, media_id):
        """Similar to self.dump_message, but for MessageAction's."""
        name = utils.action_to_name(message.action)
        if not name:
            return

        extra = message.action.to_dict()
        del extra['_']  # We don't need to store the type, already have name
        sanitize_dict(extra)
        extra = json.dumps(extra)

        row = (message.id,
               context_id,
               message.date.timestamp(),
               message.from_id,
               extra,  # Message field contains the information
               message.reply_to_msg_id,
               None,  # No forward
               None,  # No author
               None,  # No views
               media_id,  # Might have e.g. a new chat Photo
               None,  # No entities
               name)

        for callback in self._dump_callbacks['message_service']:
            callback(row)

        return self._insert('Message', row)

    def dump_admin_log_event(self, event, context_id, media_id1, media_id2):
        """Similar to self.dump_message_service but for channel actions."""
        name = utils.action_to_name(event.action)
        if not name:
            return

        extra = event.action.to_dict()
        del extra['_']  # We don't need to store the type, already have name
        sanitize_dict(extra)
        extra = json.dumps(extra)

        row = (event.id,
               context_id,
               event.date.timestamp(),
               event.user_id,
               media_id1,
               media_id2,
               name,
               extra)

        for callback in self._dump_callbacks['adminlog_event']:
            callback(row)

        return self._insert('AdminLog', row)

    def dump_user(self, user_full, photo_id, timestamp=None):
        """Dump a UserFull into the User table
        Params: UserFull to dump, MediaID of the profile photo in the DB
        Returns -, or False if not added"""
        # Rationale for UserFull rather than User is to get bio
        values = (user_full.user.id,
                  timestamp or round(time.time()),
                  user_full.user.first_name,
                  user_full.user.last_name,
                  user_full.user.username,
                  user_full.user.phone,
                  user_full.about,
                  user_full.user.bot,
                  user_full.common_chats_count,
                  photo_id)

        for callback in self._dump_callbacks['user']:
            callback(values)

        return self._insert_if_valid_date('User', values, date_column=1,
                                          where=('ID', user_full.user.id))

    def dump_channel(self, channel_full, channel, photo_id, timestamp=None):
        """Dump a Channel into the Channel table.
        Params: ChannelFull, Channel to dump, MediaID
                of the profile photo in the DB
        Returns -"""
        # Need to get the full object too for 'about' info
        values = (get_peer_id(channel),
                  timestamp or round(time.time()),
                  channel_full.about,
                  channel.title,
                  channel.username,
                  photo_id,
                  channel_full.pinned_msg_id)

        for callback in self._dump_callbacks['channel']:
            callback(values)

        return self._insert_if_valid_date('Channel', values, date_column=1,
                                          where=('ID', get_peer_id(channel)))

    def dump_supergroup(self, supergroup_full, supergroup, photo_id,
                        timestamp=None):
        """Dump a Supergroup into the Supergroup table
        Params: ChannelFull, Channel to dump, MediaID
                of the profile photo in the DB.
        Returns -"""
        # Need to get the full object too for 'about' info
        values = (get_peer_id(supergroup),
                  timestamp or round(time.time()),
                  getattr(supergroup_full, 'about', None) or '',
                  supergroup.title,
                  supergroup.username,
                  photo_id,
                  supergroup_full.pinned_msg_id)

        for callback in self._dump_callbacks['supergroup']:
            callback(values)

        return self._insert_if_valid_date('Supergroup', values, date_column=1,
                                          where=('ID', get_peer_id(supergroup)))

    def dump_chat(self, chat, photo_id, timestamp=None):
        """Dump a Chat into the Chat table
        Params: Chat to dump, MediaID of the profile photo in the DB
        Returns -"""
        if isinstance(chat.migrated_to, types.InputChannel):
            migrated_to_id = chat.migrated_to.channel_id
        else:
            migrated_to_id = None

        values = (get_peer_id(chat),
                  timestamp or round(time.time()),
                  chat.title,
                  migrated_to_id,
                  photo_id)

        for callback in self._dump_callbacks['chat']:
            callback(values)

        return self._insert_if_valid_date('Chat', values, date_column=1,
                                          where=('ID', get_peer_id(chat)))

    def dump_participants_delta(self, context_id, ids):
        """
        Dumps the delta between the last dump of IDs for the given context ID
        and the current input user IDs.
        """
        ids = set(ids)
        c = self.conn.cursor()
        c.execute('SELECT Added, Removed FROM ChatParticipants '
                  'WHERE ContextID = ? ORDER BY DateUpdated ASC',
                  (context_id,))

        row = c.fetchone()
        if not row:
            added = ids
            removed = set()
        else:
            # Build the last known list of participants from the saved deltas
            last_ids = set(int(x) for x in row[0].split(','))
            row = c.fetchone()
            while row:
                added = set(int(x) for x in row[0].split(',') if x != '')
                removed = set(int(x) for x in row[1].split(',') if x != '')
                last_ids = (last_ids | added) - removed
                row = c.fetchone()
            added = ids - last_ids
            removed = last_ids - ids

        row = (context_id,
               round(time.time()),
               ','.join(str(x) for x in added),
               ','.join(str(x) for x in removed))

        for callback in self._dump_callbacks['participants_delta']:
            callback(row)

        c.execute("INSERT INTO ChatParticipants VALUES (?, ?, ?, ?)", row)
        return added, removed

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

        if isinstance(media, types.MessageMediaContact):
            row['type'] = 'contact'
            row['name'] = '{} {}'.format(media.first_name, media.last_name)
            row['local_id'] = media.user_id
            try:
                row['secret'] = int(media.phone_number or '0')
            except ValueError:
                row['secret'] = 0

        elif isinstance(media, types.MessageMediaDocument):
            row['type'] = utils.get_media_type(media)
            doc = media.document
            if isinstance(doc, types.Document):
                row['mime_type'] = doc.mime_type
                row['size'] = doc.size
                row['thumbnail_id'] = self.dump_media(doc.thumb)
                row['local_id'] = doc.id
                row['volume_id'] = doc.version
                row['secret'] = doc.access_hash
                for attr in doc.attributes:
                    if isinstance(attr, types.DocumentAttributeFilename):
                        row['name'] = attr.file_name

        elif isinstance(media, types.MessageMediaEmpty):
            row['type'] = 'empty'
            return

        elif isinstance(media, types.MessageMediaGame):
            row['type'] = 'game'
            game = media.game
            if isinstance(game, types.Game):
                row['name'] = game.short_name
                row['thumbnail_id'] = self.dump_media(game.photo)
                row['local_id'] = game.id
                row['secret'] = game.access_hash

        elif isinstance(media, types.MessageMediaGeo):
            row['type'] = 'geo'
            geo = media.geo
            if isinstance(geo, types.GeoPoint):
                row['name'] = '({}, {})'.format(repr(geo.lat), repr(geo.long))

        elif isinstance(media, types.MessageMediaGeoLive):
            row['type'] = 'geolive'
            geo = media.geo
            if isinstance(geo, types.GeoPoint):
                row['name'] = '({}, {})'.format(repr(geo.lat), repr(geo.long))

        elif isinstance(media, types.MessageMediaInvoice):
            row['type'] = 'invoice'
            row['name'] = media.title
            row['thumbnail_id'] = self.dump_media(media.photo)

        elif isinstance(media, types.MessageMediaPhoto):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            media = media.photo

        elif isinstance(media, types.MessageMediaUnsupported):
            row['type'] = 'unsupported'
            return

        elif isinstance(media, types.MessageMediaVenue):
            row['type'] = 'venue'
            row['name'] = '{} - {} ({}, {} {})'.format(
                media.title, media.address,
                media.provider, media.venue_id, media.venue_type
            )
            geo = media.geo
            if isinstance(geo, types.GeoPoint):
                row['name'] += ' at ({}, {})'.format(
                    repr(geo.lat), repr(geo.long)
                )

        elif isinstance(media, types.MessageMediaWebPage):
            row['type'] = 'webpage'
            web = media.webpage
            if isinstance(web, types.WebPage):
                row['name'] = web.title
                row['thumbnail_id'] = self.dump_media(web.photo, 'thumbnail')
                row['local_id'] = web.id
                row['secret'] = web.hash

        if isinstance(media, types.Photo):
            # Extra fallback cases for common parts
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            row['name'] = str(media.date)
            sizes = [x for x in media.sizes
                     if isinstance(x, (types.PhotoSize, types.PhotoCachedSize))]
            if sizes:
                small = min(sizes, key=lambda s: s.w * s.h)
                large = max(sizes, key=lambda s: s.w * s.h)
                media = large
                if small != large:
                    row['thumbnail_id'] = self.dump_media(small, 'thumbnail')

        if isinstance(media, (types.PhotoSize,
                              types.PhotoCachedSize,
                              types.PhotoSizeEmpty)):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            if isinstance(media, types.PhotoSizeEmpty):
                row['size'] = 0
            else:
                if isinstance(media, types.PhotoSize):
                    row['size'] = media.size
                elif isinstance(media, types.PhotoCachedSize):
                    row['size'] = len(media.bytes)
                if isinstance(media.location, types.FileLocation):
                    media = media.location

        if isinstance(media, (types.UserProfilePhoto, types.ChatPhoto)):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            row['thumbnail_id'] = self.dump_media(
                media.photo_small, 'thumbnail'
            )
            media = media.photo_big

        if isinstance(media, types.FileLocation):
            row['local_id'] = media.local_id
            row['volume_id'] = media.volume_id
            row['secret'] = media.secret

        if row['type']:
            # We'll say two files are the same if they point to the same
            # downloadable content (through local_id/volume_id/secret).

            for callback in self._dump_callbacks['media']:
                callback(row)

            c = self.conn.cursor()
            c.execute('SELECT ID FROM Media WHERE LocalID = ? '
                      'AND VolumeID = ? AND Secret = ?',
                      (row['local_id'], row['volume_id'], row['secret']))
            existing_row = c.fetchone()
            if existing_row:
                return existing_row[0]

            return self._insert('Media', (
                None,
                row['name'], row['mime_type'], row['size'],
                row['thumbnail_id'], row['type'],
                row['local_id'], row['volume_id'], row['secret'],
                row['extra']
            ))

    def dump_forward(self, forward):
        """
        Dump a message forward relationship into the Forward table.

        Params: MessageFwdHeader Telethon object
        Returns: ID of inserted row"""
        if not forward:
            return None

        row = (None,  # Database will handle this
               forward.date.timestamp(),
               forward.from_id,
               forward.channel_post,
               forward.post_author)

        for callback in self._dump_callbacks['forward']:
            callback(row)

        return self._insert('Forward', row)

    def get_max_message_id(self, context_id):
        """
        Returns the largest saved message ID for the given
        context_id, or 0 if no messages have been saved.
        """
        row = self.conn.execute("SELECT MAX(ID) FROM Message WHERE "
                                "ContextID = ?", (context_id,)).fetchone()
        return row[0] if row else 0

    def get_message_count(self, context_id):
        """Gets the message count for the given context"""
        tuple_ = self.conn.execute(
            "SELECT COUNT(*) FROM MESSAGE WHERE ContextID = ?", (context_id,)
        ).fetchone()
        return tuple_[0] if tuple_ else 0

    def get_resume(self, context_id):
        """
        For the given context ID, return a tuple consisting of the offset
        ID and offset date from which to continue, as well as at which ID
        to stop.
        """
        c = self.conn.execute("SELECT ID, Date, StopAt FROM Resume WHERE "
                              "ContextID = ?", (context_id,))
        return c.fetchone() or (0, 0, 0)

    def save_resume(self, context_id, msg=0, msg_date=0, stop_at=0):
        """
        Saves the information required to resume a download later.
        """
        if isinstance(msg_date, datetime):
            msg_date = int(msg_date.timestamp())

        return self._insert('Resume', (context_id, msg, msg_date, stop_at))

    def iter_resume_entities(self, context_id):
        """
        Returns an iterator over the entities that need resuming for the
        given context_id. Note that the entities are *removed* once the
        iterator is consumed completely.
        """
        c = self.conn.execute("SELECT ID, AccessHash FROM ResumeEntity "
                              "WHERE ContextID = ?", (context_id,))
        row = c.fetchone()
        while row:
            kind = resolve_id(row[0])[1]
            if kind == types.PeerUser:
                yield types.InputPeerUser(row[0], row[1])
            elif kind == types.PeerChat:
                yield types.InputPeerChat(row[0])
            elif kind == types.PeerChannel:
                yield types.InputPeerChannel(row[0], row[1])
            row = c.fetchone()

        c.execute("DELETE FROM ResumeEntity WHERE ContextID = ?",
                  (context_id,))

    def save_resume_entities(self, context_id, entities):
        """
        Saves the given entities for resuming at a later point.
        """
        rows = []
        for ent in entities:
            ent = get_input_peer(ent)
            if isinstance(ent, types.InputPeerUser):
                rows.append((context_id, ent.user_id, ent.access_hash))
            elif isinstance(ent, types.InputPeerChat):
                rows.append((context_id, ent.chat_id, None))
            elif isinstance(ent, types.InputPeerChannel):
                rows.append((context_id, ent.channel_id, ent.access_hash))
        c = self.conn.cursor()
        c.executemany("INSERT OR REPLACE INTO ResumeEntity "
                      "VALUES (?,?,?)", rows)

    def iter_resume_media(self, context_id):
        """
        Returns an iterator over the media tuples that need resuming for
        the given context_id. Note that the media rows are *removed* once
        the iterator is consumed completely.
        """
        c = self.conn.execute(
            "SELECT MediaID, SenderID, Date "
            "FROM ResumeMedia WHERE ContextID = ?", (context_id,)
        )
        row = c.fetchone()
        while row:
            media_id, sender_id, date = row
            yield media_id, sender_id, datetime.utcfromtimestamp(date)
            row = c.fetchone()

        c.execute("DELETE FROM ResumeMedia WHERE ContextID = ?",
                  (context_id,))

    def save_resume_media(self, media_tuples):
        """
        Saves the given media tuples for resuming at a later point.

        The tuples should consist of four elements, these being
        ``(media_id, context_id, sender_id, date)``.
        """
        self.conn.executemany("INSERT OR REPLACE INTO ResumeMedia "
                              "VALUES (?,?,?,?)", media_tuples)

    def _insert_if_valid_date(self, into, values, date_column, where):
        """
        Helper method to self._insert(into, values) after checking that the
        given values are different than the latest dump or that the delta
        between the current date and the existing column date_column is
        bigger than the invalidation time. `where` is used to get the last
        dumped item to check for invalidation time.

        As an example, ("ID", 4) -> WHERE ID = ?, 4
        """
        last = self.conn.execute(
            'SELECT * FROM {} WHERE {} = ? ORDER BY DateUpdated DESC'
            .format(into, where[0]), (where[1],)
        ).fetchone()

        if last:
            delta = values[date_column] - last[date_column]

            # Note sqlite stores True as 1 and False
            # as 0 but this is probably ok.
            if len(values) != len(last):
                raise TypeError(
                    "values has a different number of columns to table"
                )
            rows_same = True
            for i, val in enumerate(values):
                if i != date_column and val != last[i]:
                    rows_same = False

            if delta < self.invalidation_time and rows_same:
                return False
        return self._insert(into, values)

    def _insert(self, into, values):
        """
        Helper method to insert or replace the
        given tuple of values into the given table.
        """
        try:
            fmt = ','.join('?' * len(values))
            c = self.conn.execute("INSERT OR REPLACE INTO {} VALUES ({})"
                                  .format(into, fmt), values)
            return c.lastrowid
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def commit(self):
        """
        Commits the changes made to the database to persist on disk.
        """
        self.conn.commit()
