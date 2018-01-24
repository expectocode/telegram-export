#!/usr/bin/env python3
"""A module for dumping export data into the database"""
import sqlite3
import time
import logging
import warnings
from datetime import datetime
from telethon.tl import types as tl
from telethon.utils import get_peer_id, resolve_id

logger = logging.getLogger(__name__)

DB_VERSION = 1  # database version


class Dumper:
    """Class to interface with the database for exports"""

    def __init__(self, config):
        """Initialise the dumper.
        Params:
        - settings: a dictionary of settings;
        - database_name: filename without file extension"""
        self.config = config
        if 'DBFileName' in config:
            self.conn = sqlite3.connect('{}.db'.format(self.config['DBFileName']))
        else:
            self.conn = sqlite3.connect(':memory:')
        self.cur = self.conn.cursor()

        self.chunk_size = max(config.get('ChunkSize', 100), 1)
        self.max_chunks = max(config.get('MaxChunks', 0), 0)
        self.force_no_change_dump_after = \
            max(config.get('ForceNoChangeDumpAfter', 0), -1)

        self.cur.execute("SELECT name FROM sqlite_master "
                         "WHERE type='table' AND name='Version'")

        if self.cur.fetchone():
            # Tables already exist, check for the version
            self.cur.execute("SELECT Version FROM Version")
            version = self.cur.fetchone()[0]
            if version != DB_VERSION:
                self._upgrade_database(old=version)
                self.conn.commit()
        else:
            # Tables don't exist, create new ones
            self.cur.execute("CREATE TABLE Version (Version INTEGER)")
            self.cur.execute("INSERT INTO Version VALUES (?)", (DB_VERSION,))

            self.cur.execute("CREATE TABLE Forward("
                             "ID INTEGER PRIMARY KEY AUTOINCREMENT,"
                             "OriginalDate INT NOT NULL,"
                             "FromID INT,"  # User or Channel ID
                             "ChannelPost INT,"
                             "PostAuthor TEXT)")

            self.cur.execute("CREATE TABLE Media("
                             "ID INTEGER PRIMARY KEY AUTOINCREMENT,"
                             "LocalID INT NOT NULL,"
                             "VolumeID INT NOT NULL,"
                             "DCID INT NOT NULL,"
                             "Secret INT NOT NULL)")

            self.cur.execute("CREATE TABLE User("
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

            self.cur.execute("CREATE TABLE Channel("
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

            self.cur.execute("CREATE TABLE Supergroup("
                             "ID INT NOT NULL,"
                             "DateUpdated INT NOT NULL,"
                             # "CreatorID INT,"
                             "About TEXT,"
                             "Title TEXT NOT NULL,"
                             "Username TEXT,"
                             "PictureID INT,"
                             "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                             "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            self.cur.execute("CREATE TABLE Chat("
                             "ID INT NOT NULL,"
                             "DateUpdated INT NOT NULL,"
                             # "CreatorID INT,"
                             "Title TEXT NOT NULL,"
                             "MigratedToID INT,"
                             "PictureID INT,"
                             "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                             "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            self.cur.execute("CREATE TABLE Message("
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

            self.cur.execute("CREATE TABLE LastMessage("
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

        self.cur.execute('SELECT * FROM User ORDER BY DateUpdated DESC')
        last = self.cur.fetchone()
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
                             supergroup_full.about,
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

    def dump_filelocation(self, file_location):
        """Dump a FileLocation into the Media table
        Params: FileLocation Telethon object
        Returns: ID of inserted row"""
        if not file_location:
            return None

        if isinstance(file_location, tl.InputDocumentFileLocation):
            warnings.warn("Dumping InputDocumentFileLocation not implemented.")
            return

        return self._insert('Media',
                            (None,  # Database will handle this
                             file_location.local_id,
                             file_location.volume_id,
                             # Neither of the following have .dc_id:
                             #   InputDocumentFileLocation, FileLocationUnavailable
                             getattr(file_location, 'dc_id', None),
                             file_location.secret)
                            )

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

        self.cur.execute("""SELECT * FROM Message WHERE ID = (
                                SELECT {which}(ID) FROM Message
                                WHERE ContextID = ?
                            )
                         """.format(which=which), (context_id,))
        return Dumper.message_from_tuple(self.cur.fetchone())

    def iter_messages(self, context_id):
        """Iterates over the messages on context_id, in ascending order"""
        self.cur.execute("""SELECT * FROM Message WHERE ContextID = ? ORDER BY ID ASC""",
                         (context_id,))
        msg = self.cur.fetchone()
        while msg:
            yield Dumper.message_from_tuple(msg)
            msg = self.cur.fetchone()

    def get_message_count(self, context_id):
        """Gets the message count for the given context"""
        self.cur.execute("SELECT COUNT(*) FROM MESSAGE WHERE ContextID = ?",
                         (context_id,))
        tuple_ = self.cur.fetchone()
        return tuple_[0] if tuple_ else 0

    def update_last_dumped_message(self, context_id, msg_id):
        """Updates the last dumped message"""

        try:
            self.cur.execute("INSERT OR REPLACE INTO LastMessage VALUES (?,?)",
                             (context_id, msg_id))
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def get_last_dumped_message(self, context_id):
        """Returns the last dumped message for a context iD.
        Used to determine from where a backup should resume."""
        self.cur.execute("SELECT ID FROM LastMessage WHERE ContextID = ?",
                         (context_id,))
        tuple_ = self.cur.fetchone()
        if tuple_:
            self.cur.execute("""SELECT * FROM Message WHERE
                                ID = ? AND ContextID = ?""",
                             (tuple_[0], context_id))
            return Dumper.message_from_tuple(self.cur.fetchone())

    def _insert(self, into, values):
        """
        Helper method to insert or replace the
        given tuple of values into the given table.
        """
        try:
            fmt = ','.join('?' * len(values))
            self.cur.execute("INSERT OR REPLACE INTO {} VALUES ({})"
                             .format(into, fmt), values)
            self.conn.commit()
            return self.cur.lastrowid
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    @staticmethod
    def message_from_tuple(message_tuple):
        if not message_tuple:
            return

        to_id, to_type = resolve_id(message_tuple[1])
        return tl.Message(
            id=message_tuple[0],
            to_id=to_type(to_id),
            date=datetime.fromtimestamp(message_tuple[2]),
            from_id=message_tuple[3],
            message=message_tuple[4],
            reply_to_msg_id=message_tuple[5],
            fwd_from=None,  # TODO Select from the database
            post_author=message_tuple[6],
            media=None  # TODO Select from the database
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
