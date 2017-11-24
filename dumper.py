#!/usr/bin/env python3
"""A module for dumping export data into the database"""
import sqlite3
import time
import logging

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Dumper():
    """Class to interface with the database for exports"""

    def __init__(self, config, database_name='export'):
        """Initialise the dumper.
        Params:
        - settings: a dictionary of settings;
        - database_name: filename without file extension"""
        self.config = config
        self.conn = sqlite3.connect('{}.db'.format(database_name))
        self.cur = self.conn.cursor()
        # If this is a new database, populate it
        self.cur.execute("SELECT name FROM sqlite_master "
                         "WHERE type='table' AND name='User';")
        if not self.cur.fetchall():
            # Database has no User table, so it's either new or wrecked
            # We'll treat it as a new database, make the tables
            # First, drop them all :)
            tables = list(self.cur.execute(
                "SELECT name FROM sqlite_master WHERE type is 'table'"))
            commands = ['DROP TABLE IF EXISTS {}'.format(t[0]) for t in tables]
            self.cur.executescript(';'.join(commands))
            # and yes, '' is a valid script

            self.cur.execute("CREATE TABLE Forward("
                             "ID INTEGER PRIMARY KEY AUTOINCREMENT,"
                             "OriginalDate INT NOT NULL,"
                             "FromID INT," # User or Channel ID
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
                             "MediaID INT,"
                             "FOREIGN KEY (ForwardID) REFERENCES Forward(ID),"
                             "FOREIGN KEY (MediaID) REFERENCES Media(ID),"
                             "PRIMARY KEY (ID, ContextID)) WITHOUT ROWID")
            self.conn.commit()

    def dump_message(self, message, forward_id, media_id):
        #TODO handle edits/deletes (fundamental problems with non-long-running exporter)
        """Dump a Message into the Message table
        The caller is responsible for ensuring to_id is a unique and correct contextID
        Params:
        - Message to dump,
        - ID of Forward in the DB (or None),
        - ID of message Media in the DB (or None)
        Returns: -"""
        values = (message.id,
                  message.to_id,
                  message.date.timestamp(),
                  message.from_id,
                  message.message,
                  message.reply_to_msg_id,
                  forward_id,
                  message.post_author,
                  media_id)
        try:
            self.cur.execute("INSERT INTO Message VALUES (?,?,?,?,?,?,?,?,?)", values)
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def dump_user(self, user_full, photo_id):
        #TODO: Use invalidation time
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
                and values[1] - last[1] < int(self.config['ForceNoChangeDumpAfter'])):
            return False

        try:
            self.cur.execute("INSERT INTO User VALUES (?,?,?,?,?,?,?,?,?,?)", values)
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def dump_channel(self, channel_full, channel, photo_id):
        #TODO: Use invalidation time
        """Dump a Channel into the Channel table
        Params: ChannelFull, Channel to dump, MediaID of the profile photo in the DB
        Returns -"""
        # Need to get the full object too for 'about' info
        timestamp = round(time.time())
        values = (channel.id,
                  timestamp,
                  channel_full.about,
                  channel.title,
                  channel.username,
                  photo_id)
        try:
            self.cur.execute("INSERT INTO Channel VALUES (?,?,?,?,?,?)", values)
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def dump_supergroup(self, supergroup_full, supergroup, photo_id):
        #TODO: Use invalidation time
        """Dump a Supergroup into the Supergroup table
        Params: ChannelFull, Channel to dump, MediaID of the profile photo in the DB
        Returns -"""
        # Need to get the full object too for 'about' info
        timestamp = round(time.time())
        values = (supergroup.id,
                  timestamp,
                  supergroup_full.about,
                  supergroup.title,
                  supergroup.username,
                  photo_id)
        try:
            self.cur.execute("INSERT INTO Supergroup VALUES (?,?,?,?,?,?)", values)
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def dump_chat(self, chat, photo_id):
        #TODO: Use invalidation time
        """Dump a Chat into the Chat table
        Params: Chat to dump, MediaID of the profile photo in the DB
        Returns -"""
        timestamp = round(time.time())
        values = (chat.id,
                  timestamp,
                  chat.title,
                  chat.migrated_to,
                  photo_id)
        try:
            self.cur.execute("INSERT INTO Chat VALUES (?,?,?,?,?)", values)
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def dump_filelocation(self, file_location):
        """Dump a FileLocation into the Media table
        Params: FileLocation Telethon object
        Returns: ID of inserted row"""
        values = (None, # Database will handle this
                  file_location.local_id,
                  file_location.volume_id,
                  file_location.dc_id,
                  file_location.secret)
        try:
            self.cur.execute("INSERT INTO Media VALUES (?,?,?,?,?)", values)
            self.conn.commit()
            return self.cur.lastrowid
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def dump_forward(self, forward):
        """Dump a message forward relationship into the Forward table
        The caller is responsible for ensuring from_id is a unique and correct ID
        Params: MessageFwdHeader Telethon object
        Returns: ID of inserted row"""
        values = (None, # Database will handle this
                  forward.date.timestamp(),
                  forward.from_id,
                  forward.channel_post,
                  forward.post_author)
        try:
            self.cur.execute("INSERT INTO Forward VALUES (?,?,?,?,?)", values)
            self.conn.commit()
            return self.cur.lastrowid
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    @staticmethod
    def rows_are_same(row2, row1, ignore_column):
        """Compare two records, ignoring the DateUpdated"""
        # Note that sqlite stores True as 1 and False as 0
        # but python handles this fine anyway (probably)
        if not row1 or not row2:
            return False
        if len(row1) != len(row2):
            return False
        for i,x in enumerate(row1):
            if (i != ignore_column) and x != row2[i]:
                return False
        return True


def test():
    """Enter an example user to test dump_user"""
    #TODO: real tests
    settings = {'ForceNoChangeDumpAfter':432000}
    dumper = Dumper(settings)
    from telethon.tl.types import User, UserFull
    usr = User(1,
               first_name='first',
               last_name=None,
               username='username',
               phone=None,
               bot=False)
    usrfull = UserFull(usr,
                       None,
                       None,
                       2,
                       about='test')
    dumper.dump_user(usrfull, 1)

if __name__ == '__main__':
    test()
