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

    def __init__(self, settings, database_name='export'):
        """Initialise the dumper.
        Params:
        - settings: a dictionary of settings;
        - database_name: filename without file extension"""
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
                             "ID INT PRIMARY KEY NOT NULL,"
                             "OriginalDate INT NOT NULL,"
                             "FromID INT,"
                             "ChannelPost INT,"
                             "PostAuthor TEXT) WITHOUT ROWID")

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
                             "PictureID INT,"
                             "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                             "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            self.cur.execute("CREATE TABLE Channel("
                             "ID INT NOT NULL,"
                             "DateUpdated INT NOT NULL,"
                             "CreatorID INT,"
                             "About TEXT,"
                             # "Signatures INT,"
                             "PictureID INT,"
                             "Title TEXT NOT NULL,"
                             "Username TEXT,"
                             "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                             "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            self.cur.execute("CREATE TABLE Supergroup("
                             "ID INT NOT NULL,"
                             "DateUpdated INT NOT NULL,"
                             "CreatorID INT,"
                             "About TEXT,"
                             "PictureID INT,"
                             "Title TEXT NOT NULL,"
                             "Username TEXT,"
                             "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                             "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            self.cur.execute("CREATE TABLE Chat("
                             "ID INT NOT NULL,"
                             "DateUpdated INT NOT NULL,"
                             "CreatorID INT,"
                             "PictureID INT,"
                             "Title TEXT NOT NULL,"
                             "MigratedToID INT,"
                             "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                             "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

            self.cur.execute("CREATE TABLE Message("
                             "ID INT NOT NULL,"
                             "ContextID INT NOT NULL,"
                             "Date INT NOT NULL,"
                             "FromID INT,"
                             "Message TEXT,"
                             "ReplyMessageID INT,"
                             "ForwardedFromID INT,"
                             "PostAuthor TEXT,"
                             "MediaID INT,"
                             "FOREIGN KEY (MediaID) REFERENCES Media(ID),"
                             "PRIMARY KEY (ID, ContextID)) WITHOUT ROWID")
            self.conn.commit()

    def dump_user(self, user_full, photo_id):
        #TODO: Use invalidation time
        """Dump a UserFull into the User table
        Params: UserFull to dump, MediaID of the profile photo in the DB"""
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
                  photo_id)
        try:
            self.cur.execute("INSERT INTO User VALUES (?,?,?,?,?,?,?,?,?)", values)
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

    def dump_filelocation(self, file_location):
        #TODO: Use invalidation time
        """Dump a FileLocation into the Media table"""
        values = (None,
                  file_location.local_id,
                  file_location.volume_id,
                  file_location.dc_id,
                  file_location.secret)
        try:
            self.cur.execute("INSERT INTO User VALUES (?,?,?,?,?)", values)
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            self.conn.rollback()
            logger.error("Integrity error: %s", str(error))
            raise

def test():
    """Enter an example user to test dump_user"""
    #TODO: real tests
    settings = {'invalidation time':5}
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
