#!/usr/bin/env python3
"""A module for dumping export data into the database"""
import sqlite3


class Dumper():
    """Class to interface with the database for exports"""

    def __init__(self, database_name='export'):
        self.conn = sqlite3.connect('{}.db'.format(database_name))
        with self.conn:
            self.cur = self.conn.cursor()

            # If this is a new database, populate it
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='User';")
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
                                 "ID INT PRIMARY KEY NOT NULL,"
                                 "FileName TEXT,"
                                 "TelegramReference TEXT NOT NULL) WITHOUT ROWID")
                self.cur.execute("CREATE TABLE User("
                                 "ID INT NOT NULL,"
                                 "DateUpdated INT NOT NULL,"
                                 "FirstName TEXT NOT NULL,"
                                 "LastName TEXT,"
                                 "Phone TEXT,"
                                 "Bio TEXT,"
                                 "PictureID INT NULL,"
                                 "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                                 "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

                self.cur.execute("CREATE TABLE Channel("
                                 "ID INT NOT NULL,"
                                 "DateUpdated INT NOT NULL,"
                                 "CreatorID INT,"
                                 "About TEXT,"
                                 # "Signatures INT,"
                                 "PictureID INT NULL,"
                                 "Title TEXT NOT NULL,"
                                 "Username TEXT,"
                                 "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                                 "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

                self.cur.execute("CREATE TABLE Supergroup("
                                 "ID INT NOT NULL,"
                                 "DateUpdated INT NOT NULL,"
                                 "CreatorID INT,"
                                 "About TEXT,"
                                 "PictureID INT NULL,"
                                 "Title TEXT NOT NULL,"
                                 "Username TEXT,"
                                 "FOREIGN KEY (PictureID) REFERENCES Media(ID),"
                                 "PRIMARY KEY (ID, DateUpdated)) WITHOUT ROWID")

                self.cur.execute("CREATE TABLE Chat("
                                 "ID INT NOT NULL,"
                                 "DateUpdated INT NOT NULL,"
                                 "CreatorID INT,"
                                 "PictureID INT NULL,"
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
                                 "MediaID INT NULl,"
                                 "FOREIGN KEY (MediaID) REFERENCES Media(ID),"
                                 "PRIMARY KEY (ID, ContextID)) WITHOUT ROWID")

if __name__ == '__main__':
    dumper = Dumper()
