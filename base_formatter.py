#!/usr/bin/env python3
"""Utility to extract data from a telegram-export database"""
from collections import namedtuple
import sqlite3


Message = namedtuple('Message', ['id', 'context_id', 'date', 'from_id', 'text',
                                 'reply_message_id', 'forward_id', 'post_author',
                                 'view_count', 'media_id', 'out'])
User = namedtuple('User', ['id', 'date_updated', 'first_name', 'last_name',
                           'username', 'phone', 'bio', 'bot', 'common_chats_count',
                           'picture_id'])
Channel = namedtuple('Channel', ['id', 'date_updated', 'about', 'title', 'username',
                                 'picture_id', 'pin_message_id'])
Supergroup = namedtuple('Supergroup', ['ID', 'date_updated', 'about', 'title',
                                       'username', 'picture_id', 'pin_message_id'])
Chat = namedtuple('Chat', ['id', 'date_updated', 'title', 'migrated_to_id',
                           'picture_id'])
Media = namedtuple('Media', ['id', 'name', 'mime_type', 'size', 'thumbnail_id',
                             'type', 'local_id', 'volume_id', 'secret', 'extra'])


class BaseFormatter:
    """
    A class to extract data from a given telegram-export database in the form
    of named tuples.
    """
    def __init__(self, db_name):
        self.dbconn = sqlite3.connect(db_name)
        self.our_userid = self.dbconn.execute(
            "SELECT UserID FROM SelfInformation").fetchone()[0]

    def get_messages_from_context(self, context_id, start_date=None, end_date=None,
                                  from_user_id=None, order='DESC'):
        """
        Yield Messages from a context. Start and end date should be UTC timestamps.
        Note that Channels will never yield any messages if from_user_id is set,
        as there is no FromID for Channel messages. Order is ASC or DESC"""
        order = order.upper()
        cur = self.dbconn.cursor()
        common_sql = ("SELECT ID, ContextID, Date, FromID, Message, ReplyMessageID, "
                      "ForwardID, PostAuthor, ViewCount, MediaID FROM Message "
                      "WHERE ContextID = ? {{where}} ORDER BY DATE {}").format(order)
        if from_user_id is None:
            if start_date is None and end_date is None:
                cur.execute(common_sql.format(where=''), (context_id,))
            elif start_date is None and end_date is not None:
                cur.execute(common_sql.format(where="AND Date < ?"),
                            (context_id, end_date))
            elif start_date is not  None and end_date is None:
                cur.execute(common_sql.format(where="AND Date > ?"),
                            (context_id, start_date))
            elif start_date is not None and end_date is not None:
                cur.execute(common_sql.format(where="AND Date < ? AND Date > ?"),
                            (context_id, end_date, start_date))
        else:
            if start_date is None and end_date is None:
                cur.execute(common_sql.format(where="AND FromID = ?"),
                            (context_id, from_user_id))
            elif start_date is None and end_date is not None:
                cur.execute(common_sql.format(where="AND Date < ? AND FromID = ?"),
                            (context_id, end_date, from_user_id))
            elif start_date is not  None and end_date is None:
                cur.execute(common_sql.format(where="AND Date > ? AND FromID = ?"),
                            (context_id, start_date, from_user_id))
            elif start_date is not None and end_date is not None:
                cur.execute(common_sql.format(
                    where="AND Date < ? AND Date > ? AND FromID = ?"),
                            (context_id, end_date, start_date, from_user_id))

        row = cur.fetchone()
        out = self.our_userid == row[3]
        while row:
            yield Message(*row, out)
            row = cur.fetchone()
            out = self.our_userid == row[3]

    def get_user(self, uid, at_date=None):
        """
        Return the user with given ID or raise ValueError. If at_date is set,
        get the user as they were at the given date (to the best of our knowledge).
        If it is not set, get the user as we last saw them.
        """
        cur = self.dbconn.cursor()
        if at_date is None:
            cur.execute("SELECT ID, DateUpdated, FirstName, LastName, Username, "
                        "Phone, Bio, Bot, CommonChatsCount, PictureID FROM User "
                        "WHERE ID = ? ORDER BY DateUpdated DESC", (uid,))
            row = cur.fetchone()
        else:  # Get the highest DateUpdated before at_date
            cur.execute("SELECT ID, DateUpdated, FirstName, LastName, Username, "
                        "Phone, Bio, Bot, CommonChatsCount, PictureID FROM User "
                        "WHERE ID = ? AND DateUpdated <= ? ORDER BY "
                        "DateUpdated DESC", (uid, at_date))
            row = cur.fetchone()
            if not row:  # Nothing found before at_date, so try after it
                cur.execute("SELECT ID, DateUpdated, FirstName, LastName, Username, "
                            "Phone, Bio, Bot, CommonChatsCount, PictureID FROM User "
                            "WHERE ID = ? AND DateUpdated > ? ORDER BY "
                            "DateUpdated ASC", (uid, at_date))
                row = cur.fetchone()
        if not row:
            raise ValueError("No user with ID {} in database".format(uid))
        return User(*row)

    def get_channel(self, id, at_date=None):
        """
        Return the channel with given ID or raise ValueError. If at_date is set,
        get the channel as it was at the given date (to the best of our knowledge)
        """
        cur = self.dbconn.cursor()
        if at_date is None:
            cur.execute("SELECT ID, DateUpdated, About, Title, Username, "
                        "PictureID, PinMessageID FROM Channel WHERE ID = ? "
                        "ORDER BY DateUpdated DESC", (id,))
            row = cur.fetchone()
        else:  # Get the highest DateUpdated before at_date
            cur.execute("SELECT ID, DateUpdated, About, Title, Username, "
                        "PictureID, PinMessageID FROM Channel WHERE ID = ?"
                        "AND DateUpdated <= ? ORDER BY DateUpdated DESC",
                        (id, at_date))
            row = cur.fetchone()
            if not row:  # Nothing found before at_date, so try after it
                cur.execute("SELECT ID, DateUpdated, About, Title, Username, "
                            "PictureID, PinMessageID FROM Channel WHERE ID = ?"
                            "AND DateUpdated > ? ORDER BY DateUpdated ASC",
                            (id, at_date))
                row = cur.fetchone()
        if not row:
            raise ValueError("No channel with ID {} in database".format(id))
        return Channel(*row)

    def get_supergroup(self, id, at_date=None):
        """
        Return the supergroup with given ID or raise ValueError. If at_date is set,
        get the supergroup as it was at the given date (to the best of our knowledge)
        """
        cur = self.dbconn.cursor()
        if at_date is None:
            cur.execute("SELECT ID, DateUpdated, About, Title, Username, "
                        "PictureID, PinMessageID FROM Supergroup WHERE ID = ? "
                        "ORDER BY DateUpdated DESC", (id,))
            row = cur.fetchone()
        else:  # Get the highest DateUpdated before at_date
            cur.execute("SELECT ID, DateUpdated, About, Title, Username, "
                        "PictureID, PinMessageID FROM Supergroup WHERE ID = ?"
                        "AND DateUpdated <= ? ORDER BY DateUpdated DESC",
                        (id, at_date))
            row = cur.fetchone()
            if not row:  # Nothing found before at_date, so try after it
                cur.execute("SELECT ID, DateUpdated, About, Title, Username, "
                            "PictureID, PinMessageID FROM Supergroup WHERE ID = ?"
                            "AND DateUpdated > ? ORDER BY DateUpdated ASC",
                            (id, at_date))
                row = cur.fetchone()
        if not row:
            raise ValueError("No supergroup with ID {} in database".format(id))
        return Supergroup(*row)

    def get_chat(self, id, at_date=None):
        """
        Return the chat with given ID or raise ValueError. If at_date is set,
        get the chat as it was at the given date (to the best of our knowledge)
        """
        cur = self.dbconn.cursor()
        if at_date is None:
            cur.execute("SELECT ID, DateUpdated, Title, MigratedToID, "
                        "PictureID FROM Chat WHERE ID = ? ORDER BY "
                        "DateUpdated DESC", (id,))
            row = cur.fetchone()
        else:  # Get the highest DateUpdated before at_date
            cur.execute("SELECT ID, DateUpdated, Title, MigratedToID, "
                        "PictureID FROM Chat WHERE ID = ? AND DateUpdated <= ? "
                        "ORDER BY DateUpdated DESC", (id, at_date))
            row = cur.fetchone()
            if not row:  # Nothing found before at_date, so try after it
                cur.execute("SELECT ID, DateUpdated, Title, MigratedToID, "
                            "PictureID FROM Chat WHERE ID = ? AND DateUpdated > ? "
                            "ORDER BY DateUpdated ASC", (id, at_date))
                row = cur.fetchone()
        if not row:
            raise ValueError("No chat with ID {} in database".format(id))
        return Chat(*row)

    def get_media(self, id):
        """Return the Media with given ID or raise ValueError."""
        cur = self.dbconn.cursor()
        cur.execute("SELECT ID, Name, MimeType, Size, ThumbnailID, Type, LocalID, "
                    "VolumeID, Secret, Extra FROM Media WHERE ID = ?", (id,))
        row = cur.fetchone()
        if not row:
            raise ValueError("No media with ID {} in database".format(id))
        return Media(*row)

# if __name__ == '__main__':
    # main()
