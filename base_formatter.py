#!/usr/bin/env python3
"""Utility to extract data from a telegram-export database"""
from collections import namedtuple
import sqlite3


Message = namedtuple('Message', (
    'id', 'context_id', 'date', 'from_id', 'text', 'reply_message_id',
    'forward_id', 'post_author', 'view_count', 'media_id', 'formatting', 'out'
))

User = namedtuple('User', (
    'id', 'date_updated', 'first_name', 'last_name', 'username', 'phone',
    'bio', 'bot', 'common_chats_count', 'picture_id'
))

Channel = namedtuple('Channel', (
    'id', 'date_updated', 'about', 'title', 'username', 'picture_id',
    'pin_message_id'
))

Supergroup = namedtuple('Supergroup', (
    'ID', 'date_updated', 'about', 'title', 'username', 'picture_id',
    'pin_message_id'
))

Chat = namedtuple('Chat', (
    'id', 'date_updated', 'title', 'migrated_to_id', 'picture_id'
))

Media = namedtuple('Media', (
    'id', 'name', 'mime_type', 'size', 'thumbnail_id', 'type', 'local_id',
    'volume_id', 'secret', 'extra'
))


class BaseFormatter:
    """
    A class to extract data from a given telegram-export database in the form
    of named tuples.
    """
    def __init__(self, db):
        if isinstance(db, str):
            self.dbconn = sqlite3.connect('file:{}?mode=ro'.format(db), uri=True)
        elif isinstance(db, sqlite3.Connection):
            self.dbconn = db
        else:
            raise TypeError('Invalid database object given: %s', type(db))

        self.our_userid = self.dbconn.execute(
            "SELECT UserID FROM SelfInformation").fetchone()[0]

    @staticmethod
    def _build_query(*args):
        """
        Helper method to build SQLite WHERE queries, automatically ignoring
        ``None`` values. The arguments should be tuples with two values the
        first being the name (e.g. "Date < ?") and the second the value.

        Returns a tuple consisting of (<where clause>, <args tuple>).
        """
        query = []
        param = []
        for arg in args:
            if arg[1] is not None:
                query.append(arg[0])
                param.append(arg[1])
        if query:
            return ' WHERE ' + ' AND '.join(query), tuple(param)
        else:
            return ' ', ()

    @classmethod
    def _fetch_at_date(cls, cur, query, eid, at_date):
        """
        Helper method around the common operation to fetch a type by its ID
        and a "DateUpdated" parameter.
        """
        where, query_params = cls._build_query(
            ('ID = ?', eid),
            ('DateUpdated <= ?', at_date)
        )
        cur.execute('{} {} ORDER BY DateUpdated DESC'
                    .format(query, where), query_params)
        row = cur.fetchone()
        if row:
            return row

        where, query_params = cls._build_query(
            ('ID = ?', eid),
            ('DateUpdated > ?', at_date)
        )
        cur.execute('{} {} ORDER BY DateUpdated ASC'
                    .format(query, where), query_params)
        return cur.fetchone()

    def get_messages_from_context(self, context_id, start_date=None, end_date=None,
                                  from_user_id=None, order='DESC'):
        """
        Yield Messages from a context. Start and end date should be UTC timestamps.
        Note that Channels will never yield any messages if from_user_id is set,
        as there is no FromID for Channel messages. Order is ASC or DESC.
        """
        where, params = self._build_query(
            ('ContextID = ?', context_id),
            ('Date > ?', start_date),
            ('Date < ?', end_date),
            ('FromID = ?', from_user_id)
        )

        cur = self.dbconn.cursor()
        cur.execute(
            "SELECT ID, ContextID, Date, FromID, Message, ReplyMessageID, "
            "ForwardID, PostAuthor, ViewCount, MediaID, Formatting "
            "FROM Message {} ORDER BY DATE {}".format(where, order.upper()),
            params
        )
        row = cur.fetchone()
        if not row:
            raise StopIteration
        out = self.our_userid == row[3]
        while row:
            yield Message(*row, out)
            row = cur.fetchone()
            if not row:
                raise StopIteration
            out = self.our_userid == row[3]

    def get_user(self, uid, at_date=None):
        """
        Return the user with given ID or raise ValueError. If at_date is set,
        get the user as they were at the given date (to the best of our knowledge).
        If it is not set, get the user as we last saw them.
        """
        cur = self.dbconn.cursor()
        query = (
            "SELECT ID, DateUpdated, FirstName, LastName, Username, "
            "Phone, Bio, Bot, CommonChatsCount, PictureID FROM User"
        )
        row = self._fetch_at_date(cur, query, uid, at_date)
        if not row:
            raise ValueError("No user with ID {} in database".format(uid))
        return User(*row)

    def get_channel(self, cid, at_date=None):
        """
        Return the channel with given ID or raise ValueError. If at_date is set,
        get the channel as it was at the given date (to the best of our knowledge)
        """
        cur = self.dbconn.cursor()
        query = (
            "SELECT ID, DateUpdated, About, Title, Username, "
            "PictureID, PinMessageID FROM Channel"
        )
        row = self._fetch_at_date(cur, query, cid, at_date)
        if not row:
            raise ValueError("No channel with ID {} in database".format(cid))
        return Channel(*row)

    def get_supergroup(self, sid, at_date=None):
        """
        Return the supergroup with given ID or raise ValueError. If at_date is set,
        get the supergroup as it was at the given date (to the best of our knowledge)
        """
        cur = self.dbconn.cursor()
        query = (
            "SELECT ID, DateUpdated, About, Title, Username, "
            "PictureID, PinMessageID FROM Supergroup"
        )
        row = self._fetch_at_date(cur, query, sid, at_date)
        if not row:
            raise ValueError("No supergroup with ID {} in database".format(sid))
        return Supergroup(*row)

    def get_chat(self, cid, at_date=None):
        """
        Return the chat with given ID or raise ValueError. If at_date is set,
        get the chat as it was at the given date (to the best of our knowledge)
        """
        cur = self.dbconn.cursor()
        query = (
            "SELECT ID, DateUpdated, Title, MigratedToID, PictureID FROM Chat"
        )
        row = self._fetch_at_date(cur, query, cid, at_date)
        if not row:
            raise ValueError("No chat with ID {} in database".format(cid))
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
