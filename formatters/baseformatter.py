#!/usr/bin/env python3
"""Utility to extract data from a telegram-export database"""
import datetime
import math
import sqlite3
import sys
from pathlib import Path
from collections import namedtuple
from abc import abstractmethod
from io import TextIOWrapper

from telethon import utils
from telethon.tl import types

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
            raise TypeError('Invalid database object given: {}'.format(type(db)))

        self.our_userid = self.dbconn.execute(
            "SELECT UserID FROM SelfInformation").fetchone()[0]

    @staticmethod
    @abstractmethod
    def name():
        pass

    @staticmethod
    def ensure_id_marked(eid, etype):
        """
        Given an entity ID and type (PeerUser, PeerChat, PeerChannel), return
        the marked ID regardless of whether the ID is already marked.
        """
        if etype == types.PeerUser:
            return eid
        if etype == types.PeerChat:
            if eid < 0:
                return eid
            return -eid
        if etype == types.PeerChannel:
            if str(eid).startswith('-100'):
                return eid
            # Append -100 at start. See telethon/utils.py get_peer_id.
            return -(eid + pow(10, math.floor(math.log10(eid) + 3)))

    def get_display_name(self, entity):
        """
        Get the display name of a Chat, Channel, Supergroup, or User namedtuple,
        or a Bot API marked Context ID. Modeled on telethon/utils.py
        get_display_name. Return '' if there is no name, and raise ValueError
        if not passed one of the above types.
        """
        if isinstance(entity, User):
            if entity.first_name and entity.last_name:
                return '{} {}'.format(entity.first_name, entity.last_name)
            elif entity.first_name:
                return entity.first_name
            elif entity.last_name:
                return entity.last_name
            return ''

        if isinstance(entity, (Supergroup, Channel, Chat)):
            if entity.title:
                return entity.title
            return ''

        if isinstance(entity, int):
            return self.get_display_name(self.get_entity(entity))

        raise ValueError("Cannot get display name of a {} object".format(type(entity)))

    @staticmethod
    def get_timestamp(date):
        """Get a unix timestamp from an int, datetime, or date"""
        if date is None or isinstance(date, int):
            return date
        if isinstance(date, datetime.datetime):
            return date.timestamp()
        if isinstance(date, datetime.date):
            # Midnight at the start of that day
            return datetime.datetime.combine(date, datetime.time()).timestamp()

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

    def format(self, target, file, *args, **kwargs):
        """
        The public method to format target contexts and output them to 'file'.
        Target should be an individual Context ID. File can be a filename or file-like
        object. If it is falsey, it will be interpreted as stdout.
        """
        if not file:
            file = sys.stdout
        elif isinstance(file, (str, Path)):
            file = open(file, 'w')
        elif not isinstance(file, TextIOWrapper):  # Is there a better way?
            raise TypeError("Supplied file {} could not be interpreted as a file".format(
                file))

        with file:
            if isinstance(target, int):
                return self._format(target, file, *args, **kwargs)
            if isinstance(target, (User, Chat, Channel, Supergroup)):
                return self._format(target.id, file, *args, **kwargs)

        raise TypeError("target should be a context ID or context namedtuple")

    @abstractmethod
    def _format(self, context_id, file, *args, **kwargs):
        """
        An abstract method that should be implemented by formatters
        Context ID will always be a Bot API style ID. File will always be
        something like a file object or sys.stdout, suitable for usage with
        print(file=file).
        """
        # TODO provide a way to format many targets into one directory with one
        # method, and a format syntax to specify the name scheme of the output files.
        pass

    def get_messages_from_context(self, context_id, start_date=None, end_date=None,
                                  from_user_id=None, order='DESC'):
        """
        Yield Messages from a context. Start and end date should be UTC timestamps
        or datetime objects. Note that Channels will never yield any messages if
        from_user_id is set, as there is no FromID for Channel messages. Order
        should be ASC or DESC. Note that unlike the other methods, context_id
        *must* be in the Bot API format where Channel/Supergroup IDs start with
        -100 and old-style Chat IDs start with -.
        """
        start_date, end_date = self.get_timestamp(start_date), self.get_timestamp(end_date)
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
            "FROM Message {} ORDER BY Date {}".format(where, order.upper()),
            params
        )
        row = cur.fetchone()
        if not row:
            return
        out = self.our_userid == row[3]
        while row:
            msg = Message(*row, out)
            yield msg._replace(date=datetime.datetime.fromtimestamp(msg.date))
            row = cur.fetchone()
            if not row:
                return
            out = self.our_userid == row[3]

    def get_reply(self, context_id, message: Message):
        """
        Helper method to return a tuple consisting of the (User, Message)
        that the input message was replying to. Either the User or both
        will be None, if these haven't been dumped.
        """
        if not message.reply_message_id:
            return None, None

        msg = self.get_message_by_id(context_id, message.reply_message_id)
        if msg:
            try:
                return self.get_user(msg.from_id), msg
            except ValueError:
                return None, msg

        return None, None

    def get_message_by_id(self, context_id, msg_id):
        """
        Returns the unique message with the given context and message ID.
        Returns ``None`` if the message has not been dumped.
        """
        where, params = self._build_query(
            ('ContextID = ?', context_id),
            ('ID = ?', msg_id)
        )
        cur = self.dbconn.cursor()
        cur.execute(
            "SELECT ID, ContextID, Date, FromID, Message, ReplyMessageID, "
            "ForwardID, PostAuthor, ViewCount, MediaID, Formatting "
            "FROM Message {}".format(where), params
        )
        row = cur.fetchone()
        if row:
            out = self.our_userid == row[3]
            msg = Message(*row, out)
            return msg._replace(date=datetime.datetime.fromtimestamp(msg.date))

    def iter_context_ids(self):
        """
        Iterates over all the context IDs available. This method should
        be useful if one desires to format all the available conversations.
        """
        cur = self.dbconn.cursor()
        cur.execute('SELECT DISTINCT ContextID FROM Message')
        row = cur.fetchone()
        while row:
            yield row[0]
            row = cur.fetchone()

    def get_entity(self, context_id, at_date=None):
        """
        Return the entity (user, chat or channel) corresponding to this context
        ID, at the given date (like all the specific methods). Context ID must
        be marked in the Bot API style, as with get_messages_from_context.
        """
        peer_type = utils.resolve_id(context_id)[1]
        if peer_type == types.PeerUser:
            return self.get_user(context_id, at_date=at_date)
        elif peer_type == types.PeerChat:
            return self.get_chat(context_id, at_date=at_date)
        elif peer_type == types.PeerChannel:
            try:
                return self.get_supergroup(context_id, at_date=at_date)
            except ValueError:
                return self.get_channel(context_id, at_date=at_date)
        else:
            raise ValueError('Invalid ID {} given'.format(context_id))

    def get_user(self, uid, at_date=None):
        """
        Return the user with given ID or raise ValueError. If at_date is set,
        get the user as they were at the given date (to the best of our knowledge).
        If it is not set, get the user as we last saw them. at_date should be a UTC
        timestamp or datetime object.
        """
        at_date = self.get_timestamp(at_date)
        uid = self.ensure_id_marked(uid, types.PeerUser)
        cur = self.dbconn.cursor()
        query = (
            "SELECT ID, DateUpdated, FirstName, LastName, Username, "
            "Phone, Bio, Bot, CommonChatsCount, PictureID FROM User"
        )
        row = self._fetch_at_date(cur, query, uid, at_date)
        if not row:
            raise ValueError("No user with ID {} in database".format(uid))
        user = User(*row)
        return user._replace(date_updated=datetime.datetime.fromtimestamp(user.date_updated))

    def get_channel(self, cid, at_date=None):
        """
        Return the channel with given ID or raise ValueError. If at_date is set,
        get the channel as it was at the given date (to the best of our knowledge).
        at_date should be a UTC timestamp or datetime object.
        """
        at_date = self.get_timestamp(at_date)
        cid = self.ensure_id_marked(cid, types.PeerChannel)
        cur = self.dbconn.cursor()
        query = (
            "SELECT ID, DateUpdated, About, Title, Username, "
            "PictureID, PinMessageID FROM Channel"
        )
        row = self._fetch_at_date(cur, query, cid, at_date)
        if not row:
            raise ValueError("No channel with ID {} in database".format(cid))
        channel = Channel(*row)
        return channel._replace(date_updated=datetime.datetime.fromtimestamp(channel.date_updated))

    def get_supergroup(self, sid, at_date=None):
        """
        Return the supergroup with given ID or raise ValueError. If at_date is set,
        get the supergroup as it was at the given date (to the best of our knowledge).
        at_date should be a UTC timestamp or datetime object.
        """
        at_date = self.get_timestamp(at_date)
        sid = self.ensure_id_marked(sid, types.PeerChannel)
        cur = self.dbconn.cursor()
        query = (
            "SELECT ID, DateUpdated, About, Title, Username, "
            "PictureID, PinMessageID FROM Supergroup"
        )
        row = self._fetch_at_date(cur, query, sid, at_date)
        if not row:
            raise ValueError("No supergroup with ID {} in database".format(sid))
        supergroup = Supergroup(*row)
        return supergroup._replace(date_updated=datetime.datetime.fromtimestamp(
            supergroup.date_updated))

    def get_chat(self, cid, at_date=None):
        """
        Return the chat with given ID or raise ValueError. If at_date is set,
        get the chat as it was at the given date (to the best of our knowledge).
        at_date should be a UTC timestamp or datetime object.
        """
        at_date = self.get_timestamp(at_date)
        cid = self.ensure_id_marked(cid, types.PeerChat)

        cur = self.dbconn.cursor()
        query = (
            "SELECT ID, DateUpdated, Title, MigratedToID, PictureID FROM Chat"
        )
        row = self._fetch_at_date(cur, query, cid, at_date)
        if not row:
            raise ValueError("No chat with ID {} in database".format(cid))
        chat = Chat(*row)
        return chat._replace(date_updated=datetime.datetime.fromtimestamp(chat.date_updated))

    def get_media(self, mid):
        """Return the Media with given ID or raise ValueError."""
        cur = self.dbconn.cursor()
        cur.execute("SELECT ID, Name, MimeType, Size, ThumbnailID, Type, LocalID, "
                    "VolumeID, Secret, Extra FROM Media WHERE ID = ?", (mid,))
        row = cur.fetchone()
        if not row:
            raise ValueError("No media with ID {} in database".format(mid))
        return Media(*row)

# if __name__ == '__main__':
    # main()
