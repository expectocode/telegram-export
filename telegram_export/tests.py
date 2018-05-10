import asyncio
import configparser
import random
import shutil
import string
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import utils
from downloader import Downloader
from dumper import Dumper
from telethon import TelegramClient, utils as tl_utils
from telethon.errors import (
    PhoneNumberOccupiedError, SessionPasswordNeededError
)
from telethon.extensions import markdown
from telethon.tl import functions, types

from formatters import BaseFormatter

# Configuration as to which tests to run
ALLOW_NETWORK = False


def gen_username(length):
    """Generates a random username of max length "length" (minimum 4)"""
    letters = string.ascii_letters + string.digits
    return 'exp_' + ''.join(random.choice(letters) for _ in range(length - 4))


def login_client(client, username):
    """
    Logs-in the given client and sets the desired username.

    This method will sign up, sign in, or delete existing 2FA-protected
    accounts as required.
    """
    client.session.set_dc(0, '149.154.167.40', 80)
    assert client.connect()
    phone = '+999662' + str(random.randint(0, 9999)).zfill(4)
    client.send_code_request(phone)
    while True:
        try:
            print('Signing up as', phone)
            client.sign_up('22222', username, 'User')
            break
        except PhoneNumberOccupiedError:
            try:
                print('Signing in as', phone)
                client.sign_in(phone, '22222')
                break
            except SessionPasswordNeededError:
                print('Occupied', phone, 'had password! Deleting!')
                client(functions.account.DeleteAccountRequest(''))

    print('Changing', phone, 'username to', username)
    client(functions.account.UpdateUsernameRequest(username))


class TestDumpAll(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dumper_config = {'DBFileName': 'test_db', 'OutputDirectory': 'test_work_dir',
                             'MaxSize': 0}
        # TODO test with different configurations

        assert not Path(cls.dumper_config['OutputDirectory']).exists()

        Path(cls.dumper_config['OutputDirectory']).mkdir()

        config = configparser.ConfigParser()
        config.read('config.ini')
        config = config['TelegramAPI']

        cls.client = TelegramClient(None, config['ApiId'], config['ApiHash'])
        login_client(cls.client, gen_username(10))

        dumper = Dumper(cls.dumper_config)
        dumper.check_self_user(cls.client.get_me().id)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dumper_config['OutputDirectory'])

    def test_interrupted_dump(self):
        """
        This method will ensure that all messages are retrieved even
        on weird conditions.
        """
        if not ALLOW_NETWORK:
            raise unittest.SkipTest('Network tests are disabled')

        dumper = Dumper(self.dumper_config)
        dumper.chunk_size = 1
        SEND, DUMP = True, False
        actions = (
            (3, SEND),
            (2, DUMP),
            (2, SEND),
            (2, DUMP),  # Actually one will be dumped then back to start
            (1, SEND),
            (2, DUMP),
            (1, SEND),
            (2, DUMP),  # Actually one will be saved and the other updated
            (2, SEND),
            (3, DUMP),
            (1, SEND),
            (1, DUMP),
            (1, DUMP),
        )

        self.client(functions.messages.DeleteHistoryRequest('me', 0))
        downloader = Downloader(self.client, self.dumper_config, dumper,
                                loop=asyncio.get_event_loop())

        which = 1
        for amount, what in actions:
            if what is SEND:
                print('Sending', amount, 'messages...')
                for _ in range(amount):
                    self.client.send_message('me', str(which))
                    which += 1
                    time.sleep(1)
            else:
                print('Dumping', amount, 'messages...')
                chunks = (amount + dumper.chunk_size - 1) // dumper.chunk_size
                dumper.max_chunks = chunks
                downloader.start('me')

        messages = self.client.get_message_history('me', limit=None)
        print('Full history')
        for msg in reversed(messages):
            print('ID:', msg.id, '; Message:', msg.message)

        print('Dumped history')
        fmt = BaseFormatter(dumper.conn)
        my_id = self.client.get_me().id
        dumped = list(fmt.get_messages_from_context(my_id, order='DESC'))
        for msg in dumped:
            print('ID:', msg.id, '; Message:', msg.text)

        print('Asserting dumped history matches...')
        assert len(messages) == len(dumped), 'Not all messages were dumped'
        assert all(a.id == b.id and a.message == b.text
                   for a, b in zip(messages, dumped)),\
            'Dumped messages do not match'

        print('All good! Test passed!')
        self.client.disconnect()

    def test_dump_methods(self):
        """Test dumper.dump_* works"""
        dumper = Dumper(self.dumper_config)
        message = types.Message(
            id=777,
            to_id=types.PeerUser(123),
            date=datetime.now(),
            message='Hello',
            out=True,
            via_bot_id=1000,
            fwd_from=types.MessageFwdHeader(
                date=datetime.now() - timedelta(days=1),
                from_id=321
            )
        )
        fwd_id = dumper.dump_forward(message.fwd_from)
        dumper.dump_message(message, 123, forward_id=fwd_id, media_id=None)

        message = types.Message(
            id=778,
            to_id=types.PeerUser(321),
            date=datetime.now(),
            message='Hello',
            out=False,
            via_bot_id=1000,
            media=types.MessageMediaPhoto(
                caption='Hi',
                ttl_seconds=40,
                photo=types.Photo(
                    id=2357,
                    access_hash=-123456789,
                    date=datetime.now(),
                    sizes=[
                        types.PhotoSize(
                            type='X',
                            w=100,
                            h=100,
                            size=100 * 100,
                            location=types.FileLocation(
                                dc_id=2,
                                volume_id=5,
                                local_id=7532,
                                secret=987654321
                            )
                        )
                    ]
                )
            )
        )
        loc = dumper.dump_media(message.media)
        dumper.dump_message(message, 123, forward_id=None, media_id=loc)
        dumper.dump_message_service(context_id=123, media_id=loc, message=types.MessageService(
            id=779,
            to_id=123,
            date=datetime.now(),
            action=types.MessageActionScreenshotTaken()
        ))

        me = types.User(
            id=123,
            is_self=True,
            access_hash=13515,
            first_name='Me',
            username='justme',
            phone='1234567'
        )
        dumper.dump_user(photo_id=None, user_full=types.UserFull(
            user=me,
            link=types.contacts.Link(
                my_link=types.ContactLinkContact(),
                foreign_link=types.ContactLinkContact(),
                user=me
            ),
            notify_settings=types.PeerNotifySettings(0, 'beep'),
            common_chats_count=3
        ))
        dumper.dump_chat(photo_id=None, chat=types.Chat(
            id=7264,
            title='Chat',
            photo=types.ChatPhotoEmpty(),
            participants_count=5,
            date=datetime.now() - timedelta(days=10),
            version=1
        ))

        channel = types.Channel(
            id=8247,
            title='Channel',
            photo=types.ChatPhotoEmpty(),
            username='justchannel',
            participants_count=17,
            date=datetime.now() - timedelta(days=5),
            version=7
        )
        channel_full = types.ChannelFull(
            id=8247,
            about='Just a Channel',
            read_inbox_max_id=1051,
            read_outbox_max_id=8744,
            unread_count=1568,
            chat_photo=types.PhotoEmpty(id=176489),
            notify_settings=types.PeerNotifySettingsEmpty(),
            exported_invite=types.ChatInviteEmpty(),
            bot_info=[]
        )
        dumper.dump_supergroup(channel_full, channel, photo_id=None)
        dumper.dump_channel(channel_full, channel, photo_id=None)

    def test_dump_msg_entities(self):
        """Show that entities are correctly parsed and stored"""
        message = types.Message(
            id=1,
            to_id=types.PeerUser(321),
            date=datetime.now(),
            message='No entities'
        )
        dumper = Dumper(self.dumper_config)
        fmt = BaseFormatter(dumper.conn)

        # Test with no entities
        dumper.dump_message(message, 123, None, None)
        dumper.commit()
        assert not next(fmt.get_messages_from_context(123, order='DESC')).formatting

        # Test with many entities
        text, entities = markdown.parse(
            'Testing message with __italic__, **bold**, inline '
            '[links](https://example.com) and [mentions](@hi), '
            'as well as `code` and ``pre`` blocks.'
        )
        entities[3] = types.MessageEntityMentionName(
            entities[3].offset, entities[3].length, 123
        )
        message.id = 2
        message.date -= timedelta(days=1)
        message.message = text
        message.entities = entities
        dumper.dump_message(message, 123, None, None)
        dumper.commit()
        msg = next(fmt.get_messages_from_context(123, order='ASC'))
        assert utils.decode_msg_entities(msg.formatting) == message.entities

    def test_formatter_get_chat(self):
        """
        Ensures that the BaseFormatter is able to fetch the expected
        entities when using a date parameter.
        """
        chat = types.Chat(
            id=123,
            title='Some title',
            photo=types.ChatPhotoEmpty(),
            participants_count=7,
            date=datetime.now(),
            version=1
        )
        dumper = Dumper(self.dumper_config)

        fmt = BaseFormatter(dumper.conn)
        for month in range(1, 13):
            dumper.dump_chat(chat, None, timestamp=int(datetime(
                year=2010, month=month, day=1
            ).timestamp()))
        dumper.commit()
        cid = tl_utils.get_peer_id(chat)
        # Default should get the most recent version
        date = fmt.get_chat(cid).date_updated
        assert date == datetime(year=2010, month=12, day=1)

        # Expected behaviour is to get the previous available date
        target = datetime(year=2010, month=6, day=29)
        date = fmt.get_chat(cid, target).date_updated
        assert date == datetime(year=2010, month=6, day=1)

        # Expected behaviour is to get the next date if previous unavailable
        target = datetime(year=2009, month=12, day=1)
        date = fmt.get_chat(cid, target).date_updated
        assert date == datetime(year=2010, month=1, day=1)

    def test_formatter_get_messages(self):
        """
        Ensures that the BaseFormatter is able to correctly yield messages.
        """
        dumper = Dumper(self.dumper_config)
        msg = types.Message(
            id=1,
            to_id=123,
            date=datetime(year=2010, month=1, day=1),
            message='hi'
        )
        for _ in range(365):
            dumper.dump_message(msg, 123, forward_id=None, media_id=None)
            msg.id += 1
            msg.date += timedelta(days=1)
            msg.to_id = 300 - msg.to_id  # Flip between two IDs
        dumper.commit()
        fmt = BaseFormatter(dumper.conn)

        # Assert all messages are returned
        assert len(list(fmt.get_messages_from_context(123))) == 365

        # Assert only messages after a date are returned
        min_date = datetime(year=2010, month=4, day=1)
        assert all(m.date >= min_date for m in fmt.get_messages_from_context(
            123, start_date=min_date
        ))

        # Assert only messages before a date are returned
        max_date = datetime(year=2010, month=4, day=1)
        assert all(m.date <= max_date for m in fmt.get_messages_from_context(
            123, end_date=max_date
        ))

        # Assert messages are returned in a range
        assert all(min_date <= m.date <= max_date for m in
                   fmt.get_messages_from_context(
                       123, start_date=min_date, end_date=max_date
                   ))

        # Assert messages are returned in the correct order
        desc = list(fmt.get_messages_from_context(123, order='DESC'))
        assert all(desc[i - 1] > desc[i] for i in range(1, len(desc)))

        asc = list(fmt.get_messages_from_context(123, order='ASC'))
        assert all(asc[i - 1] < asc[i] for i in range(1, len(asc)))


if __name__ == '__main__':
    unittest.main()
