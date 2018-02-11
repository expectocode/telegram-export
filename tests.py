import configparser
import random
import string
import time
import unittest
from datetime import datetime, timedelta

from telethon import TelegramClient, utils
from telethon.errors import (
    PhoneNumberOccupiedError, SessionPasswordNeededError
)
from telethon.tl import functions, types

import downloader
from dumper import Dumper


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
    def test_interrupted_dump(self):
        """
        This method will ensure that all messages are retrieved even
        on weird conditions. The process will go as follows to cover
        most corner cases:
            - 23 sent
            - 20 exported (missing  3)
            - 14 sent     (missing 17)
            - 3  exported (missing 14)
            - 2  sent     (missing 16)
            - 10 exported (missing  6)
            - 13 sent     (missing 19)
            -  6 exported (missing 13)
            - 13 exported
        """
        config = configparser.ConfigParser()
        config.read('config.ini')
        config = config['TelegramAPI']

        client = TelegramClient(None, config['ApiId'], config['ApiHash'])
        login_client(client, gen_username(10))
        # my_id = client.get_me().id
        my_id = 0

        dumper = Dumper({'DBFileName': ':memory:'})
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

        client(functions.messages.DeleteHistoryRequest('me', 0))

        which = 1
        for amount, what in actions:
            if what is SEND:
                print('Sending', amount, 'messages...')
                for _ in range(amount):
                    client.send_message('me', str(which))
                    which += 1
                    time.sleep(1)
            else:
                print('Dumping', amount, 'messages...')
                chunks = (amount + dumper.chunk_size - 1) // dumper.chunk_size
                dumper.max_chunks = chunks
                downloader.save_messages(client, dumper, 'me')

        messages = client.get_message_history('me', limit=None)
        print('Full history')
        for msg in reversed(messages):
            print('ID:', msg.id, '; Message:', msg.message)

        print('Dumped history')
        dumped = list(dumper.iter_messages(my_id))
        for msg in dumped:
            print('ID:', msg.id, '; Message:', msg.message)

        print('Asserting dumped history matches...')
        assert len(messages) == len(dumped), 'Not all messages were dumped'
        assert all(a.id == b.id and a.message == b.message
                   for a, b in zip(reversed(messages), dumped)),\
            'Dumped messages do not match'

        print('All good! Test passed!')
        client.disconnect()

    def test_dump_methods(self):
        dumper = Dumper({'DBFileName': ':memory:'})
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
        dumper.dump_message_service(media_id=loc, message=types.MessageService(
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


if __name__ == '__main__':
    unittest.main()
