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

        owner = TelegramClient(None, config['ApiId'], config['ApiHash'])
        owner_name = gen_username(10)
        login_client(owner, owner_name)

        slave = TelegramClient(None, config['ApiId'], config['ApiHash'])
        slave_name = gen_username(10)
        login_client(slave, slave_name)
        slave_id = utils.get_peer_id(owner.get_input_entity(slave_name))

        dumper = Dumper({})
        dumper.chunk_size = 10
        # (number of messages to handle, send (true) or dump (false))
        actions = (
            # True means out (send), False means in (dump)
            # No if we don't have them, yes if we do (upper = last action)
            (23, True),   # {NO:23}
            (20, False),  # {YES:20}{no:3}
            (14, True),   # {NO:14}{yes:20}{no:3}
            (3,  False),  # {no:14}{YES:23}
            (2,  True),   # {NO:16}{yes:23}
            (10, False),  # {YES:10}{no:6}{yes:23}
            (13, True),   # {NO:13}{yes:10}{no:6}{yes:23}
            (6,  False),  # {no:13}{YES:16 yes:23}
            (13, False),  # {YES:13 yes:39}
                          # {yes:52}
        )

        # Keep a array holding either None, False, True so we can visually
        # represent which wasn't sent, which was, and which we have dumped.
        visual = [None] * sum(n for n, send in actions if send)
        ok_visuals = [
            '_____________________________00000000000000000000000',
            '_____________________________11111111111111111111000',  # Starts
            '_______________0000000000000011111111111111111111000',
            '_______________0000000000000011111111111111111111111',  # Resumes
            '_____________000000000000000011111111111111111111111',
            '_____________111111111100000011111111111111111111111',  # Starts
            '0000000000000111111111100000011111111111111111111111',
            '0000000000000111111111111111111111111111111111111111',  # Resumes
            '1111111111111111111111111111111111111111111111111111'   # Starts
        ]

        print(owner_name, 'cleared the chat with', slave_name)
        owner(functions.messages.DeleteHistoryRequest(slave_name, 0))

        which = 1
        for amount, out in actions:
            if out:
                print(slave_name, 'is sending', amount, 'messages...')
                for i in range(amount):
                    if i % 2 == 0:
                        slave.send_message(owner_name, str(which))
                    else:
                        owner.send_message(slave_name, str(which))
                    visual[-which] = False
                    which += 1
                time.sleep(1)
            else:
                print(owner_name, 'is dumping', amount, 'messages...')
                chunks = (amount + dumper.chunk_size - 1) // dumper.chunk_size
                dumper.max_chunks = chunks
                downloader.save_messages(owner, dumper, slave_name)
                for msg in dumper.iter_messages(slave_id):
                    visual[-int(msg.message)] = True

            curr = ''.join('_' if x is None else str(int(x)) for x in visual)
            print('Current visual:', curr)
            assert curr == ok_visuals.pop(0)

        print(owner_name, 'full history with', slave_name)
        messages = owner.get_message_history(slave_name, limit=None)
        for msg in reversed(messages):
            print('ID:', msg.id, '; Message:', msg.message)

        print('Dumped history')
        dumped = list(dumper.iter_messages(slave_id))
        for msg in dumped:
            print('ID:', msg.id, '; Message:', msg.message)

        print('Asserting dumped history matches...')
        assert len(messages) == len(dumped), 'Not all messages were dumped'
        assert all(a.id == b.id and a.message == b.message
                   for a, b in zip(reversed(messages), dumped)),\
            'Dumped messages do not match'

        print('All good! Test passed!')
        owner.disconnect()
        slave.disconnect()

    def test_dump_methods(self):
        dumper = Dumper({})
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
        loc = dumper.dump_filelocation(downloader.get_file_location(message))
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
