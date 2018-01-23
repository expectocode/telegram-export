import configparser
import random
import string
import time
import unittest

from telethon import TelegramClient, utils
from telethon.errors import (
    PhoneNumberOccupiedError, SessionPasswordNeededError
)
from telethon.tl.functions.account import (
    UpdateUsernameRequest, DeleteAccountRequest
)
from telethon.tl.functions.messages import (
    DeleteHistoryRequest
)

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
                client(DeleteAccountRequest(''))

    print('Changing', phone, 'username to', username)
    client(UpdateUsernameRequest(username))


class TestDumpAll(unittest.TestCase):
    def test_dump(self):
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

        dumper = Dumper({'DBFileName': 'UNIT-TEST'})
        dumper.chunk_size = 10
        # (number of messages to handle, send (true) or dump (false))
        actions = (
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

        print(owner_name, 'cleared the chat with', slave_name)
        owner(DeleteHistoryRequest(slave_name, 0))

        which = 1
        for amount, out in actions:
            if out:
                print(slave_name, 'is sending', amount, 'messages...')
                for i in range(amount):
                    if i % 2 == 0:
                        slave.send_message(owner_name, str(which))
                    else:
                        owner.send_message(slave_name, str(which))
                    which += 1
                time.sleep(1)
            else:
                print(owner_name, 'is dumping', amount, 'messages...')
                chunks = (amount + dumper.chunk_size - 1) // dumper.chunk_size
                dumper.max_chunks = chunks
                downloader.save_messages(owner, dumper, slave_name)

        print(owner_name, 'full history with', slave_name)
        messages = owner.get_message_history(slave_name, limit=None)
        for msg in reversed(messages):
            print('ID:', msg.id, '; Message:', msg.message)

        print('Dumped history')
        dumped = list(dumper.iter_messages(utils.get_peer_id(
            owner.get_input_entity(slave_name)
        )))
        for msg in reversed(dumped):
            print('ID:', msg.id, '; Message:', msg.message)

        print('Asserting dumped history matches...')
        assert len(messages) == len(dumped), 'Not all messages were dumped'
        assert all(a.id == b.id and a.message == b.message for a, b in zip(messages, dumped)), 'Dumped messages do not match'
        print('All good! Test passed!')

        owner.disconnect()
        slave.disconnect()


if __name__ == '__main__':
    unittest.main()
