import unittest
import socks
from telegram_export.utils import parse_proxy_str


class TestUtils(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_proxy_str(self):
        host = "127.0.0.1"
        port = 1080
        
        proxy = (socks.SOCKS5, host, port)
        proxy_str = "socks5://127.0.0.1:1080"
        self.assertEqual(parse_proxy_str(proxy_str), proxy)

        proxy_str = "http://127.0.0.1:1080"
        proxy = (socks.HTTP, host, port)
        self.assertEqual(parse_proxy_str(proxy_str), proxy)

        proxy_str = "socks4://login:password@127.0.0.1:1080"
        proxy = (socks.SOCKS4, host, port, True, "login", "password")
        self.assertEqual(parse_proxy_str(proxy_str), proxy)

        proxy_str = "bad_type://login:password@127.0.0.1:1080"
        with self.assertRaises(ValueError):
            parse_proxy_str(proxy_str)

        proxy_str = "bad_type://127.0.0.1"
        with self.assertRaises(ValueError):
            parse_proxy_str(proxy_str)

        proxy_str = "bad_type:127.0.0.1"
        with self.assertRaises(ValueError):
            parse_proxy_str(proxy_str)

        proxy_str = "127.0.0.1:1080"
        with self.assertRaises(ValueError):
            parse_proxy_str(proxy_str)
