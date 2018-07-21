telegram-export
===============

.. figure:: https://user-images.githubusercontent.com/15344581/43033282-3eff18fc-8ce5-11e8-9994-fd1de40268e1.png
   :alt: Logo

A tool to download Telegram data (users, chats, messages, and media)
into a database (and display the saved data).

**Database schema:**

.. figure:: https://user-images.githubusercontent.com/15344581/37377008-44c93d20-271f-11e8-8170-5d6071a21b8f.png
   :alt: Schema image

Installation
============

The simplest way is to run ``sudo pip3 install --upgrade telegram_export``,
after which telegram-export should simply be available as a command: ``telegram-export``
in the terminal. That's it!

If you don't like using ``sudo pip``, you can use ``pip3 install --user telegram_export``,
but you'll have to add something like ``~/.local/bin/`` to your $PATH to get
the command available. If you don't want to add to PATH, you can also use
``python3 -m telegram_export`` anywhere instead of ``telegram-export``. You'll
have a similar issue if you're using a virtualenv, but if you're using those
you probably know what you're doing anyway :)

Slow downloads?
---------------

You may also want to install ``cryptg`` with the same method for a speed
boost when downloading media. Telegram requires a lot of encryption and
decryption and this can make downloading files especially slow unless
using a nice fast library like cryptg. One user reported a `speed
increase of
1100% <https://github.com/expectocode/telegram-export/issues/29>`__.

Usage
=====

First, copy config.ini.example (from GitHub) to ``~/.config/telegram-export/config.ini``
and edit some values. You'll probably need to create this folder. To write your
config whitelist, you may want to refer to the output of
``telegram-export --list-dialogs`` to get dialog IDs or
``telegram-export --search <query>`` to filter the results.

Then run ``telegram-export`` and allow it to dump data.

Full option listing:

.. code::

    usage: __main__.py [-h] [--list-dialogs] [--search-dialogs SEARCH_STRING]
                       [--config-file CONFIG_FILE] [--contexts CONTEXTS]
                       [--format {text,html}] [--download-past-media]

    Download Telegram data (users, chats, messages, and media) into a database
    (and display the saved data)

    optional arguments:
      -h, --help            show this help message and exit
      --list-dialogs        list dialogs and exit
      --search-dialogs SEARCH_STRING
                            like --list-dialogs but searches for a dialog by
                            name/username/phone
      --config-file CONFIG_FILE
                            specify a config file. Default config.ini
      --contexts CONTEXTS   list of contexts to act on eg --contexts=12345,
                            @username (see example config whitelist for full
                            rules). Overrides whitelist/blacklist.
      --format {text,html}  formats the dumped messages with the specified
                            formatter and exits.
      --download-past-media
                            download past media instead of dumping new data (files
                            that were seen before but not downloaded).


telegram-export vs `telegram-history-dump <https://github.com/tvdstaaij/telegram-history-dump>`__
=================================================================================================

    *(For brevity we'll just refer them to as "export" and "dump")*

-  SQLite instead of jsonlines allows for far more powerful queries and
   better efficiency but loses compatibility with text-manipulating UNIX
   tools as the data is not stored as text (or even more powerful tools
   like `jq <https://stedolan.github.io/jq/>`__).

-  export's stored data is less complicated than dump's json dumps

-  Support for saving the history of a person or other dialog, so you
   can see e.g. what their name was over time.

-  Using `telethon <https://github.com/LonamiWebs/Telethon>`__
   instead of `tg-cli <https://github.com/vysheng/tg>`__ allows
   support for newer Telegram features like pinned messages, admin logs,
   user bios, first-class support for supergroups and avoids the
   ``tg-cli`` bug which made dumping channels impossible, as well as
   several other ``tg-cli`` annoyances (such as being somewhat harder to
   install).

-  Newer and less mature than dump

-  No dedicated analysis program yet (dump has telegram-analysis and
   pisg)

-  Implemented features which dump does not support (incomplete list):

   -  Admin logs
   -  Dumping Users/Channels/Chats as their own entities, not just as
      message metadata. This allows things like user bios, channel
      descriptions and profile pictures.
   -  Pinned messages (dump kind of supports this, but only by saving a
      message replying to the pinned message with text 'pinned the
      message')
   -  Participant lists

-  Closer interaction with the Telegram API theoretically allows big
   speed improvements (Practical comparison of times soon™)

-  export's database file is bound to a user (like dump), and the
   program will exit if you login as another person to avoid mixing
   things up. If you do use export with multiple users, you should
   specify a different database for each user. You can easily select
   different config files through ``--config-file``.

Limitations
===========

-  Still being worked on. It dumps things, but the schema may change and we
   won't support old schema transitions.

-  Relies on `Telethon <https://github.com/LonamiWebs/Telethon>`, which is still pre-1.0.

-  Certain information is not dumped for simplicity's sake. For example,
   edited messages won't be re-downloaded and there is currently no
   support for multiple versions of a message in the db. However, this
   shouldn't be much of an issue, since most edits or deletions are
   legit and often to fix typos.

What does it do? Is it a bot?
=============================

It uses the Telegram API (what Telegram apps use), so it has access to
everything a Telegram app can do. This is why you need an API ID and API
hash to use it, and why one from Telegram Desktop will work. Since
normal clients need to download messages, media, users etc to display
them in-app, telegram-export can do the same, and save them into a nice
database.

So no, it's not really a bot, but it does use the same technology as
**userbots** in order to work. As far as we know, it won't get you banned from
using Telegram or anything like that.

Installation from source
========================

``git clone`` this repository, then ``python3 setup.py install``. You should
also read through the `Installation`_ section for related notes.
