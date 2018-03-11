telegram-export
===============

A tool to download Telegram data (users, chats, messages, and media)
into a database (and display the saved data).

**Database schema:**

.. figure:: /schema.png
   :alt: Schema image

   Schema image

Installation
============

This project depends on
`Telethon <https://github.com/LonamiWebs/Telethon/tree/asyncio>`__'s
asyncio branch and `tqdm <https://github.com/tqdm/tqdm>`__. The easiest
way to install these is
``pip3 install --user --upgrade -r requirements.txt`` or
``pip install --upgrade --user tqdm telethon-aio``.

You may also want to install ``cryptg`` with the same method for a speed
boost when downloading media. Telegram requires a lot of encryption and
decryption and this can make downloading files especially slow unless
using a nice fast library like cryptg. One user reported a `speed
increase of
1100% <https://%20github.com/expectocode/telegram-export/issues/29>`__.

With these installed, you can simply ``git clone`` this repository and
go onto 'Usage'.

Usage
=====

First, copy config.ini.example to config.ini and edit some values. To
write your whitelist, you may want to refer to the output of
``./telegram-export --list-dialogs`` to get dialog IDs or
``./telegram-export --search <query>`` to filter the results. Then run
``./telegram-export`` and allow it to dump data.

telegram-export vs `telegram-history-dump <https://github.com/tvdstaaij/telegram-history-dump>`__
=================================================================================================

    *(For brevity we'll just refer them to as "export" and "dump")*

-  SQLite instead of jsonlines allows for far more powerful queries and
   better efficiency but loses compatibility with text-manipulating UNIX
   tools as the data is not stored as text, or even more powerful tools
   like ```jq`` <https://stedolan.github.io/jq/>`__.

-  export's stored data is less complicated than dump's json dumps

-  Support for saving the history of a person or other dialog, so you
   can see e.g. what their name was over time.

-  Using ```telethon`` <https://github.com/LonamiWebs/Telethon>`__
   instead of ```tg-cli`` <https://github.com/vysheng/tg>`__ allows
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

-  Currently sort of unfinished. It dumps things, but the schema may
   change and we won't support old schema transitions.

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
