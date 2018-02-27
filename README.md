telegram-export
===============

A tool to download Telegram data (users, chats, messages, and media) into a
 database (and display the saved data).

**Database schema:**

![Schema image](/schema.png)

Usage
=====

First, copy config.ini.example to config.ini and edit some values.
To write your whitelist, you may want to refer to the output of
`./telegram-export --list-dialogs` to get dialog IDs or
`./telegram-export --search <query>` to filter the results.
Then run `./telegram-export` and allow it to dump data.


telegram-export vs [telegram-history-dump](https://github.com/tvdstaaij/telegram-history-dump)
==============================================================================================

> *(For brevity we'll just refer them to as "export" and "dump")*

- SQLite instead of jsonlines allows for far more powerful queries and better
  efficiency but loses compatibility with text-manipulating UNIX tools as the
  data is not stored as text, or even more powerful tools like
  [`jq`](https://stedolan.github.io/jq/).

- export's stored data is less complicated than dump's json dumps

- Support for saving the history of a person or other dialog, so you can see
  e.g. what their name was over time.

- Using [`telethon`](https://github.com/LonamiWebs/Telethon) instead of
  [`tg-cli`](https://github.com/vysheng/tg) allows support for newer Telegram
  features like pinned messages, admin logs, user bios, first-class support for
  supergroups and avoids the `tg-cli` bug which made dumping channels
  impossible, as well as several other `tg-cli` annoyances (such as being
  somewhat harder to install).

- No support for service messages yet, which dump does support.

- Newer and less mature than dump

- Implemented features which dump does not support (incomplete list):
	- Dumping Users/Channels/Chats as their own entities, not just as message
  metadata. This allows things like user bios, channel descriptions and profile
  pictures.
    - Pinned messages (dump kind of supports this, but only by saving a message
  replying to the pinned message with text 'pinned the message')

- Planned features which dump does not support (incomplete list):
    - participant lists
	- admin logs

- Closer interaction with the Telegram API theoretically allows big speed
  improvements (Practical comparison of times soon™)

- export's database file is bound to a user (like dump), and the program will
  exit if you login as another person to avoid mixing things up. If you do use
  export with multiple users, you should specify a different database for each
  user. You can easily select different config files through `--config-file`.

Limitations
===========

- Currently sort of unfinished. It dumps things, but the schema may change
  and we won't support old schema transitions. At the moment, we also do
  not yet dump admin logs or participant lists or a few other things which
  we plan to do.

- Certain information is not dumped for simplicity's sake. For example,
  edited messages won't be re-downloaded and there is currently no support
  for multiple versions of a message in the db. However, this shouldn't be
  much of an issue, since most edits or deletions are legit and often to
  fix typos.
