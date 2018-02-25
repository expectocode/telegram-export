telegram-export
===============

**Database schema:**

![Schema image](/schema.png)

Usage
=====

First, copy config.ini.example to config.ini and edit some values.
To write your whitelist, you may want to refer to the output of
`./telegram-export --list-dialogs` to get dialog IDs or
`./telegram-export --search <query>` to filter the results.
Then run `./telegram-export` and allow it to dump data.


telegram-export vs telegram-history-dump
========================================

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
  features like pinned messages and user bios, and avoids the `tg-cli` bug
  which made dumping channels impossible, as well as several other `tg-cli`
  annoyances (such as being somewhat harder to install).

- No support for service messages yet, which dump does support.

- export will dump participants lists, which dump does not do.

- export's database file is bound to an user, and the program will exit if
  you login as another person to avoid mixing things up. You should specify
  a different filename for the database for every user you plan on using.
  You can easily select different config files through `--config-file`.

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
