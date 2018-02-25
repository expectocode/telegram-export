# telegram-export
---------------

**Database schema:**

![Schema image](/schema.png)

# Usage

First, copy config.ini.example to config.ini and edit some values. To write your whitelist, you may want to refer to the output of `./telegram-export --list-dialogs` to get dialog IDs or `./telegram-export --search <query>` to filter the results. Then run `./telegram-export` and allow it to dump data.

# telegram-export vs telegram-history-dump

 - SQLite instead of jsonlines allows for far more powerful queries and better efficiency but loses compatibility with text-manipulating UNIX tools as the data is not stored as text.

 - telegram-export's stored data is less complicated than history-dump's json dumps

 - Support for saving the history of a person or other dialog, so you can see eg. what their name was over time.

 - Using telethon instead of tg-cli allows support for newer Telegram features like pinned messages and user bios, and avoids the tg-cli bug which made dumping channels impossible, as well as several other tg-cli annoyances.

 - No support for service messages yet, which history-dump does support.

 - export will dump participants lists, which history-dump does not do.

# Limitations

 - Currently sort of unfinished. It dumps things, but the schema may change and we won't support old schema transitions. At the moment, we also do not yet dump admin logs or participant lists or a few other things which we plan to do.

 - Certain information is not dumped for simplicity's sake. For example, edited messages won't be re-downloaded and there is currently no support for multiple versions of a message in the db.

 - You cannot use the program as multiple users - it assumes that everything is from the 'viewpoint' of the same user. An easy workaround is to use a different database for each user, which can be achieved by using several config files and the --config-file option of the main program.
