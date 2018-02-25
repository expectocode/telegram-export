import os
import re

import datetime

from formatters import BaseFormatter


class TextFormatter(BaseFormatter):
    @staticmethod
    def _fmt_name(user):
        return '{} {}'.format(user.first_name, user.last_name or '').strip()

    def format(self, file, context_id=None):
        if not context_id:
            for context_id in self.iter_context_ids():
                self.format(file, context_id=context_id)
            return

        entity = self.get_entity(context_id)
        try:
            name = entity.title
        except AttributeError:
            name = self._fmt_name(entity)

        if not file or os.path.isdir(file):
            file = os.path.join(file or '',
                                re.sub(r'[<>:"/\\|?*]', '', name) + '.txt')

        if isinstance(file, str):
            close = True
            file = open(file, 'w', encoding='utf-8')
        else:
            close = False

        try:
            print('== Conversation with "{}" =='.format(name), file=file)

            # TODO Replies
            for message in self.get_messages_from_context(context_id,
                                                          order='ASC'):
                try:
                    who = self._fmt_name(self.get_user(message.from_id))
                except ValueError:
                    who = '(???)'

                when = datetime.datetime.fromtimestamp(message.date)
                when = when.strftime('[%d.%m.%y %H.%M.%S]')
                print('{}, {}: {}'.format(who, when, message.text), file=file)
        finally:
            if close:
                file.close()
