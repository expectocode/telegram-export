"""A Formatter class to output pure text"""
import datetime

from formatters import BaseFormatter


class TextFormatter(BaseFormatter):
    """A Formatter class to output pure text"""

    def _format(self, context_id, file, *args, **kwargs):
        """Format the given context as text and output to 'file'"""
        entity = self.get_entity(context_id)
        name = self.get_display_name(entity) or 'unnamed'

        print('== Conversation with "{}" =='.format(name), file=file)
        for message in self.get_messages_from_context(context_id,
                                                      order='ASC'):
            try:
                who = self.get_display_name(self.get_user(message.from_id))
            except ValueError:
                who = '(???)'

            reply_sender, reply = self.get_reply(context_id, message)
            if reply:
                reply_sender = self.get_display_name(reply_sender) or '(???)'
                reply = ' (in reply to {}\'s: "{}")'.format(reply_sender, reply.text)
            else:
                reply = ''

            when = datetime.datetime.fromtimestamp(message.date)
            when = when.strftime('[%d.%m.%y %H.%M.%S]')
            print('{}, {}:{} {}'.format(
                who, when, reply or '', message.text
            ), file=file)
