"""A Formatter class to output pure text"""
from . import BaseFormatter

UNKNOWN_USER_TEXT = '(???)'

class TextFormatter(BaseFormatter):
    """A Formatter class to output pure text"""
    @staticmethod
    def name():
        return 'text'

    def generate_message(self, message):
        """Generate the text for a given Message namedtuple"""
        who = self.get_display_name(
            self.get_user(message.from_id)) or UNKNOWN_USER_TEXT

        if message.service_action:
            return "Service action {}".format(message.service_action)

        if message.reply_message is not None:
            if message.reply_message == ():  # Unlikely, message not dumped
                reply, reply_sender = '???', '???'
            else:
                reply_sender = self.get_display_name(
                    message.reply_message.from_user) or UNKNOWN_USER_TEXT
                replytext = message.reply_message.text or ''
                reply = ' (in reply to {}\'s: "{}")'.format(
                    reply_sender, replytext)
        else:
            reply = ''

        when = message.date.strftime('[%d.%m.%y %H.%M.%S]')
        return '{}, {}:{} {}'.format(who, when, reply or '', message.text)

    def _format(self, context_id, file, *args, **kwargs):
        """Format the given context as text and output to 'file'"""
        entity = self.get_entity(context_id)
        name = self.get_display_name(entity) or 'unnamed'

        print('== Conversation with "{}" =='.format(name), file=file)
        for message in self.get_messages_from_context(context_id,
                                                      order='ASC'):
            print(self.generate_message(message))
