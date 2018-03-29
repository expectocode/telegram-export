"""A Formatter class to output pure text"""
from . import BaseFormatter


class NlpFormatter(BaseFormatter):
    """A Formatter class to output only the text of messages,
    intended for natural language processing"""
    @staticmethod
    def name():
        return 'nlp'

    def _format(self, context_id, file, *args, **kwargs):
        """Format the given context as text and output to 'file'"""
        entity = self.get_entity(context_id)

        for message in self.get_messages_from_context(context_id,
                                                      order='ASC'):
            if not message.text or message.service_action is not None:
                continue
            print(message.text)
