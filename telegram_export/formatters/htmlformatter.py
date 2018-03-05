"""
Formatter to display paginated(?) HTML of a context.
Very much unfinished and needs a web designer to work on it.
"""
from . import BaseFormatter


class HtmlFormatter(BaseFormatter):
    """A Formatter class to generate HTML"""
    @staticmethod
    def name():
        return 'html'

    def output_header(self, file, context):
        """Output the header of the page. Context should be a namedtuple"""
        # TODO HTML
        print(self.get_display_name(context), file=file)

    def generate_message_html(self, message):
        """
        Return HTML for a message, showing reply message, forward headers,
        view count, post author, and media (if applicable).
        """
        # TODO HTML
        from_name = self.get_display_name(message.from_id) or "(???)"
        return "{}: {}".format(from_name, message.text)

    def _format(self, context_id, file, *args, **kwargs):
        """Format the given context as HTML and output to 'file'"""
        entity = self.get_entity(context_id)

        self.output_header(file, entity)
        for message in self.get_messages_from_context(context_id,
                                                      order='ASC'):
            print(self.generate_message_html(message), file=file)
