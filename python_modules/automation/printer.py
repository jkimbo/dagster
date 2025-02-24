import sys

from textwrap import TextWrapper

from six import StringIO

from dagster.utils.indenting_printer import IndentingPrinter


class IndentingBufferPrinter(IndentingPrinter):
    '''Subclass of IndentingPrinter wrapping a StringIO.'''

    def __init__(self, indent_level=4, current_indent=0):
        self.buffer = StringIO()
        self.printer = lambda x: self.buffer.write(x + '\n')
        super(IndentingBufferPrinter, self).__init__(
            indent_level=indent_level, printer=self.printer, current_indent=current_indent
        )

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception_value, _traceback):
        self.buffer.close()

    def read(self):
        '''Get the value of the backing StringIO.'''
        return self.buffer.getvalue()

    def block(self, text, prefix='', initial_indent=''):  # pylint: disable=arguments-differ
        '''Automagically wrap a block of text.'''
        wrapper = TextWrapper(
            width=self.line_length - len(self.current_indent_str),
            initial_indent=initial_indent,
            subsequent_indent=prefix,
            break_long_words=False,
            break_on_hyphens=False,
        )
        for line in wrapper.wrap(text):
            self.line(line)

    def write_header(self):
        self.line("'''NOTE: THIS FILE IS AUTO-GENERATED. DO NOT EDIT")
        self.blank_line()
        self.line('@generated')
        self.blank_line()
        self.line('Produced via:')
        self.line('python ' + '\n\t'.join('%s \\' % s for s in sys.argv))
        self.blank_line()
        self.line("'''")
        self.blank_line()
        self.blank_line()
