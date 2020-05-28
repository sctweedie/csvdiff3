from abc import abstractmethod
import re
import csv

class OutputDriver():
    """
    Abstract Base Class for merge output drivers.

    The merge3 run will output its merge results to an instance of an
    OutputDriver; the exact subclass chosen can format the output as
    it wishes, eg. to define the formatting of conflict blocks in a
    3-way merge or to present the results as a 2- or 3-way diff.
    """

    def __init__(self, stream, dialect_args):
        self.stream = stream
        self.dialect_args = dialect_args

    @abstractmethod
    def emit_text(self, state, line):
        pass

    @abstractmethod
    def emit_shared_line(self, state, line):
        pass

    @abstractmethod
    def emit_merged_line(self, state, line):
        pass

    @abstractmethod
    def emit_csv_row(self, state, row):
        pass

    @abstractmethod
    def emit_conflicts(self, state, conflicts):
        pass

    # Some common helper functions to assist output drivers with formatting

    newline_regexp = re.compile("\n|\r\n")

    @staticmethod
    def quote_newlines(text, replacement = "\\\\n"):
        """
        Prepare a key for printing, replacing any EOL/newline
        sequences with "\n" to keep the output on a single line.
        """
        return OutputDriver.newline_regexp.sub(replacement, format(text))


class Merge3OutputDriver(OutputDriver):
    def __init__(self, *args, **kwargs):
        OutputDriver.__init__(self, *args, **kwargs)
        self.writer = csv.writer(self.stream, **self.dialect_args)

    def emit_text(self, text):
        self.stream.write(text)

    def emit_shared_line(self, state, line):
        self.stream.write(text)

    def emit_merged_line(self, state, line):
        pass

    def emit_csv_row(self, state, row):
        self.writer.writerow(row)
        pass

    def emit_conflicts(self, state, conflicts):
        """
        Write a set of conflicts for a single line to the output.
        """

        # Side A first

        linestr = ">>>>>> %s %s\n" % \
            (state.cursor_A.file.filename,
             self.line_to_str(conflicts.line_A, state.cursor_LCA, state.cursor_A))
        self.stream.write(linestr)

        for c in conflicts:
            linestr = ">>>>>> %s = %s%s%s\n" % \
                (c.column.name,
                 state.text_red(),
                 self.quote_newlines(c.val_A),
                 state.text_reset()
                )
            self.stream.write(linestr)

        if conflicts.line_A:
            self.stream.write(state.text_red())
            self.stream.write(conflicts.line_A.text)
            self.stream.write(state.text_reset())

        # Side B next

        linestr = "====== %s %s\n" % \
            (state.cursor_B.file.filename,
             self.line_to_str(conflicts.line_B, state.cursor_LCA, state.cursor_B))
        self.stream.write(linestr)

        for c in conflicts:
            linestr = "====== %s = %s%s%s\n" % \
                (c.column.name,
                 state.text_green(),
                 self.quote_newlines(c.val_B),
                 state.text_reset()
                )
            self.stream.write(linestr)

        if conflicts.line_B:
            self.stream.write(state.text_green())
            self.stream.write(conflicts.line_B.text)
            self.stream.write(state.text_reset())

        self.stream.write(state.text_reset())
        linestr = "<<<<<<\n"

        self.stream.write(linestr)

    @staticmethod
    def line_to_str(line, cursor_LCA, cursor_line):
        if not line:
            return "Deleted @%d" % cursor_LCA.linenr
        return "@%d (%s)" % (line.linenr, line.row[cursor_line.file.key_index])


