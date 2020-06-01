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
    def emit_text(self, state, line_LCA, line_A, line_B, text):
        """
        Emit a line of merged text.

        The text will reflect the final result of merge; if a line in
        LCA has been deleted entirely, the text will have the value
        None.

        The line_LCA, line_A and line_B will all represent lines being
        merged for this row.  They may be absent: for a newly-added
        line, line_LCA will be None; and for deleted lines, line_A or
        line_B (or both) will be None.  But any lines presented will
        have the same key and will have been merged by the time this
        method is called.
        """
        pass

    @abstractmethod
    def emit_csv_row(self, state, line_LCA, line_A, line_B, row):
        """
        Emit a row of merged CSV.

        We emit merges as CSV rows, rather than text, either:

        * when reformat_all is set (in which case we want to re-write
          the CSV entirely to refresh field quoting, or

        * when a 3-way merge has been required, so that we cannot use
          either the A or B original text verbatim.
        """
        pass

    @abstractmethod
    def emit_conflicts(self, state, line_LCA, line_A, line_B, conflicts):
        """
        Emit a row with conflicts.

        The conflicts object will include a list of those fields which
        could not be automatically merged, ie. which had conflicting
        updates in A and B."""
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

    def emit_text(self, state, line_LCA, line_A, line_B, text):
        # Do nothing if there is no text in the merged output
        # (ie. this line has been deleted.)
        if not text:
            return
        self.stream.write(text)

    def emit_csv_row(self, state, line_LCA, line_A, line_B, row):
        # Do nothing if there is no content in the merged output
        # (ie. this line has been deleted.)
        if not row:
            return
        self.writer.writerow(row)
        pass

    def emit_conflicts(self, state, line_LCA, line_A, line_B, conflicts):
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



class Diff2OutputDriver(OutputDriver):
    def __init__(self, *args, **kwargs):
        OutputDriver.__init__(self, *args, **kwargs)
        self.writer = csv.writer(self.stream, **self.dialect_args)

    def emit_line_numbers(self, line_LCA, line_A):
        """
        Write diff-specific line number information, indicating if a line
        has been added, removed or reordered
        """
        if not line_LCA:
            # Line has been added
            self.stream.write(f"@@ +{line_A.linenr} @@\n")
        elif not line_A:
            # Line has been deleted
            self.stream.write(f"@@ -{line_LCA.linenr} @@\n")
        else:
            self.stream.write(f"@@ -{line_LCA.linenr} +{line_A.linenr} @@\n")

    def emit_text(self, state, line_LCA, line_A, line_B, text):
        # We emit text, rather than rows, when there is no merging to
        # be done within the line itself.
        #
        # That happens in three cases:
        # 1. The line has been deleted, so there's nothing in A/B to compare against
        # 2. The line has been added, so there's nothing in LCA to compare against
        # 3. The line is unmodified
        #
        # We'll just ignore unmodified lines here.  (We *could* try to
        # track reordered lines but choose to ignore that at this
        # point.)

        if line_LCA and line_A:
            # If we have both lines present, it's not an add/delete,
            # so it's an unmodified line: just ignore it.
            return

        self.emit_line_numbers(line_LCA, line_A)

        # For deleted lines, we need to write the old text, not the
        # new.
        if not line_A:
            text = line_LCA.text
            colour = state.text_red()
            prefix = "-"
        else:
            colour = state.text_green()
            prefix = "+"

        # We want to quote newlines embedded within the CSV fields,
        # but the full-line text will also have a trailing newline
        # that we need to remove first.

        text = self.quote_newlines(text.strip("\n"))

        self.stream.write(prefix + colour + text + state.text_reset() + "\n")

    def emit_csv_row(self, state, line_LCA, line_A, line_B, row):
        self.emit_line_numbers(line_LCA, line_A)

        fields = []
        for field in state.headers.header_map:
            val_LCA = line_LCA.get_field(field.LCA_column, "")
            val_A = line_A.get_field(field.A_column, "")

            if val_LCA == val_A:
                fields.append(self.quote_newlines(val_A))
            else:
                fields.append("{" + state.text_red() +
                              f"-{self.quote_newlines(val_LCA)}-" +
                              state.text_reset() + "," +
                              state.text_green() +
                              f"+{self.quote_newlines(val_A)}+" +
                              state.text_reset() + "}")
        self.stream.write(" " + ",".join(fields) + "\n")

    def emit_conflicts(self, state, line_LCA, line_A, line_B, conflicts):
        # A 2-way diff should never be able to produce conflicts!
        assert False
