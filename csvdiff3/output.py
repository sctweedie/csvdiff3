from abc import abstractmethod
import re
import csv
import sys
import os

class OutputDriver():
    """
    Abstract Base Class for merge output drivers.

    The merge3 run will output its merge results to an instance of an
    OutputDriver; the exact subclass chosen can format the output as
    it wishes, eg. to define the formatting of conflict blocks in a
    3-way merge or to present the results as a 2- or 3-way diff.
    """

    def __init__(self, stream, dialect_args, *args, **kwargs):
        self.stream = stream
        self.dialect_args = dialect_args

    def emit_preamble(self, state, options, file_LCA, file_A, file_B, file_common_name):
        pass

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
        self.show_reordered_lines = kwargs['show_reordered_lines']

    def emit_preamble(self, state, options, file_LCA, file_A, file_B, file_common_name):
        # For 2-way diff we start the output with a standard
        # diff-style header

        name = os.path.basename(sys.argv[0])
        file1 = file_LCA.reader.reader.file.stream.name
        file2 = file_A.reader.reader.file.stream.name

        if file_common_name:
            file1 = "a/" + file_common_name
            file2 = "b/" + file_common_name

        key = file_LCA.key
        self.stream.write(state.text_bold() +
                          f"{name} -k\"{key}\" " +
                          f"{file1} {file2}\n" +
                          state.text_bold() +
                          f"--- {file1}\n" +
                          state.text_bold() +
                          f"+++ {file2}\n" +
                          state.text_reset())

    def emit_line_numbers(self, state, line_LCA, line_A, key):
        """
        Write diff-specific line number information, indicating if a line
        has been added, removed or reordered
        """
        key_text = state.text_bold() + key + state.text_reset()

        if not line_LCA:
            # Line has been added
            self.stream.write(f"{state.text_cyan()}@@ +{line_A.linenr} @@" +
                              f"{state.text_reset()} {key_text}\n")
        elif not line_A:
            # Line has been deleted
            self.stream.write(f"{state.text_cyan()}@@ -{line_LCA.linenr} @@" +
                              f"{state.text_reset()} {key_text}\n")
        else:
            self.stream.write(f"{state.text_cyan()}@@ -{line_LCA.linenr} " +
                              f"+{line_A.linenr} @@{state.text_reset()} {key_text}\n")

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
            # so it's an unmodified line.
            #
            # Is the line in the expected order, ie. is the state
            # cursor at this line in all 3 files?  If so, then just
            # ignore it.
            if (state.cursor_LCA[0] == line_LCA and
                state.cursor_A[0] == line_A and
                state.cursor_B[0] == line_B):
                return

            # Special case: the header is not handled via the cursor
            # mechanism (it is pre-read and handled specially at
            # startup.)  But if we get a header here, it is clearly
            # not reordered, so skip it.  (Merged/changed headers will
            # still appear via emit_row(), not emit_text().)
            if text == state.file_A.header.text:
                return

            # It's out of order; do we want to show it?
            if not self.show_reordered_lines:
                return

            # Show out-of-order lines in normal colour
            line = line_LCA
            colour = ""
            prefix = " "
            key = state.cursor_LCA.current_key()

        elif not line_A:
            # For deleted lines, we need to write the old text, not the
            # new.
            line = line_LCA
            colour = state.text_red()
            prefix = "-"
            key = state.cursor_LCA.current_key()
        else:
            line = line_A
            colour = state.text_green()
            prefix = "+"
            key = state.cursor_A.current_key()

        self.emit_line_numbers(state, line_LCA, line_A, key)

        # We want to quote newlines embedded within the CSV fields,
        # but the full-line text will also have a trailing newline
        # that we need to remove first.

        text = self._row_to_text(state, line, line)

        self.stream.write(prefix + colour + text + state.text_reset() + "\n")

    def _row_to_text(self, state, line_LCA, line_A):
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
        return ",".join(fields)

    def emit_csv_row(self, state, line_LCA, line_A, line_B, row):
        key = state.cursor_A.current_key()
        self.emit_line_numbers(state, line_LCA, line_A, key)

        self.stream.write(" " +
                          self._row_to_text(state, line_LCA, line_A) +
                          "\n")

    def emit_conflicts(self, state, line_LCA, line_A, line_B, conflicts):
        # A 2-way diff should never be able to produce conflicts!
        assert False
