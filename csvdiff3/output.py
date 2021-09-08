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
    def emit_csv_row(self, state, line_LCA, line_A, line_B, row, row_key = None):
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

    def emit_csv_row(self, state, line_LCA, line_A, line_B, row, row_key = None):
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
        self.preamble_extra_text = kwargs['preamble_extra_text']

    def emit_preamble(self, state, options, file_LCA, file_A, file_B, file_common_name):
        # For 2-way diff we start the output with a standard
        # diff-style header

        all_headers = state.headers.map_all_headers(file_LCA.header.row,
                                                    file_A.header.row,
                                                    file_B.header.row)
        self.all_headers = all_headers

        name = os.path.basename(sys.argv[0])
        file1 = file_LCA.reader.reader.file.stream.name
        file2 = file_A.reader.reader.file.stream.name

        if file_common_name:
            file1 = "a/" + file_common_name
            file2 = "b/" + file_common_name

        key = file_LCA.key

        # Just save the preamble initially.  We will suppress it if we
        # find no diffs to print; it will get printed the first time
        # we find there's something else to emit.

        self.saved_preamble = (state.text_bold() +
                               f"{name} -k\"{key}\" " +
                               f"{file1} {file2}\n")

        # If we have extra preamble text, we always emit it
        # immediately after the cmdline (to be consistent with "git
        # diff".)  But the following "---"/"+++" lines are still
        # conditional on there being additional diff text to display.

        if self.preamble_extra_text:
            self.flush_preamble()

            for line in self.preamble_extra_text.strip().split("\n"):
                self.stream.write(state.text_bold() +
                                  line + "\n")

        self.saved_preamble += (state.text_bold() +
                                f"--- {file1}\n" +
                                state.text_bold() +
                                f"+++ {file2}\n" +
                                state.text_reset())

    def flush_preamble(self):
        if not self.saved_preamble:
            return

        self.stream.write(self.saved_preamble)
        self.saved_preamble = ""

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
            key = state.cursor_A.current_key()

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

        self.flush_preamble()
        self.emit_line_numbers(state, line_LCA, line_A, key)

        # We want to quote newlines embedded within the CSV fields,
        # but the full-line text will also have a trailing newline
        # that we need to remove first.

        text = ",".join(map(self.quote_newlines, line.row))

        self.stream.write(prefix + colour + text + state.text_reset() + "\n")

    def _row_to_text(self, state, line_LCA, line_A):
        fields = []
        for field in self.all_headers:
            # Columns may be added/removed so record which ones we
            # actually have as we do the lookup

            col_LCA = field.LCA_column
            col_A = field.A_column

            val_LCA = None if col_LCA == None else line_LCA.get_field(col_LCA)
            val_A = None if col_A == None else line_A.get_field(col_A)

            # We output just the text contents (eg. "text,"), not a
            # diff between contents (eg. "{-old_text-,+new_text+},")
            # in two cases:
            #
            # First, when the before/after contents are the same (LCA
            # == A contents); and second, when the *lines* are the
            # same.
            #
            # The second case matters if we are adding/removing
            # columns; in that case, we might find that one of the
            # fields is simply not present in the entire LCA or A
            # file, so can't be looked up for the first test.  This
            # second test catches that case when we know we're
            # emitting the whole line, not a field-by-field diff
            # (eg. when we're explicitly handling an inserted or
            # deleted line, but when the columns have also changed.)

            if val_LCA == val_A or line_LCA == line_A:
                fields.append(self.quote_newlines(val_A))

            elif val_LCA == None:
                if val_A == "":
                    # When inserting a column and a given line
                    # contains no data for that new column, don't emit
                    # a change field for that: it just clutters the
                    # output, especially when many lines are affected.
                    fields.append("")
                else:
                    fields.append("{" + state.text_green() +
                                  f"+{self.quote_newlines(val_A)}+" +
                                  state.text_reset() + "}")
            elif val_A == None:
                if val_LCA == "":
                    # Likewise skip the change field when deleting a
                    # column if a line previously had no data for that
                    # column
                    fields.append("")
                else:
                    fields.append("{" + state.text_red() +
                                  f"-{self.quote_newlines(val_LCA)}-" +
                                  state.text_reset() + "}")
            else:
                fields.append("{" + state.text_red() +
                              f"-{self.quote_newlines(val_LCA)}-" +
                              state.text_reset() + "," +
                              state.text_green() +
                              f"+{self.quote_newlines(val_A)}+" +
                              state.text_reset() + "}")
        return ",".join(fields)

    def emit_csv_row(self, state, line_LCA, line_A, line_B, row, row_key = None):
        key = row_key or state.cursor_A.current_key()

        # Special case first: normally we get here only if we have
        # partial updates within a line.  Wholesale insert/delete of
        # lines gets sent to emit_csv_text() instead.
        #
        # But if we are *also* changing columns, then that gets sent
        # here instead; so we still check for insert/delete and send
        # an appropriate whole-line output in that case rather than
        # going to field-by-field output.

        if (not line_LCA) or (not line_A):
            self.emit_text(state, line_LCA, line_A, line_B, row)
            return

        self.flush_preamble()
        self.emit_line_numbers(state, line_LCA, line_A, key)

        self.stream.write(" " +
                          self._row_to_text(state, line_LCA, line_A) +
                          "\n")

    def emit_conflicts(self, state, line_LCA, line_A, line_B, conflicts):
        # A 2-way diff should never be able to produce conflicts!
        assert False
