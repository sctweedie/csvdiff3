from abc import abstractmethod
import re
import csv
import sys
import os
import difflib
import re
from functools import reduce

from .output import OutputDriver

class Diff2MarkdownOutputDriver(OutputDriver):
    class ColorMapper:
        RED = "${\\color{red}\\textsf{"
        GREEN = "${\\color{green}\\textsf{"
        CYAN = "${\\color{lightblue}\\textsf{"

        STYLE_BRIGHT = "\\textbf{"
        STYLE_NORMAL = "}"
        STYLE_RESET_ALL = "}}$"

    def __init__(self, *args, **kwargs):
        OutputDriver.__init__(self, *args, **kwargs)
        self.show_reordered_lines = kwargs['show_reordered_lines']
        self.preamble_extra_text = kwargs['preamble_extra_text']

    @staticmethod
    def quote_newlines(text, replacement = "\\\\n"):
        """
        Prepare a key for printing, replacing any EOL/newline
        sequences with "\n" to keep the output on a single line
        and escaping markdown symbols.
        """

        text = text.replace("|", "\\|")

        return OutputDriver.quote_newlines(text, replacement)

    def emit_preamble(self, state, options, file_LCA, file_A, file_B, file_common_name):
        # For 2-way diff we start the output with a standard
        # diff-style header

        all_headers = state.headers.map_all_headers()
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

        self.saved_preamble = ("$" + state.text_bold() +
                               f"{name} -k\"{key}\" " +
                               f"{file1} {file2}" + state.text_unbold() + "$\n\n")

        # If we have extra preamble text, we always emit it
        # immediately after the cmdline (to be consistent with "git
        # diff".)  But the following "---"/"+++" lines are still
        # conditional on there being additional diff text to display.

        if self.preamble_extra_text:
            self.flush_preamble()

            for line in self.preamble_extra_text.strip().split("\n"):
                self.stream.write("$" + state.text_bold() +
                                  line + state.text_unbold() + "$\n")

        self.saved_preamble += ("$" + state.text_bold() +
                                f"--- {file1}" + state.text_unbold() + "$\n" +
                                "$" + state.text_bold() +
                                f"+++ {file2}" + state.text_unbold() + "$\n"
                                "\n")

        self.saved_preamble += ("| " +
            " | ".join(" " for _ in self.all_headers) + " |\n|-" +
            "-|-".join("-" for _ in self.all_headers) + "-|\n"
        )

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
        key_text = "$" + state.text_bold() + key + state.text_unbold() + "$"

        if not line_LCA:
            # Line has been added
            self.stream.write(f"| {state.text_cyan()} +{line_A.linenr} " +
                              f"{state.text_reset()} | {key_text} |\n")
        elif not line_A:
            # Line has been deleted
            self.stream.write(f"| {state.text_cyan()} -{line_LCA.linenr} " +
                              f"{state.text_reset()} | {key_text} |\n")
        else:
            self.stream.write(f"| {state.text_cyan()} -{line_LCA.linenr} {state.text_reset()} " +
                              f"{state.text_cyan()} +{line_A.linenr} {state.text_reset()} | {key_text} |\n")

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
            prefix = "| |"
            key = state.cursor_A.current_key()

        elif not line_A:
            # For deleted lines, we need to write the old text, not the
            # new.
            line = line_LCA.replace("_", "\\\\_")
            colour = state.text_red()
            prefix = "| - |"
            key = state.cursor_LCA.current_key()
        else:
            line = line_A.replace("_", "\\\\_")
            colour = state.text_green()
            prefix = "| + |"
            key = state.cursor_A.current_key()

        self.flush_preamble()
        self.emit_line_numbers(state, line_LCA, line_A, key)

        # We want to quote newlines embedded within the CSV fields,
        # but the full-line text will also have a trailing newline
        # that we need to remove first.

        text = " | ".join(map(self.quote_newlines, line.row))

        self.stream.write(prefix + colour + text + state.text_reset() + "| \n")

    def _row_to_text(self, state, line_LCA, line_A):
        top_fields = []
        bottom_fields = []
        modified = False
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
                top_fields.append(self.quote_newlines(val_A))
                bottom_fields.append("")
                continue

            modified = True

            if val_LCA == None:
                if val_A == "":
                    # When inserting a column and a given line
                    # contains no data for that new column, don't emit
                    # a change field for that: it just clutters the
                    # output, especially when many lines are affected.
                    bottom_fields.append("")
                else:
                    bottom_fields.append(state.text_green() +
                                  f"+{self.quote_newlines(val_A)}+" +
                                  state.text_reset())
                top_fields.append("")
            elif val_A == None:
                if val_LCA == "":
                    # Likewise skip the change field when deleting a
                    # column if a line previously had no data for that
                    # column
                    top_fields.append("")
                else:
                    top_fields.append(state.text_red() +
                                  f"-{self.quote_newlines(val_LCA)}-" +
                                  state.text_reset())
                bottom_fields.append("")
            else:
                (old,new) = self.highlight_word_diff(state,
                                                     self.quote_newlines(val_LCA),
                                                     self.quote_newlines(val_A))
                top_fields.append(state.text_red() +
                              f"-{old}-" +
                              state.text_reset())
                bottom_fields.append(state.text_green() +
                              f"+{new}+" +
                              state.text_reset())

        line = "| " + " | ".join(top_fields) + " |\n| " + " | ".join(bottom_fields) + " |"
        line = re.sub("(.t?tps?)://", r"\1&#8203;://", line).replace("_", "\\\\_")
        return (line, modified)

    word_split_pattern = re.compile(r"(\W+)")

    def highlight_word_diff(self, state, old, new):
        old_words = self.word_split_pattern.split(old)
        new_words = self.word_split_pattern.split(new)

        opcodes = difflib.SequenceMatcher(a=old_words, b=new_words).get_opcodes()
        old_output = ""
        new_output = ""

        for tag,i1,i2,j1,j2 in opcodes:
            if tag == "replace" or tag == "delete":
                old_output += (state.text_bold() +
                               reduce(str.__add__, old_words[i1:i2]) +
                               state.text_unbold())
            elif tag == "equal":
                old_output += reduce(str.__add__, old_words[i1:i2])

            if tag == "replace" or tag == "insert":
                new_output += (state.text_bold() +
                               reduce(str.__add__, new_words[j1:j2]) +
                               state.text_unbold())
            elif tag == "equal":
                new_output += reduce(str.__add__, new_words[j1:j2])

        return (old_output, new_output)

    def emit_csv_row(self, state, line_LCA, line_A, line_B, row, row_key = None):
        key = row_key or state.cursor_A.current_key()

        # Special cases first:

        # Normally we get here only if we have partial updates within
        # a line.  Wholesale insert/delete of lines gets sent to
        # emit_csv_text() instead.
        #
        # But if we are *also* changing columns, then that gets sent
        # here instead; so we still check for insert/delete and send
        # an appropriate whole-line output in that case rather than
        # going to field-by-field output.

        if (not line_LCA) or (not line_A):
            self.emit_text(state, line_LCA, line_A, line_B, row)
            return

        # Now, check for headers.  If we're doing a merge3 then we
        # emit the headers always; but for diff, we want to suppress
        # headers unless there's an actual change in the columns.
        #
        # Normally, unchanged lines get handled by emit_text() instead
        # and we catch this in that method above.  *BUT* we may get
        # here if the text has changed (eg. quoting is different) but
        # the actual header columns are the same.
        #
        # Check for that, and don't output anything in that case.

        text,modified = self._row_to_text(state, line_LCA, line_A)

        if row_key == "<Column names>":
            if not modified:
                return

        self.flush_preamble()
        self.emit_line_numbers(state, line_LCA, line_A, key)

        self.stream.write(text + "\n")

    def emit_conflicts(self, state, line_LCA, line_A, line_B, conflicts):
        # A 2-way diff should never be able to produce conflicts!
        assert False
