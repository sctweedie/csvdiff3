#!/usr/bin/python3

import sys
import os
import logging
import re
import random
import string

from .file import *
from .headers import Headers
from .tools.options import *
from .output import Merge3OutputDriver

class ConflictError(Exception):
    pass

class PrimaryKeyError(MergeFailedError):
    def __init__(self, message, *args):
        super(PrimaryKeyError, self).__init__(message, *args)

class __State:
    def __init__(self, file_LCA, file_A, file_B,
                 headers, output_driver,
                 colour = False,
                 reformat_all = False):

        self.file_LCA = file_LCA
        self.file_A = file_A
        self.file_B = file_B

        self.headers = headers
        self.output_driver = output_driver

        self.cursor_LCA = Cursor(file_LCA)
        self.cursor_A = Cursor(file_A)
        self.cursor_B = Cursor(file_B)

        self.file_has_conflicts = False

        self.colour = colour
        self.reformat_all = reformat_all

    def EOF(self):
        if not self.cursor_LCA.EOF():
            return False
        if not self.cursor_A.EOF():
            return False
        if not self.cursor_B.EOF():
            return False
        return True

    def advance_all(self):
        self.cursor_LCA.advance()
        self.cursor_A.advance()
        self.cursor_B.advance()

    def consume(self, key, line_LCA, line_A, line_B):
        self.cursor_LCA.consume(key, line_LCA)
        self.cursor_A.consume(key, line_A)
        self.cursor_B.consume(key, line_B)

    def text_if_colour_enabled(self, text):
        if not self.colour:
            return ""
        return text

    def text_red(self):
        return self.text_if_colour_enabled(self.output_driver.ColorMapper.RED)

    def text_green(self):
        return self.text_if_colour_enabled(self.output_driver.ColorMapper.GREEN)

    def text_cyan(self):
        return self.text_if_colour_enabled(self.output_driver.ColorMapper.CYAN)

    def text_bold(self):
        return self.text_if_colour_enabled(self.output_driver.ColorMapper.STYLE_BRIGHT)

    def text_unbold(self):
        return self.text_if_colour_enabled(self.output_driver.ColorMapper.STYLE_NORMAL)

    def text_reset(self):
        return self.text_if_colour_enabled(self.output_driver.ColorMapper.STYLE_RESET_ALL)

    @staticmethod
    def dump_one_cursor(name, cursor):
        logging.debug("cursor %s at linenr %d:" %
                      (name, cursor.linenr))

        # Dump the current line (unless EOF)
        if cursor.EOF():
            logging.debug("  EOF")

        else:

            line = cursor[0]
            try:
                key = line.row[cursor.file.key_index]
            except IndexError:
                key = ""

            logging.debug("  linenr %d, key %s, consumed %s" %
                          (line.linenr, key, line.is_consumed))

            if line.LCA_backlog_match:
                logging.debug("    has background match in LCA "
                              f"at line {line.LCA_backlog_match.linenr}")

            match_A = getattr(line, "backlog_match_in_A", None)
            match_B = getattr(line, "backlog_match_in_B", None)
            if match_A or match_B:
                logging.debug("    matches "
                              f"line {Line.linenr(match_A)} in A, "
                              f"line {Line.linenr(match_B)} in B")


        # And dump the backlog
        logging.debug(f"  backlog: {len(cursor.backlog)}")
        for key in cursor.backlog:
            logging.debug(f"  backlog key {key}")
            for line in cursor.backlog.getlist(key):
                logging.debug(f"  backlog linenr {line.linenr}, text '{line.text.strip()}' " +
                              f"is {'not ' if not line.is_consumed else ''}consumed")

    def dump_current_state(self):
        """
        On assert failure, dump the current merge state to the debug log.
        """
        self.dump_one_cursor("LCA", self.cursor_LCA)
        self.dump_one_cursor("A", self.cursor_A)
        self.dump_one_cursor("B", self.cursor_B)

        # If the user has a dump directory already created for
        # debugging, also dump the LCA/A/B file contents there.

        path = os.path.expanduser("~/.csvmerge3.dump/")
        if not os.path.exists(path):
            return

        prefix = ''.join(random.choices(string.ascii_uppercase
                                        + string.ascii_lowercase
                                        + string.digits, k=6))
        prefix = os.path.join(path, "csvmerge3-"+prefix)

        self.file_LCA.dump("LCA", prefix)
        self.file_A.dump("A", prefix)
        self.file_B.dump("B", prefix)

        print(f"csvmerge3: Files dumped to {prefix}-*.dump", file=sys.stderr)

class Conflict:
    """
    Record a single conflict during merging of a single line.
    We accumulate conflicts as we merge and then output the results at
    the end of the line.
    """
    def __init__(self, val_A, val_B, column):
        self.val_A = val_A
        self.val_B = val_B
        self.column = column

class Conflicts:
    """
    Maintain a set of conflicts while we merge a single line.
    """
    def __init__(self, line_LCA, line_A, line_B):
        self.line_LCA = line_LCA
        self.line_A = line_A
        self.line_B = line_B

        self.conflicts = []

    def __iter__(self):
        return iter(self.conflicts)

    def __bool__(self):
        return len(self.conflicts) > 0

    def add(self, conflict):
        self.conflicts.append(conflict)



def merge3_next(state):
    """
    Make one line of progress on the 3-way merge.

    Examines the next line of the LCA and both merge branches, and
    determines what progress can be made.

    This function will advance the cursor through the various input
    files as data gets handled successfully.
    """

    # First, we need to know the keys of each line

    key_LCA = state.cursor_LCA.current_key()
    key_A = state.cursor_A.current_key()
    key_B = state.cursor_B.current_key()

    line_LCA = state.cursor_LCA[0]
    line_A = state.cursor_A[0]
    line_B = state.cursor_B[0]

    logging.debug("Next iteration: lines/keys are "
                  f"{Line.linenr(line_LCA)} [{key_LCA}] "
                  f"{Line.linenr(line_A)} [{key_A}] "
                  f"{Line.linenr(line_B)} [{key_B}]")

    # Now the real work starts: figure how to handle the many, various
    # possibilities where the next lines in each source file may have
    # different keys.

    # First, do we have either the A or B next key in the LCA backlog?
    # If so, we have now found the match for a line in the LCA that
    # was previously deferred; we can emit it now.

    # NB. It's possible for both lines to match the backlog (eg. a
    # line in LCA has moved forward in both A and B.)

    if line_A and line_A.LCA_backlog_match:

        # The matching line is in the backlog, so it before the current cursor

        LCA_backlog_line = line_A.LCA_backlog_match

        logging.debug(f"  Action: match A in backlog (key {key_A}, "
                      f"LCA line {LCA_backlog_line.linenr})")

        assert LCA_backlog_line.linenr < state.cursor_LCA.linenr
        assert state.cursor_LCA.backlog[key_A] == LCA_backlog_line

        # NB. we must *NOT* use "A_match_in_B" here to determine if
        # there's a matching line in B to use for the merge.  That
        # value comes from find_next_matching_line(), which explicitly
        # skips over lines which already have a match in the backlog:
        # we depend on this to make sure that we don't match multiple
        # LCA lines to the same line in A or B.
        #
        # So we instead use any match in B that was discovered when
        # the LCA line was first pushed to the backlog.  We don't want
        # to repeat that search again and potentially come up with a
        # different line.

        backlog_match_in_B = LCA_backlog_line.backlog_match_in_B

        logging.debug(f"    Matches line {Line.linenr(backlog_match_in_B)} in B")
        merge_one_line(state, LCA_backlog_line, line_A, backlog_match_in_B)
        state.consume(key_A, LCA_backlog_line, line_A, backlog_match_in_B)
        return

    if line_B and line_B.LCA_backlog_match:

        # The matching line is in the backlog, so it before the current cursor

        LCA_backlog_line = line_B.LCA_backlog_match

        logging.debug(f"  Action: match B in backlog (key {key_B}, "
                      f"LCA line {LCA_backlog_line.linenr})")

        assert LCA_backlog_line.linenr < state.cursor_LCA.linenr
        assert state.cursor_LCA.backlog[key_B] == LCA_backlog_line

        backlog_match_in_A = LCA_backlog_line.backlog_match_in_A

        logging.debug(f"    Matches line {Line.linenr(backlog_match_in_A)} in A")
        merge_one_line(state, LCA_backlog_line, backlog_match_in_A, line_B)
        state.consume(key_B, LCA_backlog_line, backlog_match_in_A, line_B)
        return

    # Are all the keys the same?  Easy, we've matched the lines so
    # process them now.
    #
    # We test as soon as we have tested for backlog matches (we never
    # want to override matching decisions that have already been taken
    # for backlog lines), but before we do any other tests as this is
    # the common, performance-sensitive case when merging files that
    # are in largely the same order.

    if key_LCA == key_A == key_B:
        merge_one_line(state, line_LCA, line_A, line_B)
        state.consume(key_LCA, line_LCA, line_A, line_B)
        return

    A_match_in_LCA, A_distance_in_LCA = find_next_matching_line(key_A, state.cursor_LCA)
    A_match_in_B, A_distance_in_B = find_next_matching_line(key_A, state.cursor_B)

    B_match_in_LCA, B_distance_in_LCA = find_next_matching_line(key_B, state.cursor_LCA)
    B_match_in_A, B_distance_in_A = find_next_matching_line(key_B, state.cursor_A)

    # Next, we look to see if there is an insert or delete to process.
    # These do not involve matching lines between different files
    # (rather, they represent inability to do so); so they are easier
    # cases to eliminate first.

    if key_A and not A_match_in_LCA:
        # The key in A is not present in LCA: it's an insert.

        logging.debug(f"  Action: insert A (key {key_A})")
        merge_one_line(state, None, line_A, A_match_in_B)
        state.consume(key_A, None, line_A, A_match_in_B)
        return

    if key_B and not B_match_in_LCA:
        # The key at A is not an insert, but the key at B is

        logging.debug(f"  Action: insert B (key {key_B})")
        merge_one_line(state, None, B_match_in_A, line_B)
        state.consume(key_B, None, B_match_in_A, line_B)
        return

    # Not an insert... is there a delete?

    LCA_match_in_A, LCA_distance_in_A = find_next_matching_line(key_LCA, state.cursor_A)
    LCA_match_in_B, LCA_distance_in_B = find_next_matching_line(key_LCA, state.cursor_B)

    if not LCA_match_in_A:
        # The key in LCA is no longer findable in A.  It's a delete.

        logging.debug(f"  Action: delete A (key {key_LCA})")
        merge_one_line(state, line_LCA, None, LCA_match_in_B)
        state.consume(key_LCA, line_LCA, None, LCA_match_in_B)
        return

    if not LCA_match_in_B:
        # The key in LCA is present in A but not in B; again, it's a
        # delete.

        logging.debug(f"  Action: delete B (key {key_LCA})")
        merge_one_line(state, line_LCA, LCA_match_in_A, None)
        state.consume(key_LCA, line_LCA, LCA_match_in_A, None)
        return

    # We should have handled all deletes (key missing in either A or
    # B) and inserts (key missing in LCA) now, so at this point all
    # the keys should exist.

    assert key_LCA
    assert key_A
    assert key_B

    # It's not a delete: key_LCA is still present in A and B.
    # It's not an insert: key_A and key_B are also present in LCA.
    # It's not a backlog match: key_A and key_B have not yet been seen in LCA.
    #
    # So we're not sure what to output next.  Our options are:
    #
    # Emit LCA: no, because we're following the reordering of lines
    # from A and/or B.
    #
    # Emit A or B: If a line from later on in the file has been moved
    # backwards to the current position, then in order to reflect that
    # move, we should emit that line now.  We'll mark any forward
    # matches of the line as is_consumed to prevent them from being
    # handled again in the future.
    #
    # Skip the LCA and move it to the backlog: If the line at LCA has
    # been moved to later on in the file, then we need to push it to
    # the backlog and handle it later when we come across the matching
    # keys in A or B.  (NB. if the line has moved in only one of A or
    # B, then the other side of the merge will still match LCA, and it
    # also needs to be pushed to the backlog.)
    #
    # How do we know which case has occurred --- ie. whether a future
    # line of LCA has been moved backwards in A/B, or a current line
    # of LCA has been moved forwards?  They both look similar in that
    # a chunk of lines have been swapped:
    #
    #   LCA     A       B
    #   a       a       a
    #   b       b       c <---Cursor
    #   c       c       d
    #   d       d       e
    #   e       e       b <---New position in B of key at LCA/A
    #
    # In this case, we could either say that key "c" in file B has
    # been moved backwards, or that key "b" in file B has been moved
    # forward.  Both are legitimate statements; key "b" *has* been
    # moved forward, but as a consequence keys "c", "d" and "e" *have*
    # been moved backwards.
    #
    # We want to minimise the number of lines reordered in this case,
    # and resync the cursors between the files as rapidly as possible.
    # In this case, we can get a match for key "c" 3 lines in the
    # future if we move it to the backlog; but we get a match for key
    # "c" in *one* line if instead we move "b" to the backlog.  By
    # pushing the most distant match to the backlog, we get back in
    # sync between the files as quickly as possible.
    #
    # This is a "forward move" match resulting in "push to backlog"
    # action.
    #
    #   LCA     A       B
    #   a       a       a
    #   b       b       e <---Cursor
    #   c       c       b
    #   d       d       c
    #   e       e       d <---Old position in LCA/A of new key in B
    #
    # In this case, we have a match for key "e" 3 lines in the future
    # in LCA/A, and match for "b" one line in the future in file B.
    # "b" will naturally sync up fastest; "e" is the more distant
    # match so we assume that "e" is the key that has moved.  In this
    # case it has moved *backwards*, not forwards, so rather than push
    # it to the backlog, we emit it immediately.
    #
    # This is a "backwards move" match resulting in a "forced emit"
    # action which will consume lines from further on in one or two
    # files.

    # First, let's try to device whether the line at A is a forward or
    # backward move that we can handle now.

    if key_A != key_LCA:

        # The keys in LCA and A are different and we have already
        # handled backlog, so both distances should be non-None and
        # positive

        assert A_distance_in_LCA > 0
        assert LCA_distance_in_A > 0

        if LCA_distance_in_A > A_distance_in_LCA:

            # LCA key has moved significantly forward in A, and we can
            # resync key_A key in LCA sooner; treat this as a forward
            # move, so push LCA to the backlog (and B too if it has
            # the same key as LCA.)

            logging.debug(f"    Push LCA line {line_LCA.linenr} to backlog")
            state.cursor_LCA.move_to_backlog()

            if key_LCA == key_B:
                logging.debug(f"    Push B line {line_B.linenr} to backlog")
                state.cursor_B.move_to_backlog()

            # We also mark the future line in A as having a backlog
            # match, so that further forward searches for the same key
            # don't match the same line twice

            LCA_match_in_A.LCA_backlog_match = line_LCA
            logging.debug(f"    Set backlog match in A at line "
                          f"{Line.linenr(LCA_match_in_A)}")
            line_LCA.backlog_match_in_A = LCA_match_in_A

            # and we need to look forward for a similar matching line
            # in B that may need the same mark on it.

            if LCA_match_in_B:
                LCA_match_in_B.LCA_backlog_match = line_LCA
                logging.debug(f"    Found backlog match in B at line "
                              f"{Line.linenr(LCA_match_in_B)}")
            line_LCA.backlog_match_in_B = LCA_match_in_B

            return

        # key_LCA has moved forward a little but key_A has moved
        # backwards more; treat it as a backwards move, and do a
        # forced emit right now of the matching lines for key_A

        logging.debug(f"  Action: forced emit A ({key_A})")

        merge_one_line(state, A_match_in_LCA, line_A, A_match_in_B)
        state.consume(key_A, A_match_in_LCA, line_A, A_match_in_B)
        return

    # Final condition!  key_A == key_LCA, so key_B must be different;
    # something in B has moved.  Again, decide if it's a forward or
    # backward move.

    assert key_B != key_LCA

    # The keys in LCA and B are different and we have already
    # handled backlog, so both distances should be non-None and
    # positive

    assert B_distance_in_LCA > 0
    assert LCA_distance_in_B > 0

    if LCA_distance_in_B > B_distance_in_LCA:

        # LCA key has moved significantly forward in B, and we can
        # resync key_B key in LCA sooner; treat this as a forward
        # move, so push LCA to the backlog (and A too, as by now it
        # must have the same key as LCA.)

        logging.debug(f"    Push LCA line {key_LCA} to backlog")
        state.cursor_LCA.move_to_backlog()
        logging.debug(f"    Push A line {key_A} to backlog")
        state.cursor_A.move_to_backlog()

        # We also mark the future line in B as having a backlog
        # match, so that further forward searches for the same key
        # don't match the same line twice

        LCA_match_in_B.LCA_backlog_match = line_LCA
        logging.debug(f"    Set backlog match in B at line "
                      f"{Line.linenr(LCA_match_in_B)}")
        line_LCA.backlog_match_in_B = LCA_match_in_B

        # and we need to look forward for a similar matching line
        # in B that may need the same mark on it.

        if LCA_match_in_A:
            LCA_match_in_A.LCA_backlog_match = line_LCA
            logging.debug(f"    Found backlog match in A at line "
                          f"{Line.linenr(LCA_match_in_A)}")
        line_LCA.backlog_match_in_A = LCA_match_in_A

        return

    # key_LCA has moved forward a little but key_B has moved
    # backwards more; treat it as a backwards move, and do a
    # forced emit right now of the matching lines for key_B

    logging.debug(f"  Action: forced emit B ({key_B})")

    merge_one_line(state, B_match_in_LCA, B_match_in_A, line_B)
    state.consume(key_B, B_match_in_LCA, B_match_in_A, line_B)

    return


def find_next_matching_line(key, cursor):
    """
    Determine the distance into a file (searching forward from the
    given cursor) to the next match against a given key.  Used to
    determine which is the closest key to resync towards.

    Returns None if there is no key or no line found.

    If the key is already found in the backlog then there will be
    nothing appropriate to match further in the file, so return None.
    """

    if not key:
        return (None, None)
    line = cursor.find_next_match(key)
    if not line:
        return (None, None)

    distance = line.linenr - cursor.linenr
    return (line, distance)


def lookup_field(line, column):
    """
    Lookup the value of a given (numbered) column in a given Line

    Returns None if either the column or the line is None
    """

    # During merge, it is not guaranteed that each input file to the
    # merge has a matching line...

    if not line:
        return None

    # ...or that the input line has a column of the given name...

    if column == None:
        return None

    # ...or that the line in question bothers to supply every required
    # column

    try:
        return line.row[column]
    except IndexError:
        return None

def changed_line_is_compatible(before, after):
    """
    Test whether two versions of a Line are in conflict or not.  If
    either is None, then there is no conflict; otherwise we need to
    compare contents.
    """
    if not before:
        return True
    if not after:
        return True
    # It's faster to compare the whole line as a single string, and
    # most of the time this will get us the right answer
    if before.text == after.text:
        return True
    # but if the text mismatches, we still have to compare the decoded
    # fields one by one
    if before.row == after.row:
        return True
    return False

def choose3(LCAval, Aval, Bval):
    """
    Given three arbitrary values representing some property of the LCA
    and A/B branches, choose an appropriate output value.

    The usual rules for 3-way merge are used:
    * if A has changed LCA and B has not, then inherit the change from
      A
    * if B has changed LCA and A has not, then inherit the change from
      B
    * If both A and B differ from the LCA value but A and B are the
      same, then choose that value.
    * If A and B are different from each other and different from LCA,
      then we we have a conflict.  This also includes the case where
      either A or B is None, indicating that the row was deleted on
      one side but modified on the other.

      Raise a ConflictError exception in this case.
    """
    if LCAval == Aval:
        return Bval
    if LCAval == Bval:
        return Aval
    if Aval == Bval:
        return Aval
    raise ConflictError

def merge_one_line(state, line_LCA, line_A, line_B):
    """
    Perform field-by-field merging of LCA, A and B versions of a given
    line, sharing a common primary key.

    Any or all of the files may be missing a value for one or more
    fields, in which case an empty string is used for those fields.
    """

    logging.debug("  Action: merge_one_line(LCA %s, A %s, B %s)" %
                  (format(line_LCA), format(line_A), format(line_B)))

    # We call the merge function for deleted rows, just in case
    # the delete conflicts with an update/modify.
    #
    # But for delete, we do *not* write out the final row if there
    # is no conflict.

    is_delete = bool(line_LCA) and not (line_A and line_B)

    # Check that we have the right key on all three lines.  We
    # *really* do not want to merge the wrong lines by mistake!

    key_LCA = line_LCA and line_LCA.get_field(state.cursor_LCA.file.key_index)
    key_A = line_A and line_A.get_field(state.cursor_A.file.key_index)
    key_B = line_B and line_B.get_field(state.cursor_B.file.key_index)

    key = key_LCA or key_A or key_B

    if key_LCA:
        assert key_LCA == key
    if key_A:
        assert key_A == key
    if key_B:
        assert key_B == key

    # First, check if the corresponding lines of each input file have
    # the exact same text.  If they are all the same, then that is our
    # next output line, and we will avoid reformatting.

    if changed_line_is_compatible(line_LCA, line_A) and \
       changed_line_is_compatible(line_LCA, line_B) and \
       changed_line_is_compatible(line_A, line_B):

        if is_delete:
            logging.debug("  Skipping deleted row: %s" % line_LCA.row)
            # We will still send an empty line to the output driver so
            # that diff2/3 outputs can record the deleted line.
            state.output_driver.emit_text(state,
                                          line_LCA, line_A, line_B,
                                          None)
            return

        # Don't output un-reformatted existing text if we are forcing
        # a full reformat

        if not state.reformat_all:
            out_text = (line_A or line_B).text
            # Log the output without line-terminator
            logging.debug("  Writing exact text: %s" % out_text[0:-1])
            state.output_driver.emit_text(state,
                                          line_LCA, line_A, line_B,
                                          out_text)
            return

    # do field-by-field merging
    row = []
    conflicts = Conflicts(line_LCA, line_A, line_B)

    for map in state.headers.header_map:
        # The header_map maps columns in the output to the correct
        # columns in the various source files
        value_LCA = lookup_field(line_LCA, map.LCA_column)
        value_A = lookup_field(line_A, map.A_column)
        value_B = lookup_field(line_B, map.B_column)

        try:
            value = choose3(value_LCA, value_A, value_B)
        except ConflictError:
            # Need to emit a conflict marker in the output here
            conflicts.add(Conflict(value_A, value_B, map))
            value = "<conflict>"

        if value == None:
            value = ""

        row.append(value)

    if conflicts:
        logging.debug("  Writing conflicts: %s" % row)
        state.output_driver.emit_conflicts(state,
                                           line_LCA, line_A, line_B,
                                           conflicts)
        state.file_has_conflicts = True

    else:

        # We call the merge function for deleted rows, just in case
        # the delete conflicts with an update/modify.
        #
        # But for delete, we do *not* write out the final row if there
        # is no conflict.

        if is_delete:
            logging.debug("  Skipping deleted row: %s" % row)
            return

        logging.debug("  Writing row: %s" % row)
        state.output_driver.emit_csv_row(state,
                                         line_LCA, line_A, line_B,
                                         row)

# Check whether a proposed primary key is valid for all input files.
# The key must be present in each non-empty input file.

def key_is_valid(key, file_LCA, file_A, file_B):
    for file in file_LCA, file_A, file_B:
        if file.reader.empty:
            continue
        if key not in file.header.row:
            return False

    return True

def setup_primary_key(keys, headers, file_LCA, file_A, file_B):
    """
    Set up the primary key.  If the user has selected multiple
    possible keys or requested automatic key guessing, we need to
    determine the right key given the file contents.
    """

    for key in keys.split("|"):
        if key == "[auto]":
            key = guess_primary_key(headers, file_LCA, file_A, file_B)

        if key_is_valid(key, file_LCA, file_A, file_B):
            return key

    raise PrimaryKeyError(f"Cannot find a valid primary key in {key}")

def key_duplicates(key, file):
    """
    Return the number of duplicate values found for a candidate
    primary key in a given input file.
    """
    if file.reader.empty:
        return 0

    if key not in file.header.row:
        return 0

    # Figure where this key lies in this particular input file:

    column_index = file.header.row.index(next(x for x in file.header.row
                                              if x == key))
    key_values = set()

    # Now find all the distinct values this key has in this file

    lines = 0
    for line in file.lines[2:100]:

        # but don't crash on short lines that don't contain a value
        # for this particular key.

        try:
            key_values.add(line.row[column_index])
        except IndexError:
            pass
        lines += 1

    duplicates = lines - len(key_values)

    logging.debug(f"Found {len(key_values)} distinct values for key {key} "
                  f"out of {lines} lines in {file.filename}: score {duplicates}")

    return duplicates

def guess_primary_key(headers, file_LCA, file_A, file_B):
    """
    Perform automatic primary key guessing given the opened input
    files.

    We look at up-to the first 100 lines of each file (currently
    hard-coded), and select:

    * the first valid key (ie. the column is present in each non-empty
      file) that has 100% different values in each file, ie. zero
      duplicates
    * failing that, the key with the fewest duplicated values across
      all files, preferring earlier columns on tie
    """

    # First handle a special case.  If all inputs are empty, then
    # there are no potential primary keys present, so there's simply
    # no point in trying to guess which one is best.
    #
    # But at the same time, every possible primary key is compatible
    # with all of the input files; there's no data to be incompatible
    # with.
    #
    # So, just generate a fake primary key as our guess, using a
    # string that is easily identifiable if it turns up in a crash
    # report.

    if file_LCA.reader.empty and file_A.reader.empty and file_B.reader.empty:
        return "[nokeyspresent]"

    best_score = 1000
    best_key = None
    first_over_threshold = None

    logging.debug(f"Key guessing: starting to guess")
    for key in [x.name for x in headers.map_all_headers()]:
        logging.debug(f'Key guessing: trying key "{key}"')

        if not key_is_valid(key, file_LCA, file_A, file_B):
            continue

        # We want the key to be as good as possible over all the input
        # files, so we simply add the scores for each individual file.

        score = 0
        for file in file_LCA, file_A, file_B:
            score = score + key_duplicates(key, file)

        # If the key had no duplicates in any file, the key is perfect, so just use it.

        if score == 0:
            return key

        # Chose a new best-so-far key if the new key is *strictly*
        # better than all previous ones.  On tie, we always chose the
        # earlier key.

        if score < best_score:
            best_score = score
            best_key = key

        #if score > 90 and not first_over_threshold:
        #    first_over_threshold = key

    if first_over_threshold:
        return first_over_threshold
    if best_key:
        return best_key

    raise PrimaryKeyError(f"Cannot guess a valid primary key")

def merge3(file_lca, file_a, file_b, key,
           output = sys.stdout,
           debug = True,
           colour = False,
           quote = "minimal",
           lineterminator = "unix",
           reformat_all = False,
           file_common_name = None,
           output_driver_class = Merge3OutputDriver,
           output_args = {},
           filename_LCA = "LCA", filename_A = "A", filename_B = "B"):
    """
    Perform a full 3-way merge on 3 given CSV files, using the given
    column name as a primary key.
    """

    if debug:
        logging.basicConfig(filename = "DEBUG.log", level = logging.DEBUG)
        logging.debug("Started new run.")

    file_LCA = CSVFile(file_lca, filename = filename_LCA)
    file_A = CSVFile(file_a, filename = filename_A)
    file_B = CSVFile(file_b, filename = filename_B)

    headers = Headers(file_LCA.header.row,
                      file_A.header.row,
                      file_B.header.row)

    key = setup_primary_key(key, headers, file_LCA, file_A, file_B)

    file_LCA.setup_key(key)
    file_A.setup_key(key)
    file_B.setup_key(key)

    # Always reformat all output lines if the headers have changed at
    # all between files

    if headers.need_remapping:
        reformat_all = True

    # It would be great if we could reliably sniff dialect from the
    # input.
    #
    # But reading the header is not enough to establish that reliably
    # (eg. the reader ignores line termination and cannot robustly
    # detect escapechar), and if we don't set these, output may fail
    # later on.
    #
    # So instead, just force the dialect for the output.  We can add
    # additional options here later.

    # Collect dialect options into the shared tools Option class

    options = Options()
    options.quote = quote
    options.lineterminator = lineterminator

    dialect_args = options.csv_kwargs()

    # Initialise the merging state

    output_driver = output_driver_class(output, dialect_args, **output_args)
    state = __State(file_LCA, file_A, file_B, headers, output_driver,
                    colour = colour,
                    reformat_all = reformat_all)

    output_driver.emit_preamble(state, options,
                                file_LCA, file_A, file_B,
                                file_common_name)

    # If all three input files have the exact same header text, then
    # output the header as that text verbatim;
    #
    # otherwise, we output the intelligent merge of the differences

    if (not reformat_all) and \
       file_LCA.header.text == file_A.header.text == file_B.header.text:
        output_driver.emit_text(state,
                                file_LCA[1], file_A[1], file_B[1],
                                file_A.header.text)
    else:
        output_driver.emit_csv_row(state,
                                   file_LCA[1], file_A[1], file_B[1],
                                   headers.headers, row_key = "<Column names>")

    try:
        while not state.EOF():
            merge3_next(state)

        state.cursor_LCA.assert_finished()
        state.cursor_A.assert_finished()
        state.cursor_B.assert_finished()

    except:
        state.dump_current_state()
        raise

    if state.file_has_conflicts:
        return 1
    else:
        return 0
