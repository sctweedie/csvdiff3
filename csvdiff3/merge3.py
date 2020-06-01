#!/usr/bin/python3

import sys
import os
import logging
import re
import random
import string

from colorama import Fore, Style

from .file import *
from .headers import Headers
from .tools.options import Options
from .output import Merge3OutputDriver

class ConflictError(Exception):
    pass

class UnhandledError(Exception):
    pass

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
        return self.text_if_colour_enabled(Fore.RED)

    def text_green(self):
        return self.text_if_colour_enabled(Fore.GREEN)

    def text_cyan(self):
        return self.text_if_colour_enabled(Fore.CYAN)

    def text_bold(self):
        return self.text_if_colour_enabled(Style.BRIGHT)

    def text_reset(self):
        return self.text_if_colour_enabled(Style.RESET_ALL)

    @staticmethod
    def dump_one_cursor(name, cursor):
        logging.debug("cursor %s at linenr %d:" %
                      (name, cursor.linenr))

        if cursor.EOF():
            logging.debug("  EOF")
            return

        line = cursor[0]
        try:
            key = line.row[cursor.file.key_index]
        except IndexError:
            key = ""

        logging.debug("  linenr %d, key %s, consumed %s" %
                      (line.linenr, key, line.is_consumed))
        if key in cursor.backlog:
            for b in cursor.backlog[key]:
                if b is line:
                    logging.debug("  line found in backlog")

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

    logging.debug("Next iteration: keys are %s, %s, %s" % \
                  (key_LCA, key_A, key_B))

    # Are all the keys the same?  Easy, we've matched the lines so
    # process them now.

    if key_LCA == key_A == key_B:
        merge_same_keys(state, key_LCA)
        return

    # Now the real work starts: figure how to handle the many, various
    # possibilities where the next lines in each source file may have
    # different keys.

    # Before we start looking for ways to merge, resync the cursors:
    # attempt to move cursors forward harmlessly (moving lines to
    # backlog) without changing the results of the merge.
    #
    # If the resync does anything, call it progress and restart
    # matching on the new state.

    if resync_LCA(state, key_LCA, key_A, key_B):
        return

    # Resync will catch the case when the LCA key been deleted on both
    # sides; we should still check for a one-sided deleted and handle
    # any conflict that may occur from that

    # Maybe the line in the LCA has simply been deleted from A?

    if key_LCA != None and not state.cursor_A.find_next_match(key_LCA):
        return delete_one_A(state, key_LCA)

    # Or deleted from B?

    if key_LCA != None and not state.cursor_B.find_next_match(key_LCA):
        return delete_one_B(state, key_LCA)

    # So the key at LCA[0] is still present in both A and B; we're not
    # doing a delete so we need to output something.  Figure out what.
    #
    # First: do files A and B have the same next key?  If so, we have
    # a change that is common across both branch paths.

    if key_A == key_B:
        return merge_one_changed_AB(state, key_LCA, key_A)

    # Files LCA and B have the same next key but A is different?

    if key_LCA == key_B:
        return merge_one_changed_A(state, key_LCA, key_A)

    # Files LCA and a have the same next key but B is different?

    if key_LCA == key_A:
        return merge_one_changed_B(state, key_LCA, key_B)

    # Next keys are different in all 3 files

    return merge_one_all_different(state, key_LCA, key_A, key_B)


def resync_relevance(key, cursor):
    """
    Determine the distance into a file (searching forward from the
    given cursor) to the next match against a given key.  Used to
    determine which is the closest key to resync towards.

    Returns None if there is no key or no line found.

    If the key is already found in the backlog then there will be
    nothing appropriate to match further in the file, so return None.
    """

    if not key:
        return None
    line = cursor.find_next_match(key)
    if not line:
        return None

    distance = line.linenr - cursor.linenr
    if distance < 0:
        return None

    return distance

def resync_best_relevance(relevance_X, relevance_Y):
    """
    Compare two relevance distances and return the best.

    Ignores None values, except that if both X and Y are None, we
    return None.
    """

    # "relevance" is actually "distance to relevance", so lower values
    # mean more relevance

    if relevance_X == None:

        return relevance_Y

    elif relevance_Y == None:

        return relevance_X

    else:
        if relevance_X < relevance_Y:
            return relevance_X
        else:
            return relevance_Y

def resync_LCA(state, key_LCA, key_A, key_B):
    """
    Attempt to maintain cursor synchronisation between LCA and A/B, by
    pushing the head line of LCA onto the backlog if we are not going
    to need it soon.
    """

    # If there's nothing left in LCA, then there is nothing to do
    # here!

    if not key_LCA:
        return False

    # See "notes.txt" for a fuller explanation of the resync algorithm
    # and purpose.

    # First, find out how soon the LCA key might be relevant

    LCA_relevance_in_A = resync_relevance(key_LCA, state.cursor_A)
    LCA_relevance_in_B = resync_relevance(key_LCA, state.cursor_B)
    LCA_relevance = resync_best_relevance(LCA_relevance_in_A,
                                          LCA_relevance_in_B)

    # If we find LCA_key in neither A nor B, it has been deleted and
    # we can simply skip it

    if LCA_relevance == None:
        logging.debug("  Action: advance LCA (skipping %s)" % key_LCA)
        # Also pass the deleted line to the output driver, with empty
        # A and B lines and no text.
        state.output_driver.emit_text(state,
                                      state.cursor_LCA[0], None, None,
                                      None)
        state.cursor_LCA.advance()
        return True

    # Otherwise, we need to check if the LCA[0] line is more relevant
    # than A or B (for the purpose of matching against original
    # ordering)

    A_relevance_in_LCA = resync_relevance(key_A, state.cursor_LCA)
    B_relevance_in_LCA = resync_relevance(key_A, state.cursor_LCA)
    AB_relevance = resync_best_relevance(A_relevance_in_LCA,
                                         B_relevance_in_LCA)

    # If the AB keys are completely new and unknown in LCA, then they
    # are inserts; we'll consume those without advancing LCA, which
    # will give us another chance at getting in sync after the
    # insertion.

    if AB_relevance == None:
        return False

    # If LCA is no more relevant than AB keys, then do nothing;
    # there's nothing to be gained by disordering LCA

    if LCA_relevance <= AB_relevance:
        return False

    # The line at LCA is less relevant (higher relevance distance)
    # than the lines at AB, so push LCA[0] to the backlog

    logging.debug("    Push LCA line %s to backlog" % key_LCA)
    state.cursor_LCA.move_to_backlog()
    return True


def merge_same_keys(state, key_LCA):
    """
    All three branches have the same next key.
    """

    # This is the simplest case, with no special cases: perform an
    # intelligent 3-way merge on the current state and move to the
    # next line.

    merge_one_line(state,
                   state.cursor_LCA[0],
                   state.cursor_A[0],
                   state.cursor_B[0])
    state.advance_all()


def merge_one_changed_AB(state, key_LCA, key_AB):
    """
    The A and B merge branches both have the same key, which is
    different from what's on the LCA.
    """

    logging.debug("  Strategy: merge_one_changed_AB(%s,%s)" %
                  (key_LCA, key_AB))

    # Maybe the line in the LCA has simply been deleted?  If so, we
    # can skip past it and maybe resynchronise with that file later
    # on.

    if key_LCA and not (state.cursor_A.find_next_match(key_LCA) or \
                        state.cursor_B.find_next_match(key_LCA)):

        # We can't find the LCA key anywhere else, drop it.
        #
        # This should automatically pick up the case where A and B are
        # now empty.

        logging.debug("  Action: advance(LCA)")
        state.cursor_LCA.advance()
        return

    # So we have the same non-empty key on both A and B.  We still
    # need both line contents in case there are field changes, of
    # course.

    # A and B already have the right key, but the LCA line we need
    # might be in the backlog...
    line_LCA = state.cursor_LCA.find_next_match(key_AB)

    line_A = state.cursor_A[0]
    line_B = state.cursor_B[0]

    # And now do a three-way field-by-field merge.

    merge_one_line(state, line_LCA, line_A, line_B)
    state.consume(key_AB, line_LCA, line_A, line_B)


def delete_one_A(state, key_LCA):
    """
    LCA and B both contain key_LCA but it is absent in A; process as a
    deletion
    """

    # We can't find the LCA key anywhere in A, but it is still in
    # B.  Do a merge; if B has changed the contents of the line
    # and A deleted it, we still need to output a conflict,
    # otherwise the merge will output nothing.

    logging.debug("  Action: Merge delete A (missing key %s)" % key_LCA)

    line_LCA = state.cursor_LCA[0]
    line_B = state.cursor_B.find_next_match(key_LCA)

    merge_one_line(state, line_LCA, None, line_B)
    state.consume(key_LCA, line_LCA, None, line_B)
    return


def delete_one_B(state, key_LCA):
    """
    LCA and A both contain key_LCA but it is absent in B; process as a
    deletion
    """

    # We can't find the LCA key anywhere in B, but it is still in
    # A.  Do a merge; if A has changed the contents of the line
    # and B deleted it, we still need to output a conflict,
    # otherwise the merge will output nothing.

    logging.debug("  Action: Merge delete B (missing key %s)" % key_LCA)

    line_LCA = state.cursor_LCA[0]
    line_A = state.cursor_A.find_next_match(key_LCA)

    merge_one_line(state, line_LCA, line_A, None)
    state.consume(key_LCA, line_LCA, line_A, None)
    return


def merge_one_changed_A(state, key_LCA, key_A):
    """
    LCA and the B merge branch have the same key, but A has changed;
    merge in that change.
    """

    # It's not a delete (merge3_next already check for that), so we're
    # going to output a line with key_A; it has either been inserted
    # or moved to this position.  Let's find if there are other lines
    # in LCA or B that we can match against for a content merge.

    logging.debug("  Action: Merge A (key %s)" % key_A)

    line_A = state.cursor_A[0]
    match_LCA = state.cursor_LCA.find_next_match(key_A)
    match_B = state.cursor_B.find_next_match(key_A)

    merge_one_line(state, match_LCA, line_A, match_B)
    state.consume(key_A, match_LCA, line_A, match_B)

def merge_one_changed_B(state, key_LCA, key_B):
    """
    LCA and the A merge branch have the same key, but B has changed;
    merge in that change.
    """

    # We're going to output a line with key_B; it has either been
    # inserted or moved to this position.  Let's find if there are
    # other lines in LCA or A that we can match against for a content
    # merge.

    logging.debug("  Action: Merge B (key %s)" % key_B)

    line_B = state.cursor_B[0]
    match_LCA = state.cursor_LCA.find_next_match(key_B)
    match_A = state.cursor_A.find_next_match(key_B)

    merge_one_line(state, match_LCA, match_A, line_B)
    state.consume(key_B, match_LCA, match_A, line_B)


def merge_one_all_different(state, key_LCA, key_A, key_B):
    """
    LCA and the A and B merge branches all have different keys.
    Figure out what to do.
    """

    logging.debug("  Strategy: merge_one_all_different(%s,%s,%s)" %
                  (key_LCA, key_A, key_B))

    # We know that deletes have already been processed, so key_LCA
    # must already exist in both A and B.  We have no idea if key_A
    # exists in LCA or B yet, or key_B in LCA / A.

    # Because key_LCA is in both A and B, neither A nor B can be empty
    # (although the keys themselves may be empty strings)

    assert key_A != None
    assert key_B != None

    # Keys A and B both exist.  How do we decide which is better to
    # emit?

    # We will use the relevance algorithm described in notes.txt.
    # Find how close key_A's match is in B, and vice-versa; then
    # attempt to keep the closest match in the queue, to resync as
    # soon as possible.  (Ie. we emit the key that has the most
    # distant, least relevant match.)

    A_relevance_in_B = resync_relevance(key_A, state.cursor_B)
    B_relevance_in_A = resync_relevance(key_B, state.cursor_A)

    # We will emit A if it has no relevance (must be an insert
    # that is not also in B), or if it has less relevance than B

    if (A_relevance_in_B == None) or \
       (B_relevance_in_A != None and B_relevance_in_A < A_relevance_in_B):

        logging.debug("  Action: prefer A by relevance distance (%s)" % key_A)

        line_A = state.cursor_A[0]
        A_in_LCA = state.cursor_LCA.find_next_match(key_A)
        A_in_B = state.cursor_B.find_next_match(key_A)
        merge_one_line(state, A_in_LCA, line_A, A_in_B)
        state.consume(key_A, A_in_LCA, line_A, A_in_B)
        return

    else:
        logging.debug("  Action: prefer B by relevance distance (%s)" % key_B)

        line_B = state.cursor_B[0]
        B_in_LCA = state.cursor_LCA.find_next_match(key_B)
        B_in_A = state.cursor_A.find_next_match(key_B)
        merge_one_line(state, B_in_LCA, B_in_A, line_B)
        state.consume(key_B, B_in_LCA, B_in_A, line_B)
        return


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

    key_LCA = line_LCA and line_LCA.get_field(state.cursor_LCA.file.key_index, '')
    key_A = line_A and line_A.get_field(state.cursor_A.file.key_index, '')
    key_B = line_B and line_B.get_field(state.cursor_B.file.key_index, '')

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

def merge3(file_lca, file_a, file_b, key,
           output = sys.stdout,
           debug = True,
           colour = False,
           quote = "minimal",
           lineterminator = "unix",
           reformat_all = False,
           file_common_name = None,
           output_driver_class = Merge3OutputDriver):
    """
    Perform a full 3-way merge on 3 given CSV files, using the given
    column name as a primary key.
    """

    if debug:
        logging.basicConfig(filename = "DEBUG.log", level = logging.DEBUG)
        logging.debug("Started new run.")

    file_LCA = CSVFile(file_lca, key=key)
    file_A = CSVFile(file_a, key=key)
    file_B = CSVFile(file_b, key=key)

    headers = Headers(file_LCA.header.row,
                      file_A.header.row,
                      file_B.header.row)

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

    output_driver = output_driver_class(output, dialect_args)
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
                                   headers.headers)

    try:
        while not state.EOF():
            merge3_next(state)
    except AssertionError:
        state.dump_current_state()
        raise

    state.cursor_LCA.assert_finished()
    state.cursor_A.assert_finished()
    state.cursor_B.assert_finished()

    if state.file_has_conflicts:
        return 1
    else:
        return 0
