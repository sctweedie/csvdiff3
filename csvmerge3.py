#!/usr/bin/python3

import sys
import click
import logging
from csvfile import *
from headers import Headers

class ConflictError(Exception):
    pass

class UnhandledError(Exception):
    pass

def choose3(LCAval, Aval, Bval, allow_conflict = False):
    """
    Given three arbitrary values representing some property of the LCA
    and A/B branches, choose an appropriate output value.

    The usual rules for 3-way merge are used:
    * if A has changed LCA and B has not, then inherit the change from
      A
    * if B has changed LCA and A has not, then inherit the change from
      B
    * If both A and B differ from the LCA value, then either
      Choose the A value (if "allow_conflict" is set or if both A and
      B have the same change), or
      Raise a ConflictError exception
    """
    if LCAval == Aval:
        return Bval
    if LCAval == Bval:
        return Aval
    if Aval == Bval:
        return Aval
    if allow_conflict:
        return Aval
    raise ConflictError

class __State:
    def __init__(self, file_LCA, file_A, file_B,
                 headers, stream, writer):
        self.file_LCA = file_LCA
        self.file_A = file_A
        self.file_B = file_B

        self.headers = headers
        self.stream = stream
        self.writer = writer

        self.cursor_LCA = Cursor(file_LCA)
        self.cursor_A = Cursor(file_A)
        self.cursor_B = Cursor(file_B)

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

    # What _is_ the target for the merge?  If there's a change in both
    # A and B then we preserve the order in A; so we only use B if LCA
    # and A are the same

    if key_LCA == key_A and key_B:
        target_key = key_B
    else:
        target_key = key_A


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

        return relevance_X

    elif relevance_Y == None:

        return relevance_Y

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

def merge_one_changed_A(state, key_LCA, key_A):
    """
    LCA and the B merge branch have the same key, but A has changed;
    merge in that change.
    """
    raise UnhandledError

def merge_one_changed_B(state, key_LCA, key_B):
    """
    LCA and the A merge branch have the same key, but B has changed;
    merge in that change.
    """
    raise UnhandledError

def merge_one_all_different(state, key_LCA, key_A, key_B):
    raise UnhandledError

def lookup_field(line, column):
    """
    Lookup the value of a given (numbered) column in a given Line

    Returns None if either the column or the line is None
    """
    if not line:
        return None
    if column == None:
        return None
    return line.row[column]

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

def merge_one_line(state, line_LCA, line_A, line_B):
    """
    Perform field-by-field merging of LCA, A and B versions of a given
    line, sharing a common primary key.

    Any or all of the files may be missing a value for one or more
    fields, in which case an empty string is used for those fields.
    """

    logging.debug("  Action: merge_one_line(LCA %s, A %s, B %s)" %
                  (format(line_LCA), format(line_A), format(line_B)))

    # First, check if the corresponding lines of each input file have
    # the exact same text.  If they are all the same, then that is our
    # next output line, and we will avoid reformatting.

    if changed_line_is_compatible(line_LCA, line_A) and \
       changed_line_is_compatible(line_LCA, line_B) and \
       changed_line_is_compatible(line_A, line_B):

        out_text = (line_A or line_B).text
        # Log the output without line-terminator
        logging.debug("  Writing exact text: %s" % out_text[0:-1])
        state.stream.write(out_text)
        return

    # do field-by-field merging
    row = []
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
            raise UnhandledError

        if value == None:
            value = ""

        row.append(value)

    logging.debug("  Writing row: %s" % row)

    state.writer.writerow(row)

def merge3(file_lca, file_a, file_b, key,
           output = sys.stdout,
           debug = True):
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

    # We do not currently do any merging of dialect; rather, we always
    # just preserve the dialect of the A-side (usually, mainline)
    # branch file

    dialect = csv.Sniffer().sniff(file_A.header.text)
    # The reader ignores line-terminators, so force that to unix-style by default
    dialect.lineterminator = "\n"
    writer = csv.writer(output, dialect=dialect)

    # If all three input files have the exact same header text, then
    # output the header as that text verbatim;
    #
    # otherwise, we output the intelligent merge of the differences

    if file_LCA.header.text == file_A.header.text == file_B.header.text:
        output.write(file_A.header.text)
    else:
        writer.writerow(headers.headers)

    # Initialise the merging state

    state = __State(file_LCA, file_A, file_B, headers, output, writer)

    while not state.EOF():
        merge3_next(state)

    state.cursor_LCA.assert_finished()
    state.cursor_A.assert_finished()
    state.cursor_B.assert_finished()

@click.command()

@click.argument("filename_LCA", type=click.File("rt"))
@click.argument("filename_A", type=click.File("rt"))
@click.argument("filename_B", type=click.File("rt"))
@click.option("-k", "--key", required=True)
@click.option("-d", "--debug", is_flag = True, default=False)

def merge3_cli(filename_lca, filename_a, filename_b, key, debug):
    merge3(filename_lca, filename_a, filename_b, key, debug)

if __name__ == "__main__":
    merge3_cli()
