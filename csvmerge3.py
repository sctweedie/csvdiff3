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

    line_A = state.cursor_A[0]
    line_B = state.cursor_B[0]

    # But it's still possible that this key exists elsewhere in the
    # LCA and has simply moved.  Look for that as an LCA line for the
    # merge.

    line_LCA = state.cursor_LCA.find_next_match(key_AB)

    if line_LCA:
        # The AB line has a match in the LCA.
        #
        # However, the current line in the LCA may also have a match
        # in A/B.  We have a choice, which will affect how quickly we
        # can get file cursors back in sync:
        #
        # * we can leave the current lines in LCA and pull the match
        #   from the forward search, or
        #
        # * we can pull lines out of the LCA and into the backlog
        #   until we are back in sync
        #
        # In order to get the files back in sync again as quickly as
        # possible (to ensure accurate reordering of lines), it is
        # helpful to choose the action which results in the fewest
        # out-of-order lines.
        #
        # So if our AB match is a long way into LCA but the current
        # LCA line matches something close in AB, just do a lookahead
        # match and consume the distant LCA line, leaving the LCA
        # cursor where it is; and we'll be in sync again as soon as we
        # reach the AB line which matches LCA[0].
        #
        # But if the AB key matches a nearby line in the LCA, then we
        # should not process that match immediately; but rather pull
        # LCA lines into the backlog until the LCA cursor reaches the
        # matching line.

        LCA_distance = line_LCA.linenr - state.cursor_LCA.linenr

        # Only push to backlog if the LCA match is not already in the
        # backlog!

        if LCA_distance > 0:
            A_match = state.cursor_A.find_next_match(key_LCA)

            if A_match:
                A_distance = A_match.linenr - state.cursor_A.linenr

                if LCA_distance < A_distance:
                    logging.debug("  Action: Push %d lines from LCA to backlog"
                                  % LCA_distance)
                    while state.cursor_LCA.current_key() != key_AB:
                        logging.debug("    Push line %s" % state.cursor_LCA[0].text[0:-1])
                        state.cursor_LCA.move_to_backlog()

                    return

    # And now do a three-way field-by-field merge.

    merge_one_line(state, line_LCA, line_A, line_B)
    state.consume(key_AB, line_LCA, line_A, line_B)

def merge_one_changed_A(state, key_LCA, key_A):
    """
    LCA and the B merge branch have the same key, but A has changed;
    merge in that change.
    """
    return UnhandledError

def merge_one_changed_B(state, key_LCA, key_B):
    """
    LCA and the A merge branch have the same key, but B has changed;
    merge in that change.
    """
    return UnhandledError

def merge_one_all_different(state, key_LCA, key_A, key_B):
    return UnhandledError

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
