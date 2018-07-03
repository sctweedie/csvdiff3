#!/usr/bin/python3

import sys
import click
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

def merge3_next(state):
    """
    Make one line of progress on the 3-way merge.

    Examines the next line of the LCA and both merge branches, and
    determines what progress can be made.

    This function will advance the cursor through the various input
    files as data gets handled successfully.
    """

    # First, check if the next line of each input file has the
    # exact same text
    text_LCA = state.cursor_LCA[0].text
    text_A = state.cursor_A[0].text
    text_B = state.cursor_B[0].text

    # If they are all the same, then that is our next output line
    if text_LCA == text_A == text_B:
        state.stream.write(text_A)
        state.advance_all()
        return
        
    # One more all-the-same test... if the LCA and A lines are the
    # same, and B is the same *logical* contents but has reformatted
    # the fields, then just use the line from A.

    if text_LCA == text_A and \
       state.cursor_LCA[0].row == state.cursor_B[0].row:
        state.stream.write(text_A)
        state.advance_all()
        return

    # And final all-the-same test... if the same reformatting is
    # present on both A and B sides, and the logical content is
    # unchanged, then apply the reformatting.

    if text_A == text_B and \
       state.cursor_LCA[0].row == state.cursor_A[0].row:
        state.stream.write(text_A)
        state.advance_all()
        return

    # Otherwise, we do an intelligent 3-way merge on the current state

    # First, we need to know the keys of each line
    
    key_LCA = state.cursor_LCA.current_key()
    key_A = state.cursor_A.current_key()
    key_B = state.cursor_B.current_key()

    # Are the keys the same in each file?  If so, we can simply
    # consume the next line from each file, and do a field-by-field
    # merge

    if key_LCA == key_A == key_B:
        merge_one_line(state,
                       state.cursor_LCA[0],
                       state.cursor_A[0],
                       state.cursor_B[0])
        state.advance_all()
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

def merge_one_changed_AB(state, key_LCA, key_A):
    return UnhandledError

def merge_one_changed_A(state, key_LCA, key_A):
    return UnhandledError

def merge_one_changed_B(state, key_LCA, key_B):
    return UnhandledError

def merge_one_all_different(state, key_LCA, key_A, key_B):
    return UnhandledError

def lookup_field(line, column):
    """
    Lookup the value of a given (numbered) column in a given Line
    """
    if column == None:
        return None
    return line.row[column]
    
def merge_one_line(state, line_LCA, line_A, line_B):
    """
    Perform field-by-field merging of LCA, A and B versions of a given
    line, sharing a common primary key.

    Any or all of the files may be missing a value for one or more
    fields, in which case an empty string is used for those fields.
    """

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

    state.writer.writerow(row)

def merge3(file_lca, file_a, file_b, key, output = sys.stdout):
    """
    Perform a full 3-way merge on 3 given CSV files, using the given
    column name as a primary key.
    """

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

def merge3_cli(filename_lca, filename_a, filename_b, key):
    merge3(filename_lca, filename_a, filename_b, key)

if __name__ == "__main__":
    merge3_cli()
