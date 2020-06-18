#!/usr/bin/python3
#
# file.py:
#
#  Line class for reading individual CSV lines
#
#  File class for reading/writing entire CSV files, maintaining both
#  the original verbatim line-by-line content plus the field-by-field
#  content breakdown.

import csv
import logging
import shutil
from orderedmultidict import omdict
from collections import UserString

# Define a custom string type that always evaluates to True when cast
# to boolean.
#
# The core merge logic depends on matching lines with similar keys, so
# key values are a core part of the algorithm.  And it is critical to
# know when a key is present in other files; we use a value of None in
# various places as a key to indicate that a particular key is absent
# in one of the merging files.
#
# But the empty string "" is also a legal key --- it arises whenever
# we have a blank line in an input file, for example.  We treat such
# lines as entirely legal, normal lines: a blank line ("" key) in one
# file should simply match the next blank line in other files.
#
# We will be testing whether lines exist by checking if the relevant
# Key evaluates to True or False, eg. with
#
#   if Key_LCA:
#       ...key is present in the current LCA line...
#
# and we want this to evaluate to True even for the empty string.
#
# So provide an override class for such Keys which always evaluate to
# True in such situations; False will only be returned if the Key is
# None, ie. it is genuinely missing and we have no matching line in
# the given file.

class Key(UserString):
    def __bool__(self):
        return True

class Line:
    """
    Holds an individual line from a CSV file.
    """

    def __init__(self, text, row, linenr):
        self.text = text
        self.row = row
        self.linenr = linenr
        self.is_consumed = False
        self.LCA_backlog_match = None

    def __format__(self, format):
        return str(self.row)

    def __sub__(self, line):
        assert isinstance(line, Line)
        return self.linenr - line.linenr

    def get_field(self, index):
        try:
            return Key(self.row[index])
        except IndexError:
            return Key('')

    # For debugging we sometimes want to output a linenr even if we're
    # not sure we actually have a line

    @staticmethod
    def linenr(line):
        if not line:
            return "n/a"
        return line.linenr

class FileReader:
    """
    File reader which also retains the original, unmodified contents of
    each line read.

    Allows the file to be passed to an existing iterator while still
    allowing the individual lines to be recalled separately.
    """

    def __init__(self, stream):
        self.stream = stream
        self.lastlines = ""

    def __iter__(self):
        while True:
            line = self.stream.readline()
            if line == "":
                break
            self.lastlines += line
            yield line

    def get_last_lines(self):
        lines = self.lastlines
        self.lastlines = ""
        return lines

class CSVLineReader:
    """
    CSV reader which also returns the original, unmodified contents of
    each line read.

    Iterator returns a tuple (string, list)
    """

    def __init__(self, stream, **args):
        self.file = FileReader(stream)
        self.reader = csv.reader(self.file, **args)

    def __iter__(self):
        for row in self.reader:
            yield (self.file.get_last_lines(), row)

class CSVHeaderFile:
    """
    CSV reader which remembers the initial header line, and returns
    Line objects for each subsequent line as an iterator.
    """

    def __init__(self, stream, **args):
        self.reader = CSVLineReader(stream, **args)
        self.iterator = iter(self.reader)
        text,row = next(self.iterator)
        self.header = Line(text,row,1)

    def __iter__(self):
        linenr = 2
        for line,row in self.iterator:
            yield Line(line, row, linenr)
            linenr += 1

class CSVFile:
    """
    Main CSV file class.

    Reads files, remembers lines and headers/fields, and
    returns/searches for lines on demand.
    """

    def __init__(self, stream, key, filename = "input", **args):
        self.reader = CSVHeaderFile(stream, **args)

        self.header = self.reader.header
        self.lines = [self.header]
        self.lines_by_key = {}
        self.filename = filename

        if not key in self.header.row:
            raise KeyError

        self.key_index = self.header.row.index(next(x for x in self.header.row
                                                    if x == key))

        # For now we read the entire file up-front.
        #
        # In the future, this may be replaced with a read that
        # searches ahead in the file only far enough to resolve
        # reordered lines with a given window.

        for count, line in enumerate(self.reader, 2):
            self.lines.append(line)
            # If there is a short line which does not include a field
            # for the primary key column, it just gets assigned a
            # blank key
            key = line.get_field(self.key_index)
            if key in self.lines_by_key:
                self.lines_by_key[key].append(line)
            else:
                self.lines_by_key[key] = [line]

        self.last_line = len(self.lines)

    def __iter__(self):
        for line in self.lines[1:]:
            yield line

    def __getitem__(self, line):
        # List indices start at 0, but by normal convention we assume
        # line numbers start at 1 (starting at the header line.)

        # Do not honour the usual List behaviour of returning the last
        # item on list[-1]!

        if line <= 0:
            raise IndexError
        return self.lines[line-1]

    def dialect(self):
        return self.reader.reader.reader.dialect

    @staticmethod
    def open(filename, key, **args):
        with open(filename, "rt") as file:
            return CSVFile(file, key, filename, **args)

    def dump(self, id, prefix):
        """Dump a CSVFile's contents to a safe dump location for later debugging."""

        filename = prefix + "-" + id + ".dump"
        stream = self.reader.reader.file.stream
        logging.debug(f"Dumping file {id} to {filename}")
        stream.seek(0)
        with open(filename, "wt") as outfile:
            shutil.copyfileobj(stream, outfile)

class Cursor:
    """
    Remember a position with a CSV file.

    Allows for naturally moving forwards through the file as we
    process it, while still giving easy access to close-by lines near
    to the current position.
    """

    def __init__(self, file):
        self.file = file

        # Start processing the file at line 2, ie. the first
        # non-header line.
        self.linenr = 2

        # Maintain a backlog of Lines that we know we will need later
        # on from some file
        self.backlog = omdict()

    def __getitem__(self, offset):
        """
        Accessing a Cursor by [] indexing returns a line relative to
        the current cursor position.  ie.
        cursor[0]  returns the current Line
        cursor[1]  returns the next Line to be processed
        cursor[-1] returns the previous Line in the file
        etc.

        returns None if the index is out of bounds.
        """
        try:
            return self.getline(offset)
        except IndexError:
            return None

    def getline(self, offset):
        """
        Accessing a Cursor by indexing; similar to [] lookup, except
        that it raises an IndexError if the index is out of bounds
        """
        return self.file[self.linenr+offset]

    def advance(self):
        """
        Advance the current line position to the next line in a file
        """

        # First remove the line from the per-key line lookup
        line = self[0]
        if not line.is_consumed:
            key = self.current_key()
            found_line = self.file.lines_by_key[key].pop(0)
            assert line == found_line

        if self.linenr <= self.file.last_line:
            self.linenr += 1

        # Skip over any lines which have already been processed

        while (not self.EOF()) and self[0].is_consumed:
            self.linenr += 1

    def current_key(self):
        """
        Return the key of the current line

        Returns None if we are at EOF.
        """

        if self.EOF():
            return None

        # If there is a short line which does not include a field for
        # the primary key column, it just gets assigned a blank key
        return self.getline(0).get_field(self.file.key_index)

    def EOF(self):
        return self.linenr > self.file.last_line

    def assert_finished(self):
        assert self.EOF()
        assert not self.backlog

    def move_to_backlog(self):
        """
        Move the current Line to the backlog for later lookup.

        Automatically advances to the next line position.
        """
        key = self.current_key()
        # We need to be able to handle multiple instances of the same
        # key in the backlog, for the special case where multiple
        # lines with the same key exist, and all of those lines are
        # being reordered in the output
        self.backlog.add(key, self.getline(0))
        self.advance()

    def find_next_match(self, key):
        """
        Find the next Line matching a given key, to help find
        out-of-order matches between lines in the files.

        Searches the backlog first, then searches forward for the next
        match.
        """

        assert key != None

        if not key in self.file.lines_by_key:
            return None
        lines = self.file.lines_by_key[key]
        if not lines:
            return None

        # We will look forward in the matching lines, skipping any
        # that have already been matched against the backlog; those
        # lines should not be candidates for matching against any
        # further lines with the same key.

        for line in lines:
            if not line.LCA_backlog_match:
                return line

        # If all future lines with this key already have matches in
        # the backlog, then there's nothing left to match.

        return None

    def consume(self, key, line):
        """
        Complete processing for a given key and line, removing them from
        further processing (either by removing them from the backlog,
        or setting the line to be skipped by future cursor advances.

        There will always be a key but the line may be null (if we are
        processing a line that was deleted from this file.
        """

        if not line:
            return

        # If the line happens to live further on in the file, make
        # sure we never process it again.

        line.is_consumed = True

        # If this key is in the backlog, it is possible that it
        # matches a different line.  If there are multiple lines with
        # the same key, some may be reordered while others are
        # deleted; for such a deleted line, we may find the other
        # reordered line in the backlog, but the current line might
        # not be there.
        #
        # So only remove the line from the backlog if it matches both
        # by key and is the exact correct line.

        if key in self.backlog:
            if self.backlog[key] == line:
                self.backlog.popvalue(key, last=False)
                return

        # It's not in the backlog so it must be either the current
        # line or some future one.  Find the first match:

        found_line = self.file.lines_by_key[key].pop(0)

        # Check that the line we have is in fact the earliest match:

        assert found_line == line

        # and if it is the current line, we can advance to the next
        # line.

        if line.linenr == self.linenr:
            self.advance()
