#!/usr/bin/python3
#
# csvfile.py:
#
#  Line class for reading individual CSV lines
#
#  File class for reading/writing entire CSV files, maintaining both
#  the original verbatim line-by-line content plus the field-by-field
#  content breakdown.

import csv
from collections import OrderedDict

class Line:
    """
    Holds an individual line from a CSV file.
    """

    def __init__(self, text, row, linenr):
        self.text = text
        self.row = row
        self.linenr = linenr
        self.is_consumed = False

    def __format__(self, format):
        return str(self.row)

class FileReader:
    """
    File reader which also retains the original, unmodified contents of
    each line read.

    Allows the file to be passed to an existing iterator while still
    allowing the individual lines to be recalled separately.
    """

    def __init__(self, stream):
        self.stream = stream

    def __iter__(self):
        while True:
            self.lastline = self.stream.readline()
            if self.lastline == "":
                break
            yield self.lastline

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
            yield (self.file.lastline, row)

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

    def __init__(self, stream, key, **args):
        self.reader = CSVHeaderFile(stream, **args)

        self.header = self.reader.header
        self.lines = [self.header]
        self.lines_by_key = {}

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
            key = line.row[self.key_index]
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
            return CSVFile(file, key, **args)

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
        self.backlog = OrderedDict()

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

        return self.getline(0).row[self.file.key_index]

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
        assert not key in self.backlog
        self.backlog[self.current_key()] = getline(0)
        self.advance()

    def find_next_match(self, key):
        """
        Find the next Line matching a given key, to help find
        out-of-order matches between lines in the files.

        Searches the backlog first, then searches forward for the next
        match.
        """

        assert key

        if key in self.backlog:
            return self.backlog[key]

        if not key in self.file.lines_by_key:
            return None
        lines = self.file.lines_by_key[key]
        if not lines:
            return None
        return lines[0]

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

        # If this key is in the backlog, we should have found it
        # there, so check that the backlog contains the right line and
        # remove it.

        if key in self.backlog:
            assert self.backlog[key] == line
            del self.backlog[key]
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
