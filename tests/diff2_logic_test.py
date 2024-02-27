#!usr/bin/python3

import unittest
import os
import filecmp

from csvdiff3 import merge3
from csvdiff3.output import Diff2OutputDriver

class Debug:
    # Set this true to disable all tests; a single test can then be
    # manually enabled for debugging purposes.
    #
    # (Reduces clutter in the debug log file.)

    skip_tests = False

def diff2_named(filename_1, filename_2,
                filename_output, key,
                 **kwargs):
    with open(filename_1, "rt") as file_LCA, \
         open(filename_2, "rt") as file_A, \
         open(filename_2, "rt") as file_B, \
         open(filename_output, "wt") as file_output:
        return merge3.merge3(file_LCA, file_A, file_B,
                             key,
                             output = file_output,
                             output_driver_class = Diff2OutputDriver,
                             output_args = kwargs)

def keep_copy(testfile, savefile):
    """
    Keep around a copy of a test output file, for debugging.
    Removes any previous copy (we only keep one debug file.)
    """
    if os.path.exists(savefile):
        os.unlink(savefile)
    os.link(testfile, savefile)

class DiffTest(unittest.TestCase):
    """
    Tools for running diff2 tests and comparing outputs with expected
    values.

    Individual test classes will inherit from this base class.
    """

    file_empty = "testdata/empty.csv"
    # We'll test with three equivalent starting files differing only
    # in their use of quotes
    file_unquoted = "testdata/simple.csv"
    file_partially_quoted = "testdata/simple_requoted.csv"
    file_fully_quoted = "testdata/simple_quoted.csv"
    # and one variant that uses dos, not unix, line termination
    file_dos = "testdata/simple_dos.csv"

    file_output = "testdata/TEST_OUTPUT.csv"
    failure_output = "testdata/SAVED_OUTPUT.csv"

    file_longer = "testdata/longer.csv"

    def run_and_compare(self,
                        file_A, file_B,
                        file_expected, key,
                        show_reordered_lines = False,
                        preamble_extra_text = None):
        """
        Run a single diff2 and check the output against the contents
        of a given file.
        """

        diff2_named(file_A, file_B, self.file_output, key,
                    show_reordered_lines = show_reordered_lines,
                    preamble_extra_text = preamble_extra_text)
        files_equal = filecmp.cmp(self.file_output, file_expected, shallow=False)
        if not files_equal:
            keep_copy(self.file_output, self.failure_output)
        self.assertTrue(files_equal, f'Error comparing with expected output file "{file_expected}"')

    def tearDown(self):
        if os.path.exists(self.file_output):
            os.unlink(self.file_output)

class TestFormatting(DiffTest):
    """
    Simple tests for 2-way diff to check the way we format output
    when the content of each file is logically the same, only format
    such as quoting changes between files.
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_no_change(self):
        """
        Test that a diff between 2 logically-equal files results in an
        empty diff, regardless of quoting patterns or line termination
        used
        """

        for key in "name", "nosuchkey|name", "[auto]", "nosuchkey|[auto]":

            # Test different ways of quoting the same content...

            for file1 in [self.file_unquoted,
                          self.file_partially_quoted,
                          self.file_fully_quoted,
                          self.file_dos]:
                for file2 in [self.file_unquoted,
                              self.file_partially_quoted,
                              self.file_fully_quoted,
                              self.file_dos]:
                    self.run_and_compare(file1, file2, self.file_empty, key)

            # And also test that we can compare two completely empty
            # files.  Any primary key should work in this case, and
            # auto key guessing should also handle it successfully.

            self.run_and_compare(self.file_empty, self.file_empty, self.file_empty, key)

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_add_lines(self):
        """
        Test simple addition of new lines to a file
        """

        # For this simple set of tests, we'll also test primary key
        # auto-detection including both selection of key by picking
        # the key from a list, or by automatic guessing, or both.

        for key in "name", "nosuchkey|name", "[auto]", "nosuchkey|[auto]":

            # Add one or two lines to the file:

            self.run_and_compare(self.file_unquoted,
                                 "testdata/simple_append1.csv",
                                 "testdata/diffs/expected_simple_append1.csv",
                                 key)

            self.run_and_compare(self.file_unquoted,
                                 "testdata/simple_append2.csv",
                                 "testdata/diffs/expected_simple_append2.csv",
                                 key)

            # Adds new lines with conflicts: adds lines that share a key
            # with existing lines, and also modify an exiting line with
            # the same key

            # (This specific test won't run correctly with auto key
            # detection, as we deliberately include multiple lines
            # with the same primary key but different data, so the
            # data field looks like a better primary key.)

            if "[auto]" not in key:
                self.run_and_compare("testdata/longer.csv",
                                     "testdata/longer_dupmerge.csv",
                                     "testdata/diffs/expected_longer_dupmerge.csv",
                                     key)

            # Inserts a completely blank new line

            self.run_and_compare(self.file_fully_quoted,
                                 "testdata/simple_emptyline.csv",
                                 "testdata/diffs/expected_simple_emptyline.csv",
                                 key)

            # And multiple blank lines

            self.run_and_compare("testdata/multi_blank_0.csv",
                                 "testdata/multi_blank_2.csv",
                                 "testdata/diffs/expected_multi_blank.csv",
                                 key)


    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_edit_lines(self):
        """
        Test simple changes to fields within a line
        """

        # Change one field with no quoting changes

        self.run_and_compare(self.file_fully_quoted,
                             "testdata/simple_changed.csv",
                             "testdata/diffs/expected_simple_changed.csv",
                             "name")

        # Change one field but with different quoting on the new value

        self.run_and_compare(self.file_fully_quoted,
                             "testdata/simple_changed2.csv",
                             "testdata/diffs/expected_simple_changed2.csv",
                             "name")

        # Change one field with the new value containing a newline
        # (ie. split over multiple lines)
        #
        # Diff output should contain embedded "\n" strings instead.

        self.run_and_compare(self.file_fully_quoted,
                             "testdata/split.csv",
                             "testdata/diffs/expected_split.csv",
                             "name")

        # And the same test in reverse...

        self.run_and_compare("testdata/split.csv",
                             self.file_fully_quoted,
                             "testdata/diffs/expected_split_rev.csv",
                             "name")

        # Change adds additional fields at the end of some lines

        self.run_and_compare(self.file_fully_quoted,
                             "testdata/simple_trailing_blank_fields.csv",
                             "testdata/diffs/expected_trailing_blank_fields.csv",
                             "name")

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_reorder_lines(self):
        """
        Test edits which reorder lines within a file
        """

        # Changes the order of two lines with the same key.
        # By default, there should be no output, the files compare identically.

        self.run_and_compare("testdata/multiline.csv",
                             "testdata/multiline_reorder.csv",
                             self.file_empty,
                             "name")

        # Run again, but this time asking to show reordered (but
        # otherwise-unchanged) lines.  Both moved lines should show
        # up.

        self.run_and_compare("testdata/multiline.csv",
                             "testdata/multiline_reorder.csv",
                             "testdata/diffs/expected_multiline_reorder.csv",
                             "name",
                             show_reordered_lines = True)

        # Moving lines forward in the file is internally treated very
        # differently from moving lines backwards: test both cases
        # with simple reordering of lines.

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_simple_move_back.csv",
                             self.file_empty,
                             "name")

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_simple_move_forward.csv",
                             self.file_empty,
                             "name")

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_simple_move_back.csv",
                             "testdata/diffs/expected_longer_move_back.csv",
                             "name",
                             show_reordered_lines = True)

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_simple_move_forward.csv",
                             "testdata/diffs/expected_longer_move_forward.csv",
                             "name",
                             show_reordered_lines = True)

        # Combination reorder and delete lines

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_movdel1.csv",
                             "testdata/diffs/expected_longer_movdel1.csv",
                             "name",
                             show_reordered_lines = False)

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_movdel2.csv",
                             "testdata/diffs/expected_longer_movdel2.csv",
                             "name",
                             show_reordered_lines = False)

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_movdel1.csv",
                             "testdata/diffs/expected_longer_movdel1_reorder.csv",
                             "name",
                             show_reordered_lines = True)

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_movdel2.csv",
                             "testdata/diffs/expected_longer_movdel2_reorder.csv",
                             "name",
                             show_reordered_lines = True)

        # More complex: combination of moving lines and adding a new
        # column.  As every line has changed, every line is reported
        # in the diff, so the value of the show_reordered_lines flag
        # should have no effect.

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_newcol_mv1.csv",
                             "testdata/diffs/expected_longer_newcol_mv1.csv",
                             "name",
                             show_reordered_lines = False)

        self.run_and_compare("testdata/longer.csv",
                             "testdata/longer_newcol_mv1.csv",
                             "testdata/diffs/expected_longer_newcol_mv1.csv",
                             "name",
                             show_reordered_lines = True)


if __name__ == "__main__":
    unittest.main()

