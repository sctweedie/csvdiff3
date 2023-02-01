#!usr/bin/python3

import unittest
import os
import filecmp

from csvdiff3 import merge3
from output import Diff2OutputDriver

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
    file_unquoted = "testdata/simple.csv"
    file_partially_quoted = "testdata/simple_requoted.csv"
    file_fully_quoted = "testdata/simple_quoted.csv"

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
        self.assertTrue(files_equal)

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
        empty diff, regardless of quoting patterns used
        """
        for file1 in [self.file_unquoted,
                     self.file_partially_quoted,
                     self.file_fully_quoted]:
            for file2 in [self.file_unquoted,
                          self.file_partially_quoted,
                          self.file_fully_quoted]:
                self.run_and_compare(file1, file2, self.file_empty, "name")




if __name__ == "__main__":
    unittest.main()

