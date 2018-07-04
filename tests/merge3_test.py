#!usr/bin/python3

import unittest
import os
import filecmp

import csvmerge3

class Debug:
    # Set this true to disable all tests; a single test can then be
    # manually enabled for debugging purposes.
    #
    # (Reduces clutter in the debug log file.)

    skip_tests = False

def merge3_named(filename_LCA, filename_A, filename_B,
                 filename_output, key):
    with open(filename_LCA, "rt") as file_LCA, \
         open(filename_A, "rt") as file_A, \
         open(filename_B, "rt") as file_B, \
         open(filename_output, "wt") as file_output:
        return csvmerge3.merge3(file_LCA, file_A, file_B,
                                key,
                                output = file_output)

def keep_copy(testfile, savefile):
    """
    Keep around a copy of a test output file, for debugging.
    Removes any previous copy (we only keep one debug file.)
    """
    if os.path.exists(savefile):
        os.unlink(savefile)
    os.link(testfile, savefile)

class MergeTest(unittest.TestCase):
    """
    Tools for running merge tests and comparing outputs with expected
    values.

    Individual test classes will inherit from this base class.
    """

    file_unquoted = "testdata/simple.csv"
    file_partially_quoted = "testdata/simple_requoted.csv"
    file_fully_quoted = "testdata/simple_quoted.csv"

    file_output = "testdata/TEST_OUTPUT.csv"
    failure_output = "testdata/SAVED_OUTPUT.csv"

    file_longer = "testdata/longer.csv"

    def run_and_compare(self,
                        file_LCA, file_A, file_B,
                        file_expected, key):
        """
        Run a single merge3 and check the output against the contents
        of a given file.
        """

        merge3_named(file_LCA, file_A, file_B, self.file_output, key)
        files_equal = filecmp.cmp(self.file_output, file_expected, shallow=False)
        if not files_equal:
            keep_copy(self.file_output, self.failure_output)
        self.assertTrue(files_equal)

    def tearDown(self):
        if os.path.exists(self.file_output):
            os.unlink(self.file_output)

class TestFormatting(MergeTest):
    """
    Simple tests for 3-way merge to check the way we format output
    when the content of each file is logically the same, only format
    such as quoting changes between files.
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_no_change(self):
        """
        Test that a merge between 3 equal files results in the extact
        same file as output, regardless of quoting patterns used
        """
        for file in [self.file_unquoted,
                     self.file_partially_quoted,
                     self.file_fully_quoted]:
            self.run_and_compare(file, file, file, file, "name")

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_reformat_both_sides(self):
        """
        Test a merge that changes the format (eg. quoting) on both
        sides of the merge, but leaves content intact
        """
        self.run_and_compare(self.file_unquoted,
                             self.file_partially_quoted,
                             self.file_partially_quoted,
                             self.file_partially_quoted,
                             "name")

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_reformat_one_sided(self):
        """
        Test a merge that changes the format (eg. quoting) on one
        side of the merge, but leaves content intact.
        """

        # On arbitrary changes in formatting, we preserve the
        # formatting in the A merge branch

        self.run_and_compare(self.file_unquoted,
                             self.file_partially_quoted,
                             self.file_unquoted,
                             self.file_partially_quoted,
                             "name")

        self.run_and_compare(self.file_partially_quoted,
                             self.file_unquoted,
                             self.file_fully_quoted,
                             self.file_unquoted,
                             "name")

        self.run_and_compare(self.file_partially_quoted,
                             self.file_partially_quoted,
                             self.file_unquoted,
                             self.file_partially_quoted,
                             "name")

class TestABLineMerge(MergeTest):
    """
    Tests for 3-way merge where the same changes are applied to both
    the A and B branches.  Tests added, deleted or moved lines, but
    does not change the line contents.
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_deleted_lines(self):
        """
        Test handling lines deleted from both A and B
        """

        # longer_del1 has lines missing from the middle of the file
        self.run_and_compare(self.file_longer,
                             "testdata/longer_del1.csv",
                             "testdata/longer_del1.csv",
                             "testdata/longer_del1.csv",
                             "name")

        # longer_del2 has lines missing from the start and end of the
        # file
        self.run_and_compare(self.file_longer,
                             "testdata/longer_del2.csv",
                             "testdata/longer_del2.csv",
                             "testdata/longer_del2.csv",
                             "name")

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_added_lines(self):
        """
        Test handling lines added to both A and B
        """
        self.run_and_compare("testdata/longer_del1.csv",
                             self.file_longer,
                             self.file_longer,
                             self.file_longer,
                             "name")

        self.run_and_compare("testdata/longer_del2.csv",
                             self.file_longer,
                             self.file_longer,
                             self.file_longer,
                             "name")

    #@unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_moved_lines(self):
        """
        Test handling lines moved in both A and B
        """
        self.run_and_compare(self.file_longer,
                             "testdata/longer_move1.csv",
                             "testdata/longer_move1.csv",
                             "testdata/longer_move1.csv",
                             "name")

        self.run_and_compare(self.file_longer,
                             "testdata/longer_move2.csv",
                             "testdata/longer_move2.csv",
                             "testdata/longer_move2.csv",
                             "name")

if __name__ == "__main__":
    unittest.main()
