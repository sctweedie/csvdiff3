#!usr/bin/python3

import unittest
import os
import filecmp

from csvdiff3 import merge3

class Debug:
    # Set this true to disable all tests; a single test can then be
    # manually enabled for debugging purposes.
    #
    # (Reduces clutter in the debug log file.)

    skip_tests = False

def merge3_named(filename_LCA, filename_A, filename_B,
                 filename_output, key,
                 **kwargs):
    with open(filename_LCA, "rt") as file_LCA, \
         open(filename_A, "rt") as file_A, \
         open(filename_B, "rt") as file_B, \
         open(filename_output, "wt") as file_output:
        return merge3.merge3(file_LCA, file_A, file_B,
                             key,
                             output = file_output,
                             **kwargs)

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
                        file_expected, key,
                        **kwargs):
        """
        Run a single merge3 and check the output against the contents
        of a given file.
        """

        merge3_named(file_LCA, file_A, file_B, self.file_output, key, **kwargs)
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

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
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

class TestOneSidedLineChanges(MergeTest):
    """
    Test merging line changes (moves, adds, deletes) on just one side
    of the merge
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_onesided_add(self):
        """
        Test handling lines added to one side
        """

        # Adding lines on the A side
        self.run_and_compare("testdata/longer_del1.csv",
                             self.file_longer,
                             "testdata/longer_del1.csv",
                             self.file_longer,
                             "name")

        self.run_and_compare("testdata/longer_del2.csv",
                             self.file_longer,
                             "testdata/longer_del2.csv",
                             self.file_longer,
                             "name")

        # and on the B side
        self.run_and_compare("testdata/longer_del1.csv",
                             "testdata/longer_del1.csv",
                             self.file_longer,
                             self.file_longer,
                             "name")

        self.run_and_compare("testdata/longer_del2.csv",
                             "testdata/longer_del2.csv",
                             self.file_longer,
                             self.file_longer,
                             "name")

    def test_onesided_add(self):
        """
        Test handling lines removed from one side
        """

        # Deleting lines from the A side
        self.run_and_compare(self.file_longer,
                             "testdata/longer_del1.csv",
                             self.file_longer,
                             "testdata/longer_del1.csv",
                             "name")

        self.run_and_compare(self.file_longer,
                             "testdata/longer_del2.csv",
                             self.file_longer,
                             "testdata/longer_del2.csv",
                             "name")

        # and on the B side
        self.run_and_compare(self.file_longer,
                             self.file_longer,
                             "testdata/longer_del1.csv",
                             "testdata/longer_del1.csv",
                             "name")

        self.run_and_compare(self.file_longer,
                             self.file_longer,
                             "testdata/longer_del2.csv",
                             "testdata/longer_del2.csv",
                             "name")

    def test_onesided_move(self):
        """
        Test handling lines moved in one side
        """

        # Moving lines in the A side
        self.run_and_compare(self.file_longer,
                             "testdata/longer_move1.csv",
                             self.file_longer,
                             "testdata/longer_move1.csv",
                             "name")

        self.run_and_compare(self.file_longer,
                             "testdata/longer_move2.csv",
                             self.file_longer,
                             "testdata/longer_move2.csv",
                             "name")

        # and on the B side
        self.run_and_compare(self.file_longer,
                             self.file_longer,
                             "testdata/longer_move1.csv",
                             "testdata/longer_move1.csv",
                             "name")

        self.run_and_compare(self.file_longer,
                             self.file_longer,
                             "testdata/longer_move2.csv",
                             "testdata/longer_move2.csv",
                             "name")


class TestLineConflict(MergeTest):
    """
    Tests for 3-way merge where the different field changes are
    applied on each side of the merge.
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_conflicting_updates(self):
        """
        Test handling lines changed differently in both A and B
        """

        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_changed.csv",
                             "testdata/simple_changed2.csv",
                             "testdata/simple_changedmerge.csv",
                             "name")

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_conflicting_del_and_update(self):
        """
        Test handling lines deleted in one side of the merge and changed
        in the other
        """

        # Changed on side A, deleted on side B
        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_changed.csv",
                             "testdata/simple_del1.csv",
                             "testdata/simple_delmerge1.csv",
                             "name")

        # Changed on side B, deleted on side A
        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_del1.csv",
                             "testdata/simple_changed.csv",
                             "testdata/simple_delmerge2.csv",
                             "name")


class TestAsymmetricLineChanges(MergeTest):
    """
    Tests for 3-way merge where the A and B sides are adding, deleting
    or moving lines in different ways.
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_overlapping_add1(self):
        """
        Test handling lines added differently in both A and B
        """

        # A adds a single line at the start of the file;
        #
        # B adds that plus an additional line before it.
        #
        # Requires 3-way-merge handling, as LCA, A and B will all be
        # different at the start.

        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_ins1.csv",
                             "testdata/simple_ins2.csv",
                             "testdata/simple_ins2.csv",
                             "name")

        # As before, with A/B reversed.  The merge should not care
        # which order the lines are added, as the longer insert is a
        # strict superset of the shorter one regardless of which comes
        # first.

        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_ins2.csv",
                             "testdata/simple_ins1.csv",
                             "testdata/simple_ins2.csv",
                             "name")

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_overlapping_add2(self):
        """
        Test handling lines added differently in both A and B
        """

        # A adds a single line at the end of the file;
        #
        # B adds that plus an additional line after it.

        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_append1.csv",
                             "testdata/simple_append2.csv",
                             "testdata/simple_append2.csv",
                             "name")

        # A adds a single line at the end of the file;
        #
        # B adds that plus an additional line before it.

        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_append1.csv",
                             "testdata/simple_append3.csv",
                             "testdata/simple_append3.csv",
                             "name")

        # As the previous two, but with A/B reversed; output should be
        # the same.

        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_append2.csv",
                             "testdata/simple_append1.csv",
                             "testdata/simple_append2.csv",
                             "name")

        # A adds a single line at the end of the file;
        #
        # B adds that plus an additional line before it.

        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_append3.csv",
                             "testdata/simple_append1.csv",
                             "testdata/simple_append3.csv",
                             "name")

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_overlapping_del1(self):
        """
        Test handling lines deleted differently in both A and B
        """

        # A removes a single line near the start of the file;
        #
        # B removes two lines.
        #
        # Requires 3-way-merge handling, as LCA, A and B will all be
        # different at the start.

        self.run_and_compare("testdata/simple_ins2.csv",
                             "testdata/simple_ins1.csv",
                             self.file_unquoted,
                             self.file_unquoted,
                             "name")

        # And with A/B reversed

        self.run_and_compare("testdata/simple_ins2.csv",
                             self.file_unquoted,
                             "testdata/simple_ins1.csv",
                             self.file_unquoted,
                             "name")

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_overlapping_move_del(self):
        """
        Test handling lines deleted on one side and moved on the other.
        """

        # A reorders several lines in the file;
        #
        # B removes most of them.

        self.run_and_compare(self.file_longer,
                             "testdata/longer_move1.csv",
                             "testdata/longer_trunc.csv",
                             "testdata/longer_movetrunc1.csv",
                             "name")

        self.run_and_compare(self.file_longer,
                             "testdata/longer_move2.csv",
                             "testdata/longer_trunc.csv",
                             "testdata/longer_trunc.csv",
                             "name")

        # And with A/B reversed

        self.run_and_compare(self.file_longer,
                             "testdata/longer_trunc.csv",
                             "testdata/longer_move1.csv",
                             "testdata/longer_movetrunc1.csv",
                             "name")

        self.run_and_compare(self.file_longer,
                             "testdata/longer_trunc.csv",
                             "testdata/longer_move2.csv",
                             "testdata/longer_trunc.csv",
                             "name")

class TestHeaderChanges(MergeTest):
    """
    Tests for 3-way merge where the A or B side introduces changes
    into the header/columns
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_header_add(self):
        """
        Test line merge functionality in the presence of a new column
        """

        # A moves lines around; B adds a new column

        self.run_and_compare(self.file_longer,
                             "testdata/longer_move1.csv",
                             "testdata/longer_newcol.csv",
                             "testdata/longer_newcol_mv1.csv",
                             "name")

        # And with A/B reversed

        self.run_and_compare(self.file_longer,
                             "testdata/longer_newcol.csv",
                             "testdata/longer_move1.csv",
                             "testdata/longer_newcol_mv1.csv",
                             "name")

        # A adds some new lines; B adds a new column.  New lines
        # should get an empty default value for the new column.

        self.run_and_compare(self.file_longer,
                             "testdata/longer_newcol.csv",
                             "testdata/longer_more.csv",
                             "testdata/longer_newcol_more1.csv",
                             "name")

        self.run_and_compare(self.file_longer,
                             "testdata/longer_more.csv",
                             "testdata/longer_newcol.csv",
                             "testdata/longer_newcol_more1.csv",
                             "name")

class TestAsymmetricLineChangesWithConflict(MergeTest):
    """
    Tests for 3-way merge where the A and B sides are adding, deleting
    or moving lines in different ways.  Also includes conflicts where
    a line is deleted on one side and moved on another, sometimes with
    new data contents on the moved line (which should result in a
    conflict.)
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_move_delete_conflict(self):
        # Multiple moves, deletes and changes of the same lines on
        # different sides.

        self.run_and_compare(self.file_longer,
                             "testdata/longer_movdel1.csv",
                             "testdata/longer_movdel2.csv",
                             "testdata/longer_movdel_merged.csv",
                             "name")

        # With A and B swapped, the output should be the same except
        # that content will swap sides within conflict markers.

        self.run_and_compare(self.file_longer,
                             "testdata/longer_movdel2.csv",
                             "testdata/longer_movdel1.csv",
                             "testdata/longer_movdel_merged2.csv",
                             "name")


class TestShortLines(MergeTest):
    """
    Tests for handling short lines (which are missing some columns at
    the end of the line.)
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_short_lines(self):
        """
        Test handling a line which is missing content but has no
        conflict on the other side.  Treat that as deleting a field.
        """

        # A is unchanged; B includes a short line.  Output will
        # include that line, reformatted.

        self.run_and_compare(self.file_unquoted,
                             self.file_unquoted,
                             "testdata/simple_shortline.csv",
                             "testdata/simple_shortline_repaired.csv",
                             "name")

        # A changes the field; B includes a short line.  Output needs
        # to reflect a conflict.

        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_changed.csv",
                             "testdata/simple_shortline.csv",
                             "testdata/simple_shortline_conflict.csv",
                             "name")

        # And with A/B reversed.

        self.run_and_compare(self.file_unquoted,
                             "testdata/simple_shortline.csv",
                             "testdata/simple_changed.csv",
                             "testdata/simple_shortline_conflict2.csv",
                             "name")

        # A is unchanged; B includes a completely blank short line
        # which is missing the key entirely.

        self.run_and_compare(self.file_unquoted,
                             self.file_unquoted,
                             "testdata/simple_emptyline.csv",
                             "testdata/simple_emptyline.csv",
                             "name")

class TestDupKeys(MergeTest):
    """
    Tests for handling duplicated keys.  We should obey a simple rule:
    the occurrences of the same key in each file are matched together
    in the order they appear for merge.
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_dup_keys(self):
        """
        Test handling a line which is missing content but has no
        conflict on the other side.  Treat that as deleting a field.
        """

        # A and B both duplicate a (different) line from LCA

        self.run_and_compare(self.file_longer,
                             "testdata/longer_dup1.csv",
                             "testdata/longer_dup2.csv",
                             "testdata/longer_dupmerge.csv",
                             "name")

        # A and B both duplicate a (different) line from LCA.  The
        # output is almost the same, but demonstrates that when we
        # have out-of-order keys appearing differently at the same
        # position in A and B, we pick A first.

        self.run_and_compare(self.file_longer,
                             "testdata/longer_dup2.csv",
                             "testdata/longer_dup1.csv",
                             "testdata/longer_dupmerge2.csv",
                             "name")

class TestReformatAll(MergeTest):
    """
    Tests for forced reformatting of all lines
    """

    @unittest.skipIf(Debug.skip_tests, "skipping for debug")
    def test_reformat_all(self):
        """
        Test handling a line which is unchanged on both sides, but
        where we have asked for a full reformat
        """

        self.run_and_compare(self.file_unquoted,
                             self.file_unquoted,
                             self.file_unquoted,
                             self.file_fully_quoted,
                             "name",
                             quote = "all",
                             reformat_all = True)


if __name__ == "__main__":
    unittest.main()

