#!usr/bin/python3

import unittest
import click
import sys
import traceback
import filecmp
import shutil
from click.testing import CliRunner

import csvdiff3.tools
from csvdiff3.tools.tools import *

def test_path(path):
    return "testdata/" + path

class TestOnCli(unittest.TestCase):
    """
    Runs a single test via the click CLI runner method.
    """
    output_tmpfile = test_path("tmp.test.output")

    def run_one(self, args, **kwargs):
        runner = CliRunner()
        result = runner.invoke(csvdiff3.tools.csvhooks,
                               args,
                               obj = csvdiff3.tools.Options(),
                               **kwargs)

        # If the error is < 0 then we have an exception;

        if result.exit_code >= 0:
            return result

        # so extract the traceback and print that for debugging.

        print(result.exc_info)
        _,_,tb = result.exc_info
        traceback.print_tb(tb)

        return result

    def run_one_from_file(self, args, filename, **kwargs):
        """
        Run a single CLI invokation with stdin content redirected from a
        given file.
        """

        with open(filename, "rt") as file:
            return self.run_one(args, input = file.read(), **kwargs)

class TestValidateCli(TestOnCli):
    """
    Test the validate CLI

    The validate function itself is so small as to be best tested in
    context.
    """

    def test_empty_file(self):
        """
        Test handling of an empty CSV file.  It should validate OK as long
        as no key is expected.
        """
        path = test_path("empty.csv")

        result = self.run_one(["validate", path])
        self.assertEqual (result.exit_code, 0)

        result = self.run_one(["--key=madeup", "validate", path])
        self.assertEqual (result.exit_code, 1)

    def test_simple_file(self):
        """
        Test handling of a simple CSV file.
        """
        path = test_path("simple.csv")

        # The file should validate given no other args;

        result = self.run_one(["validate", path])
        self.assertEqual (result.exit_code, 0)

        # and if given a key name that exists.

        result = self.run_one(["--key=name", "validate", path])
        self.assertEqual (result.exit_code, 0)

        # It should still fail given a non-existent column as key

        result = self.run_one(["--key=madeup", "validate", path])
        self.assertEqual (result.exit_code, 1)

        # Things should still work if the file is passed in on stdin

        result = self.run_one_from_file(["--key=name", "validate"],
                                        filename = path)
        self.assertEqual (result.exit_code, 0)

        # It should still fail given a non-existent column as key

        result = self.run_one(["--key=madeup", "validate"], input=path)
        self.assertEqual (result.exit_code, 1)

class TestReformatIO(TestOnCli):
    """
    Test the reformat CLI: test the input/output functionality
    (ability to work as a filter, safely overwrite existing files,
    write to new files etc.)
    """

    def tearDown(self):
        try:
            os.unlink(self.output_tmpfile)
        except FileNotFoundError:
            pass

    def test_reformat_io_new(self):
        """
        Test reformatting onto a new output file..
        """
        inpath = test_path("simple.csv")
        outpath = self.output_tmpfile
        quotepath = test_path("simple_quoted.csv")

        # Make sure we start the test with the output file not present.

        try:
            os.unlink(outpath)
        except FileNotFoundError:
            pass

        result = self.run_one(["--quote=minimal",
                               "--lineterminator=unix",
                               "reformat",
                               inpath,
                               outpath])
        self.assertEqual (result.exit_code, 0)
        self.assertTrue (filecmp.cmp(inpath, outpath, shallow=False))

        # Try once more, overwriting the existing output file; it
        # should have new contents

        result = self.run_one(["--quote=all",
                               "--lineterminator=unix",
                               "reformat",
                               inpath,
                               outpath])
        self.assertEqual (result.exit_code, 0)
        self.assertTrue (filecmp.cmp(outpath, quotepath, shallow=False))

    def test_reformat_io_overwrite(self):
        """
        Test reformatting on top of the input file.
        """
        inpath = test_path("simple.csv")
        outpath = self.output_tmpfile
        quotepath = test_path("simple_quoted.csv")

        # Copy the unquoted input test file first

        shutil.copyfile(inpath, outpath)

        # and now try to reformat that file's contents in place

        result = self.run_one(["--quote=all",
                               "--lineterminator=unix",
                               "reformat",
                               outpath])
        self.assertEqual (result.exit_code, 0)
        self.assertTrue (filecmp.cmp(outpath, quotepath, shallow=False))

    def test_reformat_io_filter(self):
        """
        Test reformatting as a simple pipe/filter.
        """
        inpath = test_path("simple.csv")
        outpath = self.output_tmpfile
        quotepath = test_path("simple_quoted.csv")

        result = self.run_one_from_file(["--quote=all",
                                         "--lineterminator=unix",
                                         "reformat"],
                                        inpath)
        self.assertEqual (result.exit_code, 0)
        with open(quotepath, "rt") as file:
            test_output = file.read()
            self.assertEqual (test_output, result.output)

if __name__ == "__main__":
    unittest.main()

