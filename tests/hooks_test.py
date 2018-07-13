#!usr/bin/python3

import unittest
import click
import sys
import traceback
from click.testing import CliRunner

import csvdiff3.tools
from csvdiff3.tools.tools import *

def test_path(path):
    return "testdata/" + path

class TestOnCli(unittest.TestCase):
    """
    Runs a single test via the click CLI runner method.
    """
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


if __name__ == "__main__":
    unittest.main()

