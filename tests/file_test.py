#!usr/bin/python3

import unittest
from csvdiff3.file import *

class TestFile(unittest.TestCase):
    simple_filename = "testdata/simple.csv"

    def test_file_open(self):
        """
        Test handling of a simple file with 2 columns and 2 data rows
        """
        with open(self.simple_filename, "rt") as file:
            file = CSVFile(file, "name")

            self.assertEqual(file.header.text, "name,count\n")
            self.assertEqual(file.header.row, ["name","count"])

            i = iter(file)
            line = next(i)
            self.assertEqual(line.text, "apple,2\n")
            self.assertEqual(line.row, ["apple","2"])

            line = next(i)
            self.assertEqual(line.text, "banana,3\n")
            self.assertEqual(line.row, ["banana","3"])

            with self.assertRaises(StopIteration):
                line = next(i)

            self.assertEqual(file[1].text, "name,count\n")
            self.assertEqual(file[1].row, ["name","count"])

            self.assertEqual(file[2].text, "apple,2\n")
            self.assertEqual(file[2].row, ["apple","2"])

            self.assertEqual(file[3].text, "banana,3\n")
            self.assertEqual(file[3].row, ["banana","3"])

            with self.assertRaises(IndexError):
                line = file[0]

            with self.assertRaises(IndexError):
                line = file[4]

            self.assertEqual(file.lines_by_key["apple"], [file[2]])
            self.assertEqual(file.lines_by_key["banana"], [file[3]])

            with self.assertRaises(KeyError):
                lines = file.lines_by_key["plum"]

    def test_file_dups(self):
        """
        Test handling of a file containing 2 lines with the same key
        """
        with open("testdata/dupdata.csv", "rt") as file:
            file = CSVFile(file, "name")

            self.assertEqual(file.header.row, ["name","count"])

            i = iter(file)
            line = next(i)
            self.assertEqual(line.text, "apple,2\n")
            self.assertEqual(line.row, ["apple","2"])

            line = next(i)
            self.assertEqual(line.text, "apple,3\n")
            self.assertEqual(line.row, ["apple","3"])

            self.assertEqual(file.lines_by_key["apple"], [file[2], file[3]])

            with self.assertRaises(KeyError):
                lines = file.lines_by_key["banana"]

    def test_badkey(self):
        """
        Test handling of a file that does not contain the key
        """
        with open(self.simple_filename, "rt") as file:
            with self.assertRaises(KeyError):
                file = CSVFile(file, "notmyname")

class TestCursor(unittest.TestCase):
    simple_filename = "testdata/simple.csv"

    def test_cursor_open(self):
        """
        Test cursor over a simple file with 2 columns and 2 data rows
        """
        with open(self.simple_filename, "rt") as file:
            file = CSVFile(file, "name")
            cursor = Cursor(file)

            with self.assertRaises(IndexError):
                line = cursor.getline(-2)
            self.assertEqual(cursor[-2], None)

            self.assertEqual(cursor[-1].text, "name,count\n")
            self.assertEqual(cursor.getline(-1).text, "name,count\n")

            self.assertEqual(cursor[0].text, "apple,2\n")
            self.assertEqual(cursor[1].text, "banana,3\n")

            self.assertEqual(cursor[2], None)

            self.assertEqual(cursor.current_key(), "apple")

            cursor.advance()
            self.assertFalse(cursor.EOF())

            self.assertEqual(cursor[0].text, "banana,3\n")
            self.assertEqual(cursor.current_key(), "banana")

            cursor.advance()
            self.assertTrue(cursor.EOF())
            self.assertEqual(cursor.current_key(), None)

if __name__ == "__main__":
    unittest.main()

