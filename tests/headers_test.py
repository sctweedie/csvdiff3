#!usr/bin/python3

import unittest
from csvdiff3.headers import *

def headers_merge3(LCA, A, B):
    headers = Headers(LCA, A, B)
    return headers.header_map

class TestHeaders(unittest.TestCase):
    def check_column(self, headers, name, column):
        if column == None:
            self.assertFalse(name in headers)
        else:
            self.assertEqual(headers[column], name)

    def merge3(self, LCA, A, B, expected):
        headers = headers_merge3(LCA, A, B)
        map_names = [x.name for x in headers]
        self.assertEqual(map_names, expected)
        for map in headers:
            self.check_column(LCA, map.name, map.LCA_column)
            self.check_column(A, map.name, map.A_column)
            self.check_column(B, map.name, map.B_column)

    # Merging three copies of the same headers should result in the
    # same output, always
    def merge_3_copies(self, headers):
        self.merge3(headers, headers, headers, headers)

    # Merging the same changes in A and B to the LCA should always
    # result in a copy of that change
    def merge_copied_changes(self, headers_LCA, headers_new):
        self.merge3(headers_LCA, headers_new, headers_new, headers_new)

    # Merge a change on the A side (B and the LCA remain the same)
    def merge_changed_A(self, headers_LCA, headers_A, headers_new):
        self.merge3(headers_LCA, headers_A, headers_LCA, headers_new)

    # Merge a change on the B side (A and the LCA remain the same)
    def merge_changed_B(self, headers_LCA, headers_B, headers_new):
        self.merge3(headers_LCA, headers_LCA, headers_B, headers_new)

    def test_merge_3_copies(self):
        self.merge_3_copies(["name"])
        self.merge_3_copies(["name", "value"])
        self.merge_3_copies(["name", "value", "more"])

    def test_merge_same_new_add(self):
        """
        Test adding new fields to both A and B.
        """
        self.merge_copied_changes(["name"], ["name", "value"])
        self.merge_copied_changes(["name"], ["value", "name"])
        self.merge_copied_changes(["name", "value"], ["more", "name", "value"])
        self.merge_copied_changes(["name", "value"], ["name", "more", "value"])
        self.merge_copied_changes(["name", "value"], ["name", "value", "more"])

    def test_merge_same_move(self):
        """
        Test moving fields in the same way in both A and B.
        """
        self.merge_copied_changes(["name", "value"], ["value", "name"])
        self.merge_copied_changes(["name", "value", "more"], ["value", "name", "more"])
        self.merge_copied_changes(["name", "value", "more"], ["name", "more", "value"])
        self.merge_copied_changes(["name", "value", "more"], ["more", "name", "value"])

    def test_merge_same_delete(self):
        """
        Test deleting fields in the same way in both A and B.
        """
        self.merge_copied_changes(["name", "value"], ["name"])
        self.merge_copied_changes(["name", "value"], ["value"])
        self.merge_copied_changes(["name", "value", "more"], ["name", "more"])
        self.merge_copied_changes(["name", "value", "more"], ["name"])

    def test_append_onesided(self):
        """
        Test appending different fields in either A or B.
        """
        self.merge_changed_A(["name"], ["name", "value"], ["name", "value"])
        self.merge_changed_B(["name"], ["name", "value"], ["name", "value"])

    def test_delete_onesided(self):
        """
        Test deleting different fields in either A or B.
        """
        self.merge_changed_A(["name", "value"], ["name"], ["name"])
        self.merge_changed_A(["name", "value"], ["value"], ["value"])
        self.merge_changed_B(["name", "value"], ["name"], ["name"])
        self.merge_changed_B(["name", "value"], ["value"], ["value"])

    def test_move_onesided(self):
        """
        Test moving different fields either A or B.
        """
        self.merge_changed_A(["name", "value"], ["value", "name"], ["value", "name"])
        self.merge_changed_A(["name", "value", "more"],
                             ["more", "name", "value"],
                             ["more", "name", "value"])
        self.merge_changed_A(["name", "value", "more"],
                             ["value", "more", "name"],
                             ["value", "more", "name"])
        self.merge_changed_B(["name", "value"], ["value", "name"], ["value", "name"])
        self.merge_changed_B(["name", "value", "more"],
                             ["more", "name", "value"],
                             ["more", "name", "value"])
        self.merge_changed_B(["name", "value", "more"],
                             ["value", "more", "name"],
                             ["value", "more", "name"])

    def test_move_complex_conflicts(self):
        """
        Test moving, adding and deleting different fields in both A and B.
        """
        
        self.merge3(["A", "B", "C", "D", "E", "F", "G", "H"],
                    # delete B, add I, move E
                    ["A", "C", "D", "F", "G", "I", "H", "E"],
                    # delete C, move G
                    ["G", "A", "B", "D", "E", "F", "H"],
                    ["G", "A", "D", "F", "I", "H", "E"])
        self.merge3(["A", "B", "C", "D", "E", "F", "G", "H"],
                    # delete C, move G
                    ["G", "A", "B", "D", "E", "F", "H"],
                    # delete B, add I, move E
                    ["A", "C", "D", "F", "G", "I", "H", "E"],
                    ["G", "A", "D", "F", "I", "H", "E"])


if __name__ == "__main__":
    unittest.main()

