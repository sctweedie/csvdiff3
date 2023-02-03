#!/usr/bin/python3
#
# headers.py:
#
#  Header class for maintaining the output header column names in the
#  correct order

from .file import Line
import logging

class HeaderMap:
    """
    Define a single column in the output file.  Records the name of
    the column, plus the index (starting at 0) of the
    corresponding source column in each input file.

    Column numbers will be None if the column does not appear in a
    given input file.
    """

    def __init__(self, name,
                 LCA_column = None,
                 A_column = None,
                 B_column = None):
        self.name = name
        self.LCA_column = LCA_column
        self.A_column = A_column
        self.B_column = B_column

    @staticmethod
    def from_state(state, column):
        LCA_column = state.find_column(state.orig_LCA, column)
        A_column = state.find_column(state.orig_A, column)
        B_column = state.find_column(state.orig_B, column)

        return HeaderMap(column, LCA_column, A_column, B_column)

class Headers:
    class __State:

        """
        Internal processing state for the merging of headers
        """
        def __init__(self, header_LCA, header_A, header_B):
            # For processing, also store an enumerated version, ie. lists
            # of (column number, header name) tuples
            self.LCA = [x for x in enumerate(header_LCA)]
            self.A = [x for x in enumerate(header_A)]
            self.B = [x for x in enumerate(header_B)]

            # As we process columns we will gradually consume entries
            # from the three LCA, A and B lists.  But maintain a copy
            # of the original lists too, so that we can extract column
            # keys for each file even when keys are moving around
            # between files.

            self.orig_LCA = self.LCA
            self.orig_A = self.A
            self.orig_B = self.B

        def no_more_input(self):
            """
            Test if we have reached the end of the input (ie. we have
            consumed all columns for all input files)
            """
            return self.LCA == [] and self.A == [] and self.B == []

        @staticmethod
        def __next(headerlist):
            """
            Return the next entry for a given list of input columns.

            Returns the enumerated tuple (N, name) where N is the
            index of the column in the source file (starting at 0),
            and name is the name of the column.

            Returns (None, None) if there are no columns left to
            process in the given input file.
            """
            if headerlist == []:
                return (None, None)
            return headerlist[0]

        def show(self):
            print ("State: LCA", self.LCA, "A", self.A, "B", self.B)

        def next_LCA(self):
            return self.__next(self.LCA)

        def next_A(self):
            return self.__next(self.A)

        def next_B(self):
            return self.__next(self.B)

        def advance_LCA(self):
            self.LCA = self.LCA[1:]

        def advance_A(self):
            self.A = self.A[1:]

        def advance_B(self):
            self.B = self.B[1:]

        @staticmethod
        def find_column(list, column):
            try:
                entry = next(x for x in list if x[1] == column)
                return entry[0]
            except StopIteration:
                return None

        @staticmethod
        def __consume1(list, name):
            try:
                entry = next(x for x in list if x[1] == name)
                list.remove(entry)
            except StopIteration:
                pass

        def consume(self, name):
            self.__consume1(self.LCA, name)
            self.__consume1(self.A, name)
            self.__consume1(self.B, name)

    # We expect a CSV file's header to be well-formed: to have a
    # unique, non-empty name for each column.
    #
    # But for robustness, we want to handle badly-formed headers.  For
    # that, we'll construct a properly-formed internal name for each
    # column name in each header:
    #
    # * Empty column names are filled out as "[*unlabeled*]", and
    # * Duplicated column names are made unique by appending them with
    #   a [%count] suffix.

    @staticmethod
    def _uniquify(headers):
        seen = set()
        output = []
        count = {}

        for h in headers:
            if h == "":
                h = "[*unlabeled*]"

            if h in seen:
                if h not in count:
                    count[h] = 0

                count[h] = count[h] + 1
                h = f"{h}[{count[h]}]"

            else:
                seen.add(h)

            output.append(h)

        return output

    def __init__(self, header_LCA, header_A, header_B):
        Debug = False

        # Store in the object itself the original lists of header names

        header_LCA = self._uniquify(header_LCA)
        header_A = self._uniquify(header_A)
        header_B = self._uniquify(header_B)

        self.LCA = header_LCA
        self.A = header_A
        self.B = header_B
        self.full_output_map = None

        # If any of the headers are differ then we will be doing
        # remapping; this will disable some exact-text-match
        # optimisations in the main merge code.

        self.need_remapping = not (self.LCA == self.A == self.B)

        state = Headers.__State(header_LCA, header_A, header_B)

        #
        # Now we walk through the headers from all three files,
        # constructing the final destination header list
        #

        output = []

        while not state.no_more_input():

            if Debug:
                print("Output now", [h.name for h in output])
                state.show()

            col_LCA, next_LCA = state.next_LCA()
            col_A, next_A = state.next_A()
            col_B, next_B = state.next_B()

            # Easy case: if all three files agree on the next column,
            # then we can add that and move on.

            if next_LCA == next_A and next_LCA == next_B:
                map = HeaderMap.from_state(state, next_A)
                output.append(map)

                state.consume(next_LCA)
                continue

            # If files A and B agree but LCA disagrees, then the same
            # column has been moved, added or deleted in both merge
            # files.

            if next_A == next_B:
                # Have we simply run out of columns in A and B?  If
                # so, any remaining fields in LCA have been removed in
                # both merge branches, so we are done with processing
                # now.
                if not next_A:
                    break

                # Two possible cases: the column is completely new in
                # A+B, or it has simply moved.

                # If it was in the original file in a different order,
                # then remove it from the working list so we don't try
                # to deal with it again
                if next_A in self.LCA:
                    where_in_LCA = state.find_column(state.LCA, next_A)
                    map = HeaderMap.from_state(state, next_A)
                    output.append(map)

                    state.consume(next_A)
                    continue

                else:
                    # Otherwise it's a new column that is present in both
                    # merge files, so add it with no reference to the LCA
                    # at all.
                    map = HeaderMap.from_state(state, next_A)
                    output.append(map)

                    state.consume(next_A)
                    continue

            # A and B are different.  Process the differences, bearing
            # in mind that if a column is moved in both A and B, we
            # prefer the order in A.

            # First, if we have already run out of columns fields in
            # LCA, then anything remaining to be processed in A or B
            # must be new columns (or moved columns carried forward
            # from earlier processing.)
            if not next_LCA:
                # As we prefer the order of columns in A over B, we
                # add new columns from A first.  Once both LCA and A
                # are empty, we can continue with columns in B.
                if next_A:
                    map = HeaderMap.from_state(state, next_A)
                    output.append(map)

                    state.consume(next_A)
                else:
                    map = HeaderMap.from_state(state, next_B)
                    output.append(map)

                    state.consume(next_B)
                continue

            # LCA is non-NULL.  A and B are different; are either of
            # them the same as LCA?  If so, then it is the _other_
            # merge file that contains the change we need to include.
            if next_LCA == next_A:
                # Scenario: a key has been deleted from B, but is
                # still in A
                #
                # eg.
                # LCA: P Q R S
                # A:   P Q R S
                # B:   P R S ("Q" deleted from side B)
                if not next_A in self.B:
                    # Don't construct a map for the key, as the column
                    # is deleted; simply consume the removed key and
                    # continue
                    state.consume(next_A)
                    continue

                # Scenario: a key has moved to an earlier position in
                # side B but is still present in both sides
                #
                # eg.
                # LCA: P Q R S
                # A:   P Q R S
                # B:   P S Q R ("S" moved earlier in column list. next_B is "S")
                if next_B in self.A:
                    # FIXME
                    # We should really honour the position in A if A
                    # and B have both moved the column.
                    #
                    # But for now it is much easier to emit the column
                    # here while we are looking at it.  It will still
                    # include all of the correct columns, it just
                    # means that in very complex situations we might
                    # prefer a move in B over one in A.
                    #
                    # Colliding column moves are not so common that I
                    # want to worry about this.
                    map = HeaderMap.from_state(state, next_B)
                    output.append(map)

                    state.consume(next_B)
                    continue

                # Scenario: a key has moved to a later position in
                # side B but is still present in both sides
                #
                # eg.
                # LCA: P Q R S
                # A:   P Q R S
                # B:   P R S Q ("Q" moved later in column list. next_B is "R")
                #
                # We will simply advance LCA and A past the column
                # which has moved in B; we will pick it up from B
                # later on as an insert.
                state.advance_LCA()
                state.advance_A()
                continue

            # LCA is non-NULL.  A and B are different; A is not equal
            # to LCA, but is B?  If so, then the change at A is the
            # one to preserve
            if next_LCA == next_B:
                # Scenario: a key has been deleted from A, but is
                # still in B
                #
                # eg.
                # LCA: P Q R S
                # A:   P R S ("Q" deleted from side A)
                # B:   P Q R S
                if not next_B in self.A:
                    # Don't construct a map for the key, as the column
                    # is deleted; simply consume the removed key and
                    # continue
                    state.consume(next_B)
                    continue

                # Scenario: a key has moved to an earlier position in
                # side A but is still present in both sides
                #
                # eg.
                # LCA: P Q R S
                # A:   P S Q R ("S" moved earlier in column list. next_A is "S")
                # B:   P Q R S
                if next_A in self.B:
                    # Emit the key in the current position, and
                    # consume it from further processing.
                    map = HeaderMap.from_state(state, next_A)
                    output.append(map)

                    state.consume(next_A)
                    continue

                # Scenario: a key has moved in side A but is still
                # present in both sides
                #
                # eg.
                # LCA: P Q R S
                # A:   P R S Q ("Q" moved later in column list. next_A is "R")
                # B:   P Q R S
                #
                # We will simply advance LCA and B past the column
                # which has moved in A; we will pick it up from A
                # later on as an insert.
                state.advance_LCA()
                state.advance_B()
                continue

            # LCA is non-NULL, and LCA, A and B are all different.
            # If A or B is empty, that indicates a deletion: something
            # in LCA is not in A or B, so handle that first.
            if not next_A:
                # The next key in LCA has been deleted from A; consume
                # it and move on.
                state.consume(next_LCA)
                continue

            if not next_B:
                # The next key in LCA has been deleted from B; consume
                # it and move on.
                state.consume(next_LCA)
                continue

            # LCA, A and B are all different and are all non-NULL.
            # Something has been moved around in both A and B, or
            # added; we always prefer A as the definition of the order
            # in this case.
            map = HeaderMap.from_state(state, next_A)
            output.append(map)

            state.consume(next_A)

        if Debug:
            print("Final output: ", [h.name for h in output])

        self.header_map = output
        self.headers = [h.name for h in output]

        logging.debug("Header map:")
        for column, map in enumerate(self.header_map):
            logging.debug('  Column %d ("%s"): from LCA %s, A %s, B %s' %
                          (column, map.name,
                           map.LCA_column, map.A_column, map.B_column))

    # For 3-way merge, we only care about mapping which columns appear
    # where in the output.
    #
    # But for diffs, we need to record both added and removed columns;
    # so generate an additional map that inserts removed columns at
    # appropriate places in the map.

    def map_all_headers(self):

        if self.full_output_map:
            return self.full_output_map

        def add_to_output_map(key):
            """Add a given key to the output map, also removing that key from the
            list of outstanding keys in each key source"""

            nonlocal output_map, left_in_LCA, left_in_A, left_in_B, left_in_map, state;

            map = HeaderMap.from_state(state, key)
            output_map.append(map)

            if map.LCA_column != None:
                left_in_LCA.remove((map.LCA_column, key))
            if map.A_column != None:
                left_in_A.remove((map.A_column, key))
            if map.B_column != None:
                left_in_B.remove((map.A_column, key))

            if key in left_in_map:
                left_in_map.remove(key)

        header_LCA = self.LCA
        header_A = self.A
        header_B = self.B

        all_headers = set(header_LCA) | set(header_A) | set(header_B)
        map_headers = [map.name for map in self.header_map]
        missing_headers = all_headers.difference(map_headers)

        # Make a copy of all the keys still left in headers A and B
        left_in_LCA = list(enumerate(header_LCA))
        left_in_A = list(enumerate(header_A))
        left_in_B = list(enumerate(header_B))

        left_in_map = list(map_headers)

        output_map = []

        # Now construct a new map, trying to preserve as much order as
        # possible from both the added and removed keys.
        #
        # We will iterate over the main output map, copying those keys
        # to the full map of all keys, removing each key from the list
        # of leftovers in LCA/A/B as we go.  As we remove keys,
        # whenever we discover that a missing key has reached the
        # starting position of any leftover list, we will add that key
        # next.
        #
        # This should result in missing keys being reinsterted at
        # roughly the location they were previously in (although if
        # keys are being reordered, there will be no exact correct
        # location for them.)

        state = Headers.__State(header_LCA, header_A, header_B)

        while left_in_LCA or left_in_A or left_in_B:

            if left_in_LCA:
                _, next_LCA = left_in_LCA[0]
                if next_LCA in missing_headers:
                    add_to_output_map(next_LCA)
                    continue

            if left_in_A:
                _, next_A = left_in_A[0]
                if next_A in missing_headers:
                    add_to_output_map(next_A)
                    continue

            if left_in_B:
                _, next_B = left_in_B[0]
                if next_B in missing_headers:
                    add_to_output_map(next_B)
                    continue

            next_map = left_in_map[0]
            add_to_output_map(next_map)

        self.full_output_map = output_map

        return output_map

