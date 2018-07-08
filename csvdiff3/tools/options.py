#!/usr/bin/python3
#
# options.py
#
# Common options for the csv tool commands

import csv
import os

class Options:
    def __init__(self):
        self.quote = None
        self.key = None
        self.lineterminator = None

    def csv_kwargs(self):
        """
        Construct a keyword-arguments dict from the options, suitable
        for passing into a csv reader/writer initialiser as dialect
        options
        """

        kwargs = {}

        if self.lineterminator == "unix":
            kwargs["lineterminator"] = "\n"
        elif self.lineterminator == "dos":
            kwargs["lineterminator"] = "\r\n"
        elif self.lineterminator == "native":
            kwargs["lineterminator"] = os.linesep

        if self.quote == "minimal":
            kwargs["quoting"] = csv.QUOTE_MINIMAL
        elif self.quote == "full":
            kwargs["quoting"] = csv.QUOTE_ALL

        return kwargs
