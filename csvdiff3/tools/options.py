#!/usr/bin/python3
#
# options.py
#
# Common options for the csv tool commands

import csv
import os

class MergeFailedError(Exception):
    def __init__(self, message, *args):
        self.message = message

        super(MergeFailedError, self).__init__(message, *args)

class Options:
    def __init__(self, quote = None, key = None, lineterminator = None):
        self.quote = quote
        self.key = key
        self.lineterminator = lineterminator

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
        elif self.quote == "all":
            kwargs["quoting"] = csv.QUOTE_ALL
        elif self.quote == "nonnumeric":
            kwargs["quoting"] = csv.QUOTE_NONNUMERIC

        return kwargs
