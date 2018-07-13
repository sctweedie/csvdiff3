#!/usr/bin/python3
#
# csvdiff3/tools/hooks
#
# Simple utility functions useful for general manipulation/validation
# of CSV files

import csv
import sys

from .options import *

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def __check_key(reader, filename, key):
    if not key:
        return True

    try:
        header = next(reader)
    except StopIteration:
        eprint ("Error: file %s has no CSV header" % filename)
        return False

    if not key in header:
        eprint ("Error: file %s has no column %s" % (filename, key))
        return False

    return True

def validate(options, file, filename):
    key = options.key

    reader = csv.reader(file)

    if not __check_key(reader, filename, key):
        return 1

    try:
        for line, _ in enumerate(reader, 1):
            pass
    except:
        eprint ("Error: file %s failed to parse at line %d" % (filename, line))
        return 1

    return 0

def reformat(options, infile):
    key = options.key

    reader = csv.reader(infile)
    if not __check_key(reader, infile.name, key):
        return 1

    writer = csv.writer(sys.stdout, **options.csv_kwargs())

    try:
        for line, row in enumerate(reader, 1):
            writer.writerow(row)
    except:
        eprint ("Error: file %s failed to parse at line %d" % (file.name, line))
        return 1

    return 0
