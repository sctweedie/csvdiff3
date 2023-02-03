#!/usr/bin/python3

import click
import sys
import os
import shutil
import tempfile

import colorama

from .merge3 import merge3
from .output import Diff2OutputDriver
from .file import CSVKeyError
from .tools.options import *

@click.command()

@click.option("-c", "--colour/--nocolour", is_flag = True,
              default = None,
              help = "Colourise diffs in output (default is True " + \
              "if outputting to a tty)")
@click.option("-k", "--key",
              required = False,
              default = "[auto]",
              help = "Identifies column name for the CSV files' primary key " + \
              "(used to identify matching lines across changed files)")
@click.option("-d", "--debug", is_flag = True, default=False,
              help = "Enable logging in DEBUG.log")
@click.option("-r", "--show-reordered-lines", is_flag = True, default = False,
              help = "Show unchanged but reordered lines")
@click.argument("file1", type=click.Path(exists = True))
@click.argument("file2", type=click.Path(exists = True))

def cli_diff2(file1, file2,
              colour, key,
              debug, show_reordered_lines):

    colorama.init()

    if colour == None:
        colour = sys.stdout.isatty()

    output_args = {'show_reordered_lines': show_reordered_lines,
                   'preamble_extra_text': None}

    with open(file1, "rt") as file_LCA:
        # For 2-way diff, we just present the same file for
        # both A and B.
        with open(file2, "rt") as file_A:
            with open(file2, "rt") as file_B:
                try:
                    rc = merge3(file_LCA, file_A, file_B, key,
                                debug = debug,
                                colour = colour,
                                reformat_all = False,
                                output_driver_class = Diff2OutputDriver,
                                output_args = output_args,
                                filename_LCA = file1, filename_A = file2, filename_B = file2)
                except MergeFailedError as e:
                    print(f"{os.path.basename(sys.argv[0])}: Error: {e.message}", file=sys.stdout)
                    sys.exit(1)

    sys.exit(rc)


# We also include a variant diff command designed to run directly as a
# git diff driver, accepting all the arguments that git provides to a
# GIT_EXTERNAL_DIFF engine by default.
#
# This allows csvdiff_git to be used from a git config without needing
# a shell script wrapper.

# Arguments for git diff drivers are:
#
# filename old_file old_hex old_mode new_file new_hex new_mode
#
# where
# * filename is the name of the file as stored in git
# * {old|new}_file are the temporary files for diff
# * {old|new}_hex are the git hash of the old/new blobs
# * {old|new}_mode are the file mode bits

@click.command()

@click.option("-c", "--colour/--nocolour", is_flag = True,
              default = None,
              help = "Colourise diffs in output (default is True " + \
              "if outputting to a tty)")
@click.option("-k", "--key",
              required = False,
              default = "[auto]",
              help = "Identifies column name for the CSV files' primary key " + \
              "(used to identify matching lines across changed files)")
@click.option("-d", "--debug", is_flag = True, default = False,
              help = "Enable logging in DEBUG.log")
@click.option("-r", "--show-reordered-lines", is_flag = True, default = False,
              help = "Show unchanged but reordered lines")
@click.argument("file_common_name", type=click.Path(exists = False))
@click.argument("old_file", type=click.Path(exists = True))
@click.argument("old_hex", type=str)
@click.argument("old_mode", type=str)
@click.argument("new_file", type=click.Path(exists = True))
@click.argument("new_hex", type=str)
@click.argument("new_mode", type=str)
@click.argument("new_name", type=str, required = False, default = None)
@click.argument("extra_text", type=str, required = False, default = None)

def cli_diff2_git(file_common_name,
                  old_file, old_hex, old_mode,
                  new_file, new_hex, new_mode,
                  new_name, extra_text,
                  colour, key,
                  debug, show_reordered_lines):

    colorama.init()

    # For git output, we masquerade the command name as "csvdiff"
    # rather than the full "csvdiff_git" in the diff preamble.
    #
    # The preamble is always formatted to present normal csvdiff args,
    # not the 7-fold args of csvdiff_git, so there's no point in
    # directing the user to invoke the _git cli with the simplified
    # arguments.

    sys.argv[0] = "csvdiff"

    if colour == None:
        colour = sys.stdout.isatty()

    output_args = {'show_reordered_lines': show_reordered_lines,
                   'preamble_extra_text': extra_text}

    with open(old_file, "rt") as file_LCA:
        # For 2-way diff, we just present the same file for
        # both A and B.
        with open(new_file, "rt") as file_A:
            with open(new_file, "rt") as file_B:
                try:
                    rc = merge3(file_LCA, file_A, file_B, key,
                                debug = debug,
                                colour = colour,
                                reformat_all = False,
                                file_common_name = file_common_name,
                                output_driver_class = Diff2OutputDriver,
                                output_args = output_args,
                                filename_LCA = old_file, filename_A = new_file, filename_B = new_file)
                except MergeFailedError as e:
                    print(f"{os.path.basename(sys.argv[0])}: Error: {e.message}", file=sys.stdout)
                    sys.exit(1)

    sys.exit(rc)

if __name__ == "__main__":
    cli_diff2()
