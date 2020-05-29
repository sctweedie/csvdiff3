#!/usr/bin/python3

import click
import sys
import os
import shutil
import tempfile

import colorama

from .merge3 import merge3
from .output import Diff2OutputDriver

@click.command()

@click.argument("file1", type=click.Path(exists = True))
@click.argument("file2", type=click.Path(exists = True))
@click.option("-c", "--colour/--nocolour", is_flag = True,
              default = None,
              help = "Colourise conflicts in output (default is True " + \
              "if outputting to a tty)")
@click.option("-k", "--key",
              required=True,
              help = "Identifies column name for the CSV files' primary key " + \
              "(used to identify matching lines across merged files)")
@click.option("-d", "--debug", is_flag = True, default=False,
              help = "Enable logging in DEBUG.log")

def cli_diff2(file1, file2,
              colour, key,
              debug):

    colorama.init()

    if colour == None:
        colour = sys.stdout.isatty()

    with open(file1, "rt") as file_LCA:
        # For 2-way diff, we just present the same file for
        # both A and B.
        with open(file2, "rt") as file_A:
            with open(file2, "rt") as file_B:

                  rc = merge3(file_LCA, file_A, file_B, key,
                              debug = debug,
                              colour = colour,
                              reformat_all = True,
                              output_driver_class = Diff2OutputDriver)

    sys.exit(rc)

if __name__ == "__main__":
    cli_diff2()
