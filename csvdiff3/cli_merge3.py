#!/usr/bin/python3

import click
import sys
import os
import shutil
import tempfile

import colorama

from .merge3 import merge3
from .output import Merge3OutputDriver
from .file import CSVKeyError

@click.command()

@click.argument("filename_LCA", type=click.File("rt"))
@click.argument("filename_A", type=click.File("rt"))
@click.argument("filename_B", type=click.File("rt"))
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
@click.option("-q", "--quote",
              type = click.Choice(["minimal", "nonnumeric", "all"]),
              default = "minimal",
              help = "Selects field quoting style for output CSV files")
@click.option("-l", "--lineterminator",
              type = click.Choice(["native", "dos","unix"]),
              default = "unix",
              help = "Selects line termination for output CSV files")
@click.option("-r", "--reformat-all", is_flag = True, default=False,
              help = "Reformat all lines in output (default is " + \
              "reformat only changed lines)")
@click.option("-o", "--output-file", type=click.Path(),
              default = None,
              help = "Save merged results to given output file (default is stdout)")

def cli_merge3(filename_lca, filename_a, filename_b,
               colour, key,
               debug, quote, lineterminator, reformat_all,
               output_file):

    # If an output filename has been specified, redirect output to a
    # temporary file and then we will copy it over at the end.  We
    # don't wan't to accidentally overwrite an input file as we are
    # processing.

    colorama.init()

    if colour == None:
        colour = (sys.stdout.isatty() and not output_file)

    if output_file:

        with tempfile.NamedTemporaryFile("wt") as temp_output:
            try:
                rc = merge3(filename_lca, filename_a, filename_b, key,
                            output = temp_output,
                            debug = debug,
                            colour = colour,
                            quote = quote,
                            lineterminator = lineterminator,
                            reformat_all = reformat_all,
                            output_driver_class = Merge3OutputDriver,
                            filename_LCA = filename_lca, filename_A = filename_a, filename_B = filename_b)
            except CSVKeyError as e:
                print(f"{os.path.basename(sys.argv[0])}: Error: {e.message}", file=sys.stdout)
                sys.exit(1)

            temp_name = temp_output.name
            temp_output.flush()
            shutil.copyfile(temp_name, output_file)

    else:

        try:
            rc = merge3(filename_lca, filename_a, filename_b, key,
                        debug = debug,
                        colour = colour,
                        quote = quote,
                        lineterminator = lineterminator,
                        reformat_all = reformat_all,
                        output_driver_class = Merge3OutputDriver,
                        filename_LCA = filename_lca, filename_A = filename_a, filename_B = filename_b)
        except CSVKeyError as e:
            print(f"{os.path.basename(sys.argv[0])}: Error: {e.message}", file=sys.stdout)
            sys.exit(1)

    sys.exit(rc)

if __name__ == "__main__":
    cli_merge3()
