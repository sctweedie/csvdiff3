#!/usr/bin/python3

from . import merge3, file, headers

import click
import sys
import shutil
import tempfile

@click.command()

@click.argument("filename_LCA", type=click.File("rt"))
@click.argument("filename_A", type=click.File("rt"))
@click.argument("filename_B", type=click.File("rt"))
@click.option("-k", "--key", required=True)
@click.option("-d", "--debug", is_flag = True, default=False)
@click.option("-o", "--output-file", type=click.Path(),
              default = None,
              help = "Save merged results to given output file (default is stdout)")

def merge3_cli(filename_lca, filename_a, filename_b, key, debug, output_file):

    # If an output filename has been specified, redirect output to a
    # temporary file and then we will copy it over at the end.  We
    # don't wan't to accidentally overwrite an input file as we are
    # processing.

    if output_file:

        with tempfile.NamedTemporaryFile("wt") as temp_output:
            rc = merge3.merge3(filename_lca, filename_a, filename_b, key,
                               debug = debug,
                               output = temp_output)

            temp_name = temp_output.name
            temp_output.flush()
            shutil.copyfile(temp_name, output_file)

    else:

        rc = merge3.merge3(filename_lca, filename_a, filename_b, key,
                           debug = debug)

    sys.exit(rc)

if __name__ == "__main__":
    merge3_cli()
