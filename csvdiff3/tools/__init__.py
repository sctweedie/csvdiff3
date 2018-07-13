#!/usr/bin/python3

import click
import sys

import csvdiff3.tools.output
import csvdiff3.tools.tools
from .options import Options

# Top level CLI group for options common across the subcommands

@click.group()

@click.option("-k", "--key", default = None,
              help = "File is indexed by a given primary key " + \
              "(a column with this name must exist)")
@click.option("-q", "--quote",
              type = click.Choice(["minimal", "nonnumeric", "all"]),
              default = "minimal",
              help = "Selects field quoting style for output CSV files")
@click.option("-l", "--lineterminator",
              type = click.Choice(["native", "dos","unix"]),
              default = "unix",
              help = "Selects line termination for output CSV files")

@click.pass_context
def csvhooks(ctx, key, quote, lineterminator):
    options = ctx.obj
    options.key = key
    options.quote = quote
    options.lineterminator = lineterminator

# "validate" subcommand

@csvhooks.command("validate")
@click.argument("file", type=click.File("rt"),
                required = False)
@click.pass_context

def validate_cli(ctx, file):
    if file:
        filename = file.name
    else:
        file, filename = (sys.stdin, "<stdin>")

    options = ctx.obj
    rc = tools.validate(options, file, filename)
    sys.exit(rc)

# "reformat" subcommand

@csvhooks.command("reformat")
@click.argument("infile", type=click.File("rt"),
                required = False)
@click.argument("outfile", type=click.Path(),
                required = False,
                default = None)
@click.pass_context

def reformat_cli(ctx, infile, outfile):
    options = ctx.obj

    # If we are working as a strict filter, don't redirect stdout at all

    if not (infile or outfile):
        rc = tools.reformat(options, sys.stdin)
        sys.exit(rc)

    # Otherwise, if we only have an input file specified, default is
    # to rewrite output to that file.

    if not outfile:
        outfile = infile.name

    with output.safe_redirect_stdout([infile], outfile):
        rc = tools.reformat(options, infile)
    sys.exit(rc)

# Provide this as a callable hook for setup.py consolescripts

def csvhooks_cli():
    csvhooks(obj=Options())

if __name__ == "__main__":
    csvhooks_cli()
