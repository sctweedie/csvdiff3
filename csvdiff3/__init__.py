#!/usr/bin/python3

from . import merge3, file, headers

import click

@click.command()

@click.argument("filename_LCA", type=click.File("rt"))
@click.argument("filename_A", type=click.File("rt"))
@click.argument("filename_B", type=click.File("rt"))
@click.option("-k", "--key", required=True)
@click.option("-d", "--debug", is_flag = True, default=False)

def merge3_cli(filename_lca, filename_a, filename_b, key, debug):
    merge3(filename_lca, filename_a, filename_b, key, debug)

if __name__ == "__main__":
    merge3_cli()
