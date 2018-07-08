import sys
import os
from contextlib import contextmanager, redirect_stdout
from stat import *
from tempfile import NamedTemporaryFile

@contextmanager
def safe_redirect_stdout(existing_files, new_filename, mode = "w+t"):
    """
    Output redirection helper.

    Creates a context within which stdout is redirected to a given
    filename.

    Takes a sequence of additional optional existing files as input;
    these input files will *not* be overwritten during the context.
    If the output filename is the same as any one of the input files,
    then stdout will be instead be redirected to a temporary file;
    only when the context finishes will that file be renamed to the
    final filename.
    """

    # Redirecting to "-" will leave stdout as it is; we do nothing in
    # that case.

    if new_filename == "-":
        yield
        return

    try:
        newfile_stat = os.stat(new_filename)

        for file in existing_files:
            file_stat = os.fstat(file.fileno())

            # Both stat() calls succeeded; does the new_filename point to
            # the existing open file?
            if file_stat[ST_DEV] == newfile_stat[ST_DEV] and \
               file_stat[ST_INO] == newfile_stat[ST_INO]:

                # We found a match between the new filename and one of
                # the existing files.  So we need to redirect output
                # to a safe temporary file first, and only move that
                # to the target path at the end.

                directory = os.path.dirname(new_filename)
                with NamedTemporaryFile(mode = mode,
                                        dir = directory,
                                        delete = False) as tmpfile:
                    tmpname = tmpfile.name
                    with redirect_stdout(tmpfile):
                        yield

                os.rename(tmpname, new_filename)
                return

    except FileNotFoundError:

        # If the stat of the new filename failed, then that's fine,
        # we're just about to create it.

        pass

    # The new filename matched none of the existing files, so we will
    # just redirect stdout to the new name immediately.

    with open(new_filename, mode = mode) as outfile:
        with redirect_stdout(outfile):
            yield
