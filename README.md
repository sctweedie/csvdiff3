# csvdiff3
#
# README.md for 3-way csv merge utility

csvdiff3 offers the csvmerge3 command, which performs intelligent
3-way merging between CSV files.

It can:

* Automatically merge changes in file structure, such as added,
  removed or moved columns;

* Merge multiple changes to an individual line (identified by a given
  primary key), automatically merging changes to distinct fields in
  the line and creating conflict markers for conflicting changes to
  the same fields

* Handle lines being reordered in different files, merging the order
  sensibly and identifying matching lines for line merging

* Correctly handle formatting changes (eg. changes in CSV quoting that
  have no effect on the contents of the fields), preserving formatting
  in lines which are unchanged but reformatting lines on merge if
  lines have changed

The package also includes a csvhook CLI tool to provide basic CSV
validation/formatting for use in git hooks and filters.
