# csvdiff3 : 3-way diff/merge tools for CSV files.

The csvdiff3 package offers the csvmerge3 command, which performs
intelligent 3-way merging between CSV files.  2- and 3-way diff are
planned but are not currently part of the package.

cvsmerge3 can:

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

## Getting started

Use

	python3 setup.py install [--user]

to install the package.

## How to use csvmerge3

A 3-way merge involves merging together two separate sets of changes
to the same original file.  csvmerge3 identifies the changes versus
the original file and attempts to apply both sets of changes to the
output; where it detects a conflict, it will emit a conflict marker
similar to those output by normal "diff".

The original file is termed the "LCA": Latest Common Ancestor, and we
refer to the two sets of changes to be merged as the A and B sides of
the merge.  The LCA, A and B files are supplied to csvmerge3 as
filenames on the command line.  Output may be to stdout, or may be
sent to a given file with the `-o filename` option.

csvmerge3 operates on CSV files where the first row of the file is a
header, providing names for each column of the file.  One of these
columns must be identified as the **primary key**.  The merge will
attempt to match together lines from each file based on the primary
key, even if those lines appear in a different order in different
source files (although it will try to preserve the order in the input
files as much as possible.)

csvmerge3 will identify the following changes and attempt to merge
them together:

* Changes in columns: A or B has added, deleted or moved a column.

* Changes in line order: A or B has inserted, deleted or moved a line.
  (Lines are identified by primary key; so if the primary key of a
  line changes, that will be seen as a deletion of the old line and an
  insertion of a new one.)

* Changes in the field contents of a given line.

In all cases,

* If the same change is applied on both the A or B sides, then that
  change is applied on the output;

* If content (columns or lines) are inserted or moved in either A or
  B, then those moves are reflected in the output;

* Compatible changes are merged together; eg. if a line is moved in A
  and its contents are changed in B, then both the reordering and the
  new content will be present in the output;

  or if A and B make changes to different columns of the same line,
  then all those changes will be merged in the output;

* Incompatible changes are marked with a conflict block.  Incompatible
  changes include A and B making different changes to the same field
  within a single line, or one side of the merge deleting a line while
  the other side modifies that same line.

**NB** csvmerge3 does *not* create a conflict if a column is deleted
entirely in one side of the merge, and that column's contents are
modified on the other.

A conflict block could look like this:

	name,count
	apple,2
	>>>>>> input @3 (banana)
	>>>>>> count = 4
	banana,4
	====== input @3 (banana)
	====== count = 5
	banana,5
	<<<<<<

The conflict block is the content between the first >>>>>> marker and
the final <<<<<<.  The line above the ====== markers represents the
line as it appears in the A file; below that marker is the B version
of the line.

The content indicates both the line number (@linenr) and key (banana)
for the source lines in A and B; followed by a line for each field in
which there is a conflict.  In the example above, the A and B files
have assigned two different changes to the "count" column for the line
with name "banana", and the value of that field in A and B is listed
before the contents of the full line itself.

If a line is deleted on one side of the merge, but modified on the
other, then the missing line will be identified in its line-number
marker, and the contents of any fields with conflict will appear as
"None".  For example

	>>>>>> input Deleted @5
	>>>>>> count = None

A field marker line in a conflict block will always consist of just
one line.  If a CSV row covers multiple lines in the input (because a
newline has been included in a field by quoting/escaping), then that
newline will appear as a "\n" in the conflict marker line.  But in
this case, the full output row will still appear exactly as it appears
in the input, potentially taking more than one line of the output
within the conflict block.

As with most merges, in order to complete the merge manually, the user
is expected to search for all conflict markers and decide the correct
content within them.  This can often be done by selecting one of the
original lines above or below the ====== markers, and deleting the
rest of the conflict block; but sometimes the user may need to
manually edit the line contents to select the desired field contents.
csvmerge3 does not automate selection of which fields are wanted in
the output.

## Using csvmerge3 automatically within git

csvmerge3 can help with 3-way merge operations in git.  Typically,
these merges are performed by git when you request a `git pull` to
merge two different branches together.

To configure git to use csvmerge3, you will need to define a `merge
driver` in a .git/config file: eg.

    [merge "csv_pkg"]
	    name = 3-way CSV merge
	    driver = csvmerge3 --key=\"Package Name\" --quote=all --reformat-all %O %A %B -o %A
	    recursive = text

NB. this is not just a generic merge driver for all CSV files;
csvmerge3 needs to know what the primary key is for a CSV file, so
that it can identify which lines from different input files are
supposed to match each other.  You can have CSV files with different
primary keys within a single git repository, but you will need to
define a specific merge driver with the right primary key on the
csvmerge3 command line for each such key that you want to use.

You must have a `--key` option defined for the csvmerge3 command in a
merge driver.  Optionally, you can also add `--quote` ,
`--lineterminator` or `--reformat-all` options to coerce output into a
specific format.

Once the merge driver is defined, you can set up a line in a
`.gitattributes` file to enable it for a given class of CSV files:

    fruit.csv	 merge=csv_fruit
	*.csv		 merge=csv_pkg

`git rebase` does not normally use 3-way merging, but it can be told
to do so by using the `-m` option.  In this case, it will use
csvmerge3 if that is configured as the merge driver for a given file.

## Utility commands to help manage CSV files in a git repo

The csvdiff3 package also comes with a `csvhooks` command that
provides features usable within git hooks or filters.

One use case is to set up a "filter driver" to make sure that CSV
files in the repository are always stored and compared in a consistent
format (eg. with a specific line terminator, or with particular
quoting rules).

A filter driver can also make `git diff` more readable by ignoring
line-termination or quotation formating changes and showing only
actual changed data.

The filter driver is set up with a block in `.git/config` similar to:

    [filter "csv"]
	    name = CSV normalisation filter
	    smudge = cat
	    clean = csvhooks --quote=all --lineterminator=unix reformat

Once the filter driver is defined, you can apply it to files of your
choice with lines in `.gitattributes` files, eg.

    *.csv	filter=csv

If you add a filter driver that changes the stored format of any files
already present in the repository, you should perform a

    git add --renormalize .

command to apply the formatting to the repo (those changes can then be
committed to the repository in the usual manner.)
