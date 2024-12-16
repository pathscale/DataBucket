# DataBucket

This is a library for writing and reading data files.

## Command line tools

The command line tools reside in the `tools` directory.

### `create-data-file`

Creates a data file with test data. The filename is provided using the `--filename` command line flag,
the number of pages to be written is provided using the `--pages-count` command line flag, and
the content of the pages is provided using the `--page-content` command line flag.

### `dump-data-file`

Loads the data from a file and prints it. The filename is provided using the `--filename` command line flag.
