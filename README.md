# DataBucket

This is a library for writing and reading data in files.

## Command line tools

### `create-data-file`

Creates a data file with test data. The filename is provided using the `--filename` command line flag,
the number of pages to be written is provided using the `--pages-count` command line flag, and
the content of the pages is provided using the `--page-content` command line flag.

### `dump-data-file`

Loads the data from a file and prits it. The filename is provided using the `--filename` commnad line flag.
