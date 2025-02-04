# DataBucket

This is a library for writing and reading data files.

## Command line tools

The command line tools reside in the `tools` directory.

### `create-data-file`

Creates a data file with test data. The filename is provided using the `--filename` command line flag,
the number of pages to be written is provided using the `--count` command line flag which sets amount of data records.

### `dump-data-file`

Loads the data from a file and prints it. The filename is provided using the `--filename` command line flag.


### Example of generated file after dump
```
--count 10

+-----+----------+
| val | attr     |
+-----+----------+
| 0   | string 0 |
| 1   | string 1 |
| 2   | string 2 |
| 3   | string 3 |
| 4   | string 4 |
| 5   | string 5 |
| 6   | string 6 |
| 7   | string 7 |
| 8   | string 8 |
| 9   | string 9 |
+-----+----------+
```
