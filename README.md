# xml_file_splitter

## Description

A simple XML file splitter, written in Rust.

The file splitter expects the input file to have multiple `element`s hanging off a root node, e.g.

```xml
<catalog>
  <record>...</record>
  <record>...</record>
  <record>...</record>
  <record>...</record>
</catalog>
```

### Run the splitter:

```sh
./xml_file_splitter \
  --input input.xml.gz \
  --element entry \
  --chunk-size 1000 \
  --output-prefix path/to/output/dir/chunk
```

The output will be saved in the directory `path/to/output/dir/` with file names of the form `chunk_00001.xml`, `chunk_00002.xml`, `chunk_00003.xml`, and so on.

Produce gzip-compressed output files:

```sh
./xml_file_splitter \
  --input input.xml.gz \
  --element entry \
  --chunk-size 1000 \
  --output-prefix path/to/output/dir/chunk
  --gzip
```

Output:
```sh
path/to/output/dir/chunk_00001.xml.gz
path/to/output/dir/chunk_00002.xml.gz
path/to/output/dir/chunk_00003.xml.gz
```

### Arguments:

- `input`: the input file; should be a gzipped XML file
- `element`, default `entry`: the XML tag name to collect for the output file
- `chunk-size`, default `100000`: number of elements per output file
- `output-prefix`, default `part`: path to the output file and the prefix to use for each output file. The prefix is appended with `_nnnnn.xml`, a zero-padded digit representing the number of the file in the sequence.
- `gzip`: whether or not the output files should be gzip compressed; include the parameter if the output files should be gzipped.


## Changelog

### v0.1.1

- Add `gzip` parameter to produce gzip-compressed output files.
