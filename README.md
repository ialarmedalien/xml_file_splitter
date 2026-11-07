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
- `validate`: whether or not to validate the output files; include the parameter to validate them.

### Testing

Run the unit and binary integration tests:

```sh
cargo test
```

Run the end-to-end Docker integration tests (builds `xml_file_splitter` from source and installs it into `ghcr.io/kbase/cdm-data-loaders:main` via the `cdm-integration-test` stage in `Dockerfile`):

```sh
cargo test --test integration_docker -- --ignored
```

To test against an already-built image tag, set `DOCKER_TEST_IMAGE`:

```sh
docker build --target cdm-integration-test -t xml-file-splitter-cdm-test:latest .
DOCKER_TEST_IMAGE=xml-file-splitter-cdm-test:latest cargo test --test integration_docker -- --ignored
```


## Changelog

### v0.1.4

- Add in output validation.

### v0.1.3

- Minor code change to emit a note to STDOUT when a file has finished writing.

### v0.1.2

- No code changes. Use [`dist`](https://axodotdev.github.io/cargo-dist/) for generating release binaries.

### v0.1.1

- Add `gzip` parameter to produce gzip-compressed output files.
