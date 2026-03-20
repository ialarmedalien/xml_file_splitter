//! Integration tests for the XML splitter.
//!
//! Each test runs the splitter against `tests/fixtures/input.xml.gz` and
//! compares every output chunk byte-for-byte against the pre-generated golden
//! files stored under `tests/fixtures/chunk_N/`.

use std::fs::File;
use std::io::{BufReader, Read};

use anyhow::Result;
use flate2::read::GzDecoder;
use xml_file_splitter::{splitter, writer};
mod common;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read a plain-text chunk file into a String.
fn read_plain(path: &std::path::Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("missing actual chunk: {}", path.display()))
}

/// Read and decompress a gzipped chunk file into a String.
fn read_gz(path: &std::path::Path) -> String {
    let file = File::open(path)
        .unwrap_or_else(|_| panic!("missing actual gzip chunk: {}", path.display()));
    let mut decoder = GzDecoder::new(BufReader::new(file));
    let mut content = String::new();
    decoder
        .read_to_string(&mut content)
        .unwrap_or_else(|_| panic!("failed to decompress chunk: {}", path.display()));
    content
}

/// Core helper: run the splitter and compare every output against golden files.
///
/// When `gzip` is true the output chunks are decompressed before comparison,
/// so the same plain-text golden files are reused for both modes.
fn run_and_compare(chunk_size: usize, gzip: bool) -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();

    let input = common::input_path();
    let gz = splitter::open_gz(&input)?;
    let mut reader = quick_xml::reader::Reader::from_reader(gz);
    reader.config_mut().trim_text(false);

    let preamble = splitter::read_preamble(&mut reader)?;
    let stats = splitter::split(
        &mut reader,
        &preamble,
        b"entry",
        chunk_size,
        &prefix,
        gzip,           // ← new parameter
    )?;

    let golden = common::golden_dir(chunk_size);

    for chunk_index in 1..=stats.chunks {
        let actual_path = writer::chunk_path(&prefix, chunk_index, gzip);
        let golden_path = writer::chunk_path(
            golden.join("chunk").to_str().unwrap(),
            chunk_index,
            false,          // golden files are always plain XML
        );

        // Decompress actual output when in gzip mode; golden is always plain.
        let actual = if gzip {
            read_gz(&actual_path)
        } else {
            read_plain(&actual_path)
        };
        let expected = std::fs::read_to_string(&golden_path)
            .unwrap_or_else(|_| panic!("missing golden file: {}", golden_path.display()));

        let actual_events   = common::parse_events(&actual);
        let expected_events = common::parse_events(&expected);

        assert_eq!(
            actual_events, expected_events,
            "chunk {chunk_index} differs from golden file {} (structural comparison)",
            golden_path.display()
        );
    }

    // Verify no extra chunks were produced beyond what the golden dir contains.
    let extra_path = writer::chunk_path(&prefix, stats.chunks + 1, gzip);
    assert!(
        !extra_path.exists(),
        "unexpected extra chunk produced: {}",
        extra_path.display()
    );

    Ok(())
}

/// Assert that a gzip chunk file starts with the gzip magic bytes (0x1f 0x8b).
fn assert_gzip_magic(path: &std::path::Path) {
    let raw = std::fs::read(path)
        .unwrap_or_else(|_| panic!("cannot read file for magic check: {}", path.display()));
    assert!(
        raw.len() >= 2 && raw[0] == 0x1f && raw[1] == 0x8b,
        "file does not have gzip magic bytes: {}",
        path.display()
    );
}

// ---------------------------------------------------------------------------
// Plain-output tests (existing, updated call sites only)
// ---------------------------------------------------------------------------

#[test]
fn test_split_chunk_size_20() {
    run_and_compare(20, false).expect("splitter failed for chunk_size=20");
}

#[test]
fn test_split_chunk_size_5() {
    run_and_compare(5, false).expect("splitter failed for chunk_size=5");
}

#[test]
fn test_split_chunk_size_4() {
    run_and_compare(4, false).expect("splitter failed for chunk_size=4");
}

// ---------------------------------------------------------------------------
// Gzip-output tests
// ---------------------------------------------------------------------------

/// Structural content of every gzip chunk matches the plain golden files.
#[test]
fn test_split_gzip_chunk_size_20() {
    run_and_compare(20, true).expect("gzip splitter failed for chunk_size=20");
}

#[test]
fn test_split_gzip_chunk_size_5() {
    run_and_compare(5, true).expect("gzip splitter failed for chunk_size=5");
}

#[test]
fn test_split_gzip_chunk_size_4() {
    run_and_compare(4, true).expect("gzip splitter failed for chunk_size=4");
}

/// Output files carry the `.xml.gz` extension and are valid gzip streams.
#[test]
fn test_split_gzip_output_files_are_valid_gz() {
    let tmp = tempfile::tempdir().unwrap();
    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();

    let input = common::input_path();
    let gz = splitter::open_gz(&input).unwrap();
    let mut reader = quick_xml::reader::Reader::from_reader(gz);
    reader.config_mut().trim_text(false);

    let preamble = splitter::read_preamble(&mut reader).unwrap();
    let stats = splitter::split(&mut reader, &preamble, b"entry", 20, &prefix, true).unwrap();

    for chunk_index in 1..=stats.chunks {
        let path = writer::chunk_path(&prefix, chunk_index, true);

        // Extension must be .xml.gz
        assert_eq!(
            path.extension().and_then(|e| e.to_str()),
            Some("gz"),
            "chunk {chunk_index} does not have .gz extension"
        );

        // File must be a valid gzip stream.
        assert_gzip_magic(&path);
    }
}

/// Plain-mode output files must NOT be gzip streams.
#[test]
fn test_split_plain_output_files_are_not_gz() {
    let tmp = tempfile::tempdir().unwrap();
    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();

    let input = common::input_path();
    let gz = splitter::open_gz(&input).unwrap();
    let mut reader = quick_xml::reader::Reader::from_reader(gz);
    reader.config_mut().trim_text(false);

    let preamble = splitter::read_preamble(&mut reader).unwrap();
    let stats = splitter::split(&mut reader, &preamble, b"entry", 20, &prefix, false).unwrap();

    for chunk_index in 1..=stats.chunks {
        let path = writer::chunk_path(&prefix, chunk_index, false);

        assert_eq!(
            path.extension().and_then(|e| e.to_str()),
            Some("xml"),
            "chunk {chunk_index} should have .xml extension"
        );

        let raw = std::fs::read(&path).unwrap();
        let is_gz = raw.len() >= 2 && raw[0] == 0x1f && raw[1] == 0x8b;
        assert!(!is_gz, "plain chunk {chunk_index} looks like a gzip stream");
    }
}
