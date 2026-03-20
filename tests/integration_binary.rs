//! Integration tests for the XML splitter binary.
//!
//! Each test runs the splitter against `tests/fixtures/input.xml.gz` and
//! compares every output chunk byte-for-byte against the pre-generated golden
//! files stored under `tests/fixtures/chunk_N/`.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::process::Command;

use flate2::read::GzDecoder;
mod common;

fn binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_xml_file_splitter"))
}

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

/// Core helper: invoke the binary and compare every output chunk against the
/// plain-text golden files. When `gzip` is true the `--gzip` flag is passed
/// to the binary and each output is decompressed before comparison.
fn run_and_compare(chunk_size: usize, gzip: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();

    let mut cmd = Command::new(binary_path());
    cmd.arg("--input")
        .arg(common::input_path())
        .arg("--chunk-size")
        .arg(chunk_size.to_string())
        .arg("--element")
        .arg("entry")
        .arg("--output-prefix")
        .arg(&prefix);

    if gzip {
        cmd.arg("--gzip");
    }

    let status = cmd.status().expect("failed to launch xml_file_splitter");
    assert!(
        status.success(),
        "binary exited with non-zero status for chunk_size={chunk_size} gzip={gzip}"
    );

    let golden = common::golden_dir(chunk_size);
    let mut chunk_index = 1;
    loop {
        // Golden files are always plain XML regardless of the mode under test.
        let golden_path = xml_file_splitter::writer::chunk_path(
            golden.join("chunk").to_str().unwrap(),
            chunk_index,
            false,
        );
        let actual_path =
            xml_file_splitter::writer::chunk_path(&prefix, chunk_index, gzip);

        if !golden_path.exists() {
            // No more golden files — verify no extra chunks were produced either.
            assert!(
                !actual_path.exists(),
                "binary produced unexpected extra chunk {chunk_index} (gzip={gzip})"
            );
            break;
        }

        let actual = if gzip {
            read_gz(&actual_path)
        } else {
            read_plain(&actual_path)
        };
        let expected = std::fs::read_to_string(&golden_path)
            .unwrap_or_else(|_| panic!("missing golden file: {}", golden_path.display()));

        assert_eq!(
            common::parse_events(&actual),
            common::parse_events(&expected),
            "chunk {chunk_index} differs from golden file {} (structural comparison, gzip={gzip})",
            golden_path.display()
        );

        chunk_index += 1;
    }
}

/// Assert that a file starts with the gzip magic bytes (0x1f 0x8b).
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
// Plain-output tests
// ---------------------------------------------------------------------------

#[test]
fn test_binary_split_chunk_size_20() {
    run_and_compare(20, false);
}

#[test]
fn test_binary_split_chunk_size_5() {
    run_and_compare(5, false);
}

#[test]
fn test_binary_split_chunk_size_4() {
    run_and_compare(4, false);
}

// ---------------------------------------------------------------------------
// Gzip-output tests
// ---------------------------------------------------------------------------

#[test]
fn test_binary_split_gzip_chunk_size_20() {
    run_and_compare(20, true);
}

#[test]
fn test_binary_split_gzip_chunk_size_5() {
    run_and_compare(5, true);
}

#[test]
fn test_binary_split_gzip_chunk_size_4() {
    run_and_compare(4, true);
}

/// Output files carry the `.xml.gz` extension and are valid gzip streams.
#[test]
fn test_binary_gzip_output_files_are_valid_gz() {
    let tmp = tempfile::tempdir().unwrap();
    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();

    let status = Command::new(binary_path())
        .arg("--input")
        .arg(common::input_path())
        .arg("--chunk-size")
        .arg("20")
        .arg("--element")
        .arg("entry")
        .arg("--output-prefix")
        .arg(&prefix)
        .arg("--gzip")
        .status()
        .expect("failed to launch xml_file_splitter");

    assert!(status.success(), "binary exited with non-zero status");

    let golden = common::golden_dir(20);
    let mut chunk_index = 1;
    loop {
        let golden_path = xml_file_splitter::writer::chunk_path(
            golden.join("chunk").to_str().unwrap(),
            chunk_index,
            false,
        );
        if !golden_path.exists() {
            break;
        }

        let actual_path =
            xml_file_splitter::writer::chunk_path(&prefix, chunk_index, true);

        assert_eq!(
            actual_path.extension().and_then(|e| e.to_str()),
            Some("gz"),
            "chunk {chunk_index} does not have .gz extension"
        );
        assert_gzip_magic(&actual_path);

        chunk_index += 1;
    }
}

/// Plain-mode output files must NOT be gzip streams.
#[test]
fn test_binary_plain_output_files_are_not_gz() {
    let tmp = tempfile::tempdir().unwrap();
    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();

    let status = Command::new(binary_path())
        .arg("--input")
        .arg(common::input_path())
        .arg("--chunk-size")
        .arg("20")
        .arg("--element")
        .arg("entry")
        .arg("--output-prefix")
        .arg(&prefix)
        .status()
        .expect("failed to launch xml_file_splitter");

    assert!(status.success(), "binary exited with non-zero status");

    let golden = common::golden_dir(20);
    let mut chunk_index = 1;
    loop {
        let golden_path = xml_file_splitter::writer::chunk_path(
            golden.join("chunk").to_str().unwrap(),
            chunk_index,
            false,
        );
        if !golden_path.exists() {
            break;
        }

        let actual_path =
            xml_file_splitter::writer::chunk_path(&prefix, chunk_index, false);

        assert_eq!(
            actual_path.extension().and_then(|e| e.to_str()),
            Some("xml"),
            "chunk {chunk_index} should have .xml extension"
        );

        let raw = std::fs::read(&actual_path).unwrap();
        let is_gz = raw.len() >= 2 && raw[0] == 0x1f && raw[1] == 0x8b;
        assert!(!is_gz, "plain chunk {chunk_index} looks like a gzip stream");

        chunk_index += 1;
    }
}
