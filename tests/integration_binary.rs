//! Integration tests for the XML splitter binary.
//!
//! Each test runs the splitter against `tests/fixtures/input.xml.gz` and
//! compares every output chunk byte-for-byte against the pre-generated golden
//! files stored under `tests/fixtures/chunk_N/`.

use std::path::PathBuf;
use std::process::Command;

mod common;

fn binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_xml_file_splitter"))
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

    common::compare_chunks_against_golden(&prefix, chunk_size, gzip);
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

    common::assert_gzip_chunks_valid(&prefix, 20);
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

    common::assert_plain_chunks_not_gz(&prefix, 20);
}
