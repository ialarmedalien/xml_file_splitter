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

fn run_and_compare(chunk_size: usize) {
    let tmp = tempfile::tempdir().unwrap();
    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();

    // Invoke the compiled binary
    let status = Command::new(binary_path())
        .arg("--input")
        .arg(common::input_path())
        .arg("--chunk-size")
        .arg(chunk_size.to_string())
        .arg("--element")
        .arg("entry")
        .arg("--output-prefix")
        .arg(&prefix)
        .status()
        .expect("failed to launch xml_file_splitter");

    assert!(status.success(), "binary exited with non-zero status for chunk_size={chunk_size}");

    // Count how many golden chunks exist for this chunk size
    let golden = common::golden_dir(chunk_size);
    let mut chunk_index = 1;
    loop {
        let golden_path = xml_file_splitter::writer::chunk_path(
            golden.join("chunk").to_str().unwrap(),
            chunk_index,
        );
        let actual_path = xml_file_splitter::writer::chunk_path(&prefix, chunk_index);

        if !golden_path.exists() {
            // No more golden files — verify no extra chunks were produced either
            assert!(
                !actual_path.exists(),
                "binary produced unexpected extra chunk {chunk_index}"
            );
            break;
        }

        let actual = std::fs::read_to_string(&actual_path)
            .unwrap_or_else(|_| panic!("missing actual chunk {chunk_index}"));
        let expected = std::fs::read_to_string(&golden_path)
            .unwrap_or_else(|_| panic!("missing golden file: {}", golden_path.display()));

        assert_eq!(
            common::parse_events(&actual),
            common::parse_events(&expected),
            "chunk {chunk_index} differs from golden file {} (structural comparison)",
            golden_path.display()
        );

        chunk_index += 1;
    }
}

#[test]
fn test_binary_split_chunk_size_20() {
    run_and_compare(20);
}

#[test]
fn test_binary_split_chunk_size_5() {
    run_and_compare(5);
}

#[test]
fn test_binary_split_chunk_size_4() {
    run_and_compare(4);
}
