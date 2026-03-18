//! Integration tests for the XML splitter.
//!
//! Each test runs the splitter against `tests/fixtures/input.xml.gz` and
//! compares every output chunk byte-for-byte against the pre-generated golden
//! files stored under `tests/fixtures/chunk_N/`.

use anyhow::Result;
use xml_file_splitter::{splitter, writer};
mod common;

/// Core helper: run the splitter and compare every output against golden files.
fn run_and_compare(chunk_size: usize) -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();

    // Bind to a local so the PathBuf outlives the borrow in open_gz
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
    )?;

    // --- compare each chunk against its golden file ---
    let golden = common::golden_dir(chunk_size);

    for chunk_index in 1..=stats.chunks {
        let actual_path   = writer::chunk_path(&prefix, chunk_index);
        let golden_path   = writer::chunk_path(
            golden.join("chunk").to_str().unwrap(),
            chunk_index,
        );

        let actual = std::fs::read_to_string(&actual_path)
            .unwrap_or_else(|_| panic!("missing actual chunk {chunk_index}"));
        let expected = std::fs::read_to_string(&golden_path)
            .unwrap_or_else(|_| panic!(
                "missing golden file: {}",
                golden_path.display()
            ));

        let actual_events   = common::parse_events(&actual);
        let expected_events = common::parse_events(&expected);

        assert_eq!(
            actual_events, expected_events,
            "chunk {chunk_index} differs from golden file {} (structural comparison)",
            golden_path.display()
        );
    }

    // Verify no extra chunks were produced beyond what the golden dir contains.
    let extra_path = writer::chunk_path(&prefix, stats.chunks + 1);
    assert!(
        !extra_path.exists(),
        "unexpected extra chunk produced: {}",
        extra_path.display()
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_split_chunk_size_20() {
    run_and_compare(20).expect("splitter failed for chunk_size=20");
}

#[test]
fn test_split_chunk_size_5() {
    run_and_compare(5).expect("splitter failed for chunk_size=5");
}

#[test]
fn test_split_chunk_size_4() {
    run_and_compare(4).expect("splitter failed for chunk_size=4");
}
