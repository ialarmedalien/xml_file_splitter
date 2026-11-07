// tests/common/mod.rs
#![allow(dead_code)]

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;


/// path to the fixtures directory for the integration tests
pub fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// path to the input file for the integration tests
pub fn input_path() -> PathBuf {
    fixtures_dir().join("input.xml.gz")
}

/// path to the expected output file(s) for the integration tests
pub fn golden_dir(chunk_size: usize) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("tests/fixtures/chunk_{chunk_size}"))
}

/// Parse an XML string into a canonical sequence of events, discarding
/// whitespace-only text nodes (which are just formatting artefacts).
pub fn parse_events(xml: &str) -> Vec<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut buf = Vec::new();
    let mut events = Vec::new();

    loop {
        buf.clear();
        match reader.read_event_into(&mut buf).unwrap() {
            Event::Eof => break,
            Event::Text(e) => {
                let text = std::str::from_utf8(&e).unwrap();
                if !text.trim().is_empty() {
                    events.push(format!("Text({text})"));
                }
                // whitespace-only text nodes are dropped
            }
            Event::Start(e) => {
                // Canonicalise attributes by sorting them
                let name = std::str::from_utf8(e.name().as_ref()).unwrap().to_string();
                let mut attrs: Vec<String> = e.attributes()
                    .map(|a| {
                        let a = a.unwrap();
                        let key = std::str::from_utf8(a.key.as_ref()).unwrap().to_string();
                        let val = std::str::from_utf8(&a.value).unwrap().to_string();
                        format!("{key}={val}")
                    })
                    .collect();
                attrs.sort();
                events.push(format!("Start({name} [{attrs}])", attrs = attrs.join(",")));
            }
            Event::End(e) => {
                let name = std::str::from_utf8(e.name().as_ref()).unwrap().to_string();
                events.push(format!("End({name})"));
            }
            Event::Empty(e) => {
                let name = std::str::from_utf8(e.name().as_ref()).unwrap().to_string();
                let mut attrs: Vec<String> = e.attributes()
                    .map(|a| {
                        let a = a.unwrap();
                        let key = std::str::from_utf8(a.key.as_ref()).unwrap().to_string();
                        let val = std::str::from_utf8(&a.value).unwrap().to_string();
                        format!("{key}={val}")
                    })
                    .collect();
                attrs.sort();
                events.push(format!("Empty({name} [{attrs}])", attrs = attrs.join(",")));
            }
            Event::CData(e) => events.push(format!("CData({e:?})")),
            Event::Comment(e) => events.push(format!("Comment({e:?})")),
            Event::Decl(_) => {} // ignore XML declaration differences
            _ => {}
        }
    }
    events
}

/// Read a plain-text chunk file into a String.
pub fn read_plain(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("missing actual chunk: {}", path.display()))
}

/// Read and decompress a gzipped chunk file into a String.
pub fn read_gz(path: &Path) -> String {
    let file = File::open(path)
        .unwrap_or_else(|_| panic!("missing actual gzip chunk: {}", path.display()));
    let mut decoder = GzDecoder::new(BufReader::new(file));
    let mut content = String::new();
    decoder
        .read_to_string(&mut content)
        .unwrap_or_else(|_| panic!("failed to decompress chunk: {}", path.display()));
    content
}

/// Assert that a file starts with the gzip magic bytes (0x1f 0x8b).
pub fn assert_gzip_magic(path: &Path) {
    let raw = std::fs::read(path)
        .unwrap_or_else(|_| panic!("cannot read file for magic check: {}", path.display()));
    assert!(
        raw.len() >= 2 && raw[0] == 0x1f && raw[1] == 0x8b,
        "file does not have gzip magic bytes: {}",
        path.display()
    );
}

/// Compare every output chunk under `prefix` against the plain-text golden
/// files for `chunk_size`. When `gzip` is true each actual chunk is
/// decompressed before comparison.
pub fn compare_chunks_against_golden(prefix: &str, chunk_size: usize, gzip: bool) {
    let golden = golden_dir(chunk_size);
    let mut chunk_index = 1;
    loop {
        // Golden files are always plain XML regardless of the mode under test.
        let golden_path = xml_file_splitter::writer::chunk_path(
            golden.join("chunk").to_str().unwrap(),
            chunk_index,
            false,
        );
        let actual_path = xml_file_splitter::writer::chunk_path(prefix, chunk_index, gzip);

        if !golden_path.exists() {
            // No more golden files — verify no extra chunks were produced either.
            assert!(
                !actual_path.exists(),
                "splitter produced unexpected extra chunk {chunk_index} (gzip={gzip})"
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
            parse_events(&actual),
            parse_events(&expected),
            "chunk {chunk_index} differs from golden file {} (structural comparison, gzip={gzip})",
            golden_path.display()
        );

        chunk_index += 1;
    }
}

/// Verify that all expected chunks for `chunk_size` carry the `.gz` extension
/// and start with the gzip magic bytes.
pub fn assert_gzip_chunks_valid(prefix: &str, chunk_size: usize) {
    let golden = golden_dir(chunk_size);
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

        let actual_path = xml_file_splitter::writer::chunk_path(prefix, chunk_index, true);

        assert_eq!(
            actual_path.extension().and_then(|e| e.to_str()),
            Some("gz"),
            "chunk {chunk_index} does not have .gz extension"
        );
        assert_gzip_magic(&actual_path);

        chunk_index += 1;
    }
}

/// Verify that all expected chunks for `chunk_size` carry the `.xml` extension
/// and do not start with the gzip magic bytes.
pub fn assert_plain_chunks_not_gz(prefix: &str, chunk_size: usize) {
    let golden = golden_dir(chunk_size);
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

        let actual_path = xml_file_splitter::writer::chunk_path(prefix, chunk_index, false);

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
