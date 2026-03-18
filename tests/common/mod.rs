// tests/common/mod.rs
use std::path::{Path, PathBuf};

use quick_xml::events::Event;
use quick_xml::reader::Reader;


/// path to the input file for the integration tests
pub fn input_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/input.xml.gz")
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
