use std::io::BufRead;

use anyhow::Result;
use quick_xml::events::Event;
use quick_xml::reader::Reader;

use crate::writer::{ChunkWriter, Preamble, SplitStats};

/// Open a gzip-compressed file and return a buffered reader over it.
pub fn open_gz(path: &std::path::PathBuf) -> Result<impl BufRead> {
    use flate2::read::GzDecoder;
    use std::{fs::File, io::BufReader};
    Ok(BufReader::new(GzDecoder::new(File::open(path)?)))
}

/// Read events until the root element's opening tag is found, capturing the
/// XML declaration (if present) and the root start tag verbatim.
///
/// The reader is left positioned immediately after the root start tag, ready
/// to yield the first child element.
pub fn read_preamble<R: BufRead>(reader: &mut Reader<R>) -> Result<Preamble> {
    let mut buf = Vec::new();
    let mut declaration: Option<Vec<u8>> = None;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Decl(e) => {
                let mut raw = b"<?".to_vec();
                raw.extend_from_slice(&e);
                raw.extend_from_slice(b"?>");
                declaration = Some(raw);
                buf.clear();
            }
            Event::Start(e) => {
                let root_name = e.name().as_ref().to_vec();
                let mut root_start = b"<".to_vec();
                root_start.extend_from_slice(&e);
                root_start.push(b'>');

                return Ok(Preamble {
                    declaration,
                    root_start,
                    root_name,
                });
            }
            Event::Eof => anyhow::bail!("Unexpected EOF before root element"),
            _ => {
                buf.clear();
            }
        }
    }
}


/// Read the bytes of one complete entry element from the reader.
///
/// The caller has already consumed the opening `<entry …>` tag event;
/// `start_tag_bytes` are the raw bytes of that tag (without `<` or `>`).
/// `read_to_end_into` is used so the interior of the entry is never parsed —
/// the raw bytes are accumulated directly into the return buffer.
///
/// Returns the full `<entry …>…</entry>` bytes, or `None` on EOF.
pub fn read_raw_entry<R: BufRead>(
    reader: &mut Reader<R>,
    start_tag_bytes: &[u8],
) -> Result<Vec<u8>> {
    // Start building with the opening tag
    let mut raw = b"<".to_vec();
    raw.extend_from_slice(start_tag_bytes);
    raw.push(b'>');

    let mut buf = Vec::new();
    let mut depth = 1usize;

    loop {
        buf.clear();
        match reader.read_event_into(&mut buf)? {
            Event::Start(ref e) => {
                depth += 1;
                raw.push(b'<');
                raw.extend_from_slice(e);
                raw.push(b'>');
            }
            Event::End(ref e) => {
                depth -= 1;
                raw.extend_from_slice(b"</");
                raw.extend_from_slice(e.name().as_ref());
                raw.push(b'>');
                if depth == 0 {
                    break;
                }
            }
            Event::Empty(ref e) => {
                raw.push(b'<');
                raw.extend_from_slice(e);
                raw.extend_from_slice(b"/>");
            }
            Event::Text(ref e) => {
                raw.extend_from_slice(e);
            }
            Event::CData(ref e) => {
                raw.extend_from_slice(b"<![CDATA[");
                raw.extend_from_slice(e);
                raw.extend_from_slice(b"]]>");
            }
            Event::Comment(ref e) => {
                raw.extend_from_slice(b"<!--");
                raw.extend_from_slice(e);
                raw.extend_from_slice(b"-->");
            }
            Event::PI(ref e) => {
                raw.extend_from_slice(b"<?");
                raw.extend_from_slice(e);
                raw.extend_from_slice(b"?>");
            }
            Event::Eof => anyhow::bail!("Unexpected EOF inside entry element"),
            _ => {}
        }
    }

    Ok(raw)
}

// pub fn read_raw_entry<R: BufRead>(
//     reader: &mut Reader<R>,
//     start_tag_bytes: &[u8],
//     entry_tag: &[u8],
// ) -> Result<Vec<u8>> {
//     let mut interior = Vec::new(); // fresh buffer — no stale bytes
//     reader.read_to_end_into(quick_xml::name::QName(entry_tag), &mut interior)?;

//     let mut raw = b"<".to_vec();
//     raw.extend_from_slice(start_tag_bytes);
//     raw.push(b'>');
//     raw.extend_from_slice(&interior);
//     Ok(raw)
// }

/// Drive the split: distribute all entry elements across chunk files of at
/// most `chunk_size` entries each.
pub fn split<R: BufRead>(
    reader: &mut Reader<R>,
    preamble: &Preamble,
    entry_tag: &[u8],
    chunk_size: usize,
    output_prefix: &str,
) -> Result<SplitStats> {
    let mut chunk_index = 1usize;
    let mut current = ChunkWriter::create(output_prefix, chunk_index, preamble)?;
    let mut total_entries = 0usize;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == entry_tag => {
                let raw = read_raw_entry(reader, &e.to_owned())?;

                if current.entries_written == chunk_size {
                    // Finalise the full chunk and open the next one.
                    current.finalise(preamble)?;
                    chunk_index += 1;
                    current = ChunkWriter::create(output_prefix, chunk_index, preamble)?;
                }

                current.write_entry(&raw)?;
                total_entries += 1;
            }

            // Whitespace / comments between entries at the root level.
            Event::Text(_) | Event::Comment(_) => {}

            // Closing root tag or end of file — we're done.
            Event::End(_) | Event::Eof => break,

            _ => {}
        }
        buf.clear();
    }

    current.finalise(preamble)?;

    Ok(SplitStats {
        total_entries,
        chunks: chunk_index,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use quick_xml::reader::Reader;

    const SAMPLE_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<catalog>
  <entry id="1"><title>Alpha</title></entry>
  <entry id="2"><title>Beta</title></entry>
  <entry id="3"><title>Gamma</title></entry>
  <entry id="4"><title>Delta</title></entry>
  <entry id="5"><title>Epsilon</title></entry>
</catalog>"#;

    fn make_reader(xml: &str) -> Reader<&[u8]> {
        let mut r = Reader::from_str(xml);
        r.config_mut().trim_text(false);
        r
    }

    #[test]
    fn test_read_preamble_captures_declaration_and_root() {
        let mut reader = make_reader(SAMPLE_XML);
        let preamble = read_preamble(&mut reader).unwrap();

        let decl = String::from_utf8(preamble.declaration.unwrap()).unwrap();
        assert!(decl.contains("xml version"));
        assert_eq!(preamble.root_name, b"catalog");
        let root = String::from_utf8(preamble.root_start).unwrap();
        assert!(root.starts_with("<catalog"));
    }

    #[test]
    fn test_read_preamble_no_declaration() {
        let mut reader = make_reader("<root><entry/></root>");
        let preamble = read_preamble(&mut reader).unwrap();
        assert!(preamble.declaration.is_none());
        assert_eq!(preamble.root_name, b"root");
    }

    #[test]
    fn test_read_preamble_eof_error() {
        let mut reader = make_reader("<?xml version=\"1.0\"?>");
        assert!(read_preamble(&mut reader).is_err());
    }

    #[test]
    fn test_read_raw_entry() {
        let mut reader = make_reader(SAMPLE_XML);
        read_preamble(&mut reader).unwrap();

        // Advance to the first <entry> start event.
        let mut buf = Vec::new();
        let start_bytes = loop {
            match reader.read_event_into(&mut buf).unwrap() {
                Event::Start(e) if e.name().as_ref() == b"entry" => {
                    break e.to_owned()
                }
                Event::Eof => panic!("no entry found"),
                _ => buf.clear(),
            }
        };

        let raw = read_raw_entry(&mut reader, &start_bytes).unwrap();
        let text = String::from_utf8(raw).unwrap();
        assert!(text.starts_with(r#"<entry id="1">"#));
        assert!(text.contains("<title>Alpha</title>"));
        assert!(text.ends_with("</entry>"));
    }

    #[test]
    fn test_split_counts_entries_and_chunks() {
        let tmp = std::env::temp_dir();
        let prefix = tmp.join("splitter_test_chunk").to_str().unwrap().to_string();

        let mut reader = make_reader(SAMPLE_XML);
        let preamble = read_preamble(&mut reader).unwrap();
        let stats = split(&mut reader, &preamble, b"entry", 2, &prefix).unwrap();

        assert_eq!(stats.total_entries, 5);
        assert_eq!(stats.chunks, 3); // ceil(5/2) = 3

        use crate::writer::chunk_path;
        for (i, expected) in [(1, 2), (2, 2), (3, 1)] {
            let path = chunk_path(&prefix, i);
            let content = std::fs::read_to_string(&path).unwrap();
            assert_eq!(content.matches("<entry").count(), expected);
            assert!(content.contains("<catalog"));
            assert!(content.contains("</catalog>"));
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn test_split_single_chunk_when_few_entries() {
        let tmp = std::env::temp_dir();
        let prefix = tmp.join("splitter_test_single").to_str().unwrap().to_string();

        let mut reader = make_reader(SAMPLE_XML);
        let preamble = read_preamble(&mut reader).unwrap();
        let stats = split(&mut reader, &preamble, b"entry", 100, &prefix).unwrap();

        assert_eq!(stats.total_entries, 5);
        assert_eq!(stats.chunks, 1);

        use crate::writer::chunk_path;
        std::fs::remove_file(chunk_path(&prefix, 1)).unwrap();
    }
}
