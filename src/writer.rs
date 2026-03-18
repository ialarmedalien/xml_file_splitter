use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::Result;

/// Everything captured from the XML preamble (declaration + root open tag).
pub struct Preamble {
    /// Raw bytes of the XML declaration, e.g. `<?xml version="1.0"?>`
    pub declaration: Option<Vec<u8>>,
    /// The root element's opening tag, preserved exactly (with all attributes).
    pub root_start: Vec<u8>,
    /// Local name of the root element, needed to write the closing tag.
    pub root_name: Vec<u8>,
}

/// Stats returned after a completed split run.
pub struct SplitStats {
    pub total_entries: usize,
    pub chunks: usize,
}

/// Build the output [`PathBuf`] for a given chunk index.
///
/// # Example
/// `chunk_path("out", 3)` → `out_00003.xml`
pub fn chunk_path(prefix: &str, index: usize) -> PathBuf {
    PathBuf::from(format!("{prefix}_{index:05}.xml"))
}

/// A single open chunk output file.
pub struct ChunkWriter {
    inner: BufWriter<File>,
    pub entries_written: usize,
}

impl ChunkWriter {
    /// Create a new chunk file, writing the preamble immediately.
    pub fn create(prefix: &str, index: usize, preamble: &Preamble) -> Result<Self> {
        let path = chunk_path(prefix, index);
        println!("Opening chunk file: {}", path.display());
        let mut inner = BufWriter::new(File::create(path)?);
        write_preamble(&mut inner, preamble)?;
        Ok(Self { inner, entries_written: 0 })
    }

    /// Write a raw entry blob (the full `<entry>…</entry>` bytes) to the file.
    pub fn write_entry(&mut self, raw: &[u8]) -> Result<()> {
        self.inner.write_all(raw)?;
        self.inner.write_all(b"\n")?;
        self.entries_written += 1;
        Ok(())
    }

    /// Write the root closing tag and flush the file to disk.
    pub fn finalise(mut self, preamble: &Preamble) -> Result<()> {
        write_closing_tag(&mut self.inner, &preamble.root_name)?;
        self.inner.flush()?;
        Ok(())
    }
}

/// Write the preamble (declaration + root opening tag) to a [`Write`].
fn write_preamble(out: &mut impl Write, preamble: &Preamble) -> Result<()> {
    if let Some(decl) = &preamble.declaration {
        out.write_all(decl)?;
        out.write_all(b"\n")?;
    }
    out.write_all(&preamble.root_start)?;
    out.write_all(b"\n")?;
    Ok(())
}

/// Write a root closing tag to a [`Write`].
fn write_closing_tag(out: &mut impl Write, root_name: &[u8]) -> Result<()> {
    out.write_all(b"\n</")?;
    out.write_all(root_name)?;
    out.write_all(b">\n")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_path_formatting() {
        assert_eq!(chunk_path("out", 1), PathBuf::from("out_00001.xml"));
        assert_eq!(chunk_path("out", 42), PathBuf::from("out_00042.xml"));
        assert_eq!(chunk_path("data/chunk", 999), PathBuf::from("data/chunk_00999.xml"));
    }

    fn sample_preamble() -> Preamble {
        Preamble {
            declaration: Some(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>".to_vec()),
            root_start: b"<catalog>".to_vec(),
            root_name: b"catalog".to_vec(),
        }
    }

    #[test]
    fn test_write_preamble_with_declaration() {
        let preamble = sample_preamble();
        let mut out = Vec::new();
        write_preamble(&mut out, &preamble).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("<?xml"));
        assert!(text.contains("<catalog>"));
    }

    #[test]
    fn test_write_preamble_without_declaration() {
        let preamble = Preamble {
            declaration: None,
            root_start: b"<root>".to_vec(),
            root_name: b"root".to_vec(),
        };
        let mut out = Vec::new();
        write_preamble(&mut out, &preamble).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(!text.contains("<?xml"));
        assert!(text.contains("<root>"));
    }

    #[test]
    fn test_write_closing_tag() {
        let mut out = Vec::new();
        write_closing_tag(&mut out, b"catalog").unwrap();
        let text = String::from_utf8(out).unwrap();
        assert_eq!(text, "\n</catalog>\n");
    }

    #[test]
    fn test_chunk_writer_write_entry() {
        let tmp = std::env::temp_dir();
        let prefix = tmp.join("cwtest_chunk").to_str().unwrap().to_string();
        let preamble = sample_preamble();

        let mut cw = ChunkWriter::create(&prefix, 1, &preamble).unwrap();
        cw.write_entry(b"<entry id=\"1\"><title>Alpha</title></entry>").unwrap();
        cw.write_entry(b"<entry id=\"2\"><title>Beta</title></entry>").unwrap();
        assert_eq!(cw.entries_written, 2);
        cw.finalise(&preamble).unwrap();

        let content = std::fs::read_to_string(chunk_path(&prefix, 1)).unwrap();
        assert!(content.contains("<catalog>"));
        assert!(content.contains("</catalog>"));
        assert!(content.contains("Alpha"));
        assert!(content.contains("Beta"));
        std::fs::remove_file(chunk_path(&prefix, 1)).unwrap();
    }
}
