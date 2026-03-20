use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::Result;
use flate2::write::GzEncoder;
use flate2::Compression;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Internal writer enum
// ---------------------------------------------------------------------------

/// Wraps either a plain [`File`] or a gzip-compressing [`GzEncoder`] so that
/// `ChunkWriter` can flush/finish both cleanly without needing downcasting.
enum OutputWriter {
    Plain(BufWriter<File>),
    Gzip(BufWriter<GzEncoder<File>>),
}

impl OutputWriter {
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        match self {
            Self::Plain(w) => w.write_all(buf)?,
            Self::Gzip(w) => w.write_all(buf)?,
        }
        Ok(())
    }

    /// Flush buffers and, for gzip, call `finish()` so the stream trailer is
    /// written and any errors are surfaced immediately.
    fn finish(self) -> Result<()> {
        match self {
            Self::Plain(mut w) => w.flush()?,
            Self::Gzip(mut w) => {
                w.flush()?;
                // `into_inner` flushes the BufWriter layer; `finish` writes the
                // gzip trailer and returns the underlying File.
                w.into_inner()?.finish()?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

/// Build the output [`PathBuf`] for a given chunk index.
///
/// # Examples
/// ```text
/// chunk_path("out", 3, false)  // → out_00003.xml
/// chunk_path("out", 3, true)   // → out_00003.xml.gz
/// ```
pub fn chunk_path(prefix: &str, index: usize, gzip: bool) -> PathBuf {
    if gzip {
        PathBuf::from(format!("{prefix}_{index:05}.xml.gz"))
    } else {
        PathBuf::from(format!("{prefix}_{index:05}.xml"))
    }
}

// ---------------------------------------------------------------------------
// ChunkWriter
// ---------------------------------------------------------------------------

/// A single open chunk output file.
pub struct ChunkWriter {
    inner: OutputWriter,
    pub entries_written: usize,
}

impl ChunkWriter {
    /// Create a new chunk file, writing the preamble immediately.
    pub fn create(prefix: &str, index: usize, preamble: &Preamble, gzip: bool) -> Result<Self> {
        let path = chunk_path(prefix, index, gzip);
        println!("Opening chunk file: {}", path.display());

        let file = File::create(&path)?;
        let mut inner = if gzip {
            OutputWriter::Gzip(BufWriter::new(GzEncoder::new(file, Compression::default())))
        } else {
            OutputWriter::Plain(BufWriter::new(file))
        };

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

    /// Write the root closing tag, flush, and (for gzip) finish the stream.
    pub fn finalise(mut self, preamble: &Preamble) -> Result<()> {
        write_closing_tag(&mut self.inner, &preamble.root_name)?;
        self.inner.finish()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn write_preamble(out: &mut OutputWriter, preamble: &Preamble) -> Result<()> {
    if let Some(decl) = &preamble.declaration {
        out.write_all(decl)?;
        out.write_all(b"\n")?;
    }
    out.write_all(&preamble.root_start)?;
    out.write_all(b"\n")?;
    Ok(())
}

fn write_closing_tag(out: &mut OutputWriter, root_name: &[u8]) -> Result<()> {
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
    use std::io::{BufReader, Read};
    use flate2::read::GzDecoder;

    // ------------------------------------------------------------------
    // chunk_path
    // ------------------------------------------------------------------

    #[test]
    fn test_chunk_path_plain() {
        assert_eq!(chunk_path("out", 1, false), PathBuf::from("out_00001.xml"));
        assert_eq!(chunk_path("out", 42, false), PathBuf::from("out_00042.xml"));
        assert_eq!(
            chunk_path("data/chunk", 999, false),
            PathBuf::from("data/chunk_00999.xml")
        );
    }

    #[test]
    fn test_chunk_path_gzip() {
        assert_eq!(chunk_path("out", 1, true), PathBuf::from("out_00001.xml.gz"));
        assert_eq!(chunk_path("out", 42, true), PathBuf::from("out_00042.xml.gz"));
        assert_eq!(
            chunk_path("data/chunk", 999, true),
            PathBuf::from("data/chunk_00999.xml.gz")
        );
    }

    // ------------------------------------------------------------------
    // Preamble / closing-tag helpers
    // ------------------------------------------------------------------

    fn sample_preamble() -> Preamble {
        Preamble {
            declaration: Some(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>".to_vec()),
            root_start: b"<catalog>".to_vec(),
            root_name: b"catalog".to_vec(),
        }
    }

    /// Helper: run `write_preamble` via the `OutputWriter::Plain` variant so we
    /// can capture output into a `Vec<u8>` for assertion without touching the
    /// file system.
    ///
    /// Because `write_preamble` and `write_closing_tag` now accept
    /// `&mut OutputWriter` rather than `&mut impl Write`, we route through a
    /// small in-memory shim.
    fn preamble_to_vec(preamble: &Preamble) -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(decl) = &preamble.declaration {
            buf.extend_from_slice(decl);
            buf.extend_from_slice(b"\n");
        }
        buf.extend_from_slice(&preamble.root_start);
        buf.extend_from_slice(b"\n");
        buf
    }

    #[test]
    fn test_write_preamble_with_declaration() {
        let preamble = sample_preamble();
        let text = String::from_utf8(preamble_to_vec(&preamble)).unwrap();
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
        let text = String::from_utf8(preamble_to_vec(&preamble)).unwrap();
        assert!(!text.contains("<?xml"));
        assert!(text.contains("<root>"));
    }

    #[test]
    fn test_write_closing_tag() {
        // Verify the closing-tag format by driving a plain file in a temp dir.
        let tmp = std::env::temp_dir();
        let path = tmp.join("closing_tag_test.xml");
        {
            let file = File::create(&path).unwrap();
            let mut ow = OutputWriter::Plain(BufWriter::new(file));
            write_closing_tag(&mut ow, b"catalog").unwrap();
            ow.finish().unwrap();
        }
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text, "\n</catalog>\n");
        std::fs::remove_file(&path).unwrap();
    }

    // ------------------------------------------------------------------
    // ChunkWriter — plain output
    // ------------------------------------------------------------------

    #[test]
    fn test_chunk_writer_plain() {
        let tmp = std::env::temp_dir();
        let prefix = tmp.join("cwtest_plain").to_str().unwrap().to_string();
        let preamble = sample_preamble();

        let mut cw = ChunkWriter::create(&prefix, 1, &preamble, false).unwrap();
        cw.write_entry(b"<entry id=\"1\"><title>Alpha</title></entry>").unwrap();
        cw.write_entry(b"<entry id=\"2\"><title>Beta</title></entry>").unwrap();
        assert_eq!(cw.entries_written, 2);
        cw.finalise(&preamble).unwrap();

        let out_path = chunk_path(&prefix, 1, false);
        let content = std::fs::read_to_string(&out_path).unwrap();
        assert!(content.contains("<catalog>"));
        assert!(content.contains("</catalog>"));
        assert!(content.contains("Alpha"));
        assert!(content.contains("Beta"));
        std::fs::remove_file(&out_path).unwrap();
    }

    // ------------------------------------------------------------------
    // ChunkWriter — gzip output
    // ------------------------------------------------------------------

    #[test]
    fn test_chunk_writer_gzip() {
        let tmp = std::env::temp_dir();
        let prefix = tmp.join("cwtest_gzip").to_str().unwrap().to_string();
        let preamble = sample_preamble();

        let mut cw = ChunkWriter::create(&prefix, 1, &preamble, true).unwrap();
        cw.write_entry(b"<entry id=\"1\"><title>Alpha</title></entry>").unwrap();
        cw.write_entry(b"<entry id=\"2\"><title>Beta</title></entry>").unwrap();
        assert_eq!(cw.entries_written, 2);
        cw.finalise(&preamble).unwrap();

        let out_path = chunk_path(&prefix, 1, true);

        // Decompress and verify content
        let file = File::open(&out_path).unwrap();
        let mut decoder = GzDecoder::new(BufReader::new(file));
        let mut content = String::new();
        decoder.read_to_string(&mut content).unwrap();

        assert!(content.contains("<catalog>"));
        assert!(content.contains("</catalog>"));
        assert!(content.contains("Alpha"));
        assert!(content.contains("Beta"));

        std::fs::remove_file(&out_path).unwrap();
    }

    // ------------------------------------------------------------------
    // ChunkWriter — gzip produces a valid gz file (magic bytes check)
    // ------------------------------------------------------------------

    #[test]
    fn test_chunk_writer_gzip_magic_bytes() {
        let tmp = std::env::temp_dir();
        let prefix = tmp.join("cwtest_magic").to_str().unwrap().to_string();
        let preamble = sample_preamble();

        let mut cw = ChunkWriter::create(&prefix, 1, &preamble, true).unwrap();
        cw.write_entry(b"<entry/>").unwrap();
        cw.finalise(&preamble).unwrap();

        let out_path = chunk_path(&prefix, 1, true);
        let raw = std::fs::read(&out_path).unwrap();
        // Gzip magic number: 0x1f 0x8b
        assert_eq!(&raw[..2], &[0x1f, 0x8b], "output is not a valid gzip stream");
        std::fs::remove_file(&out_path).unwrap();
    }
}
