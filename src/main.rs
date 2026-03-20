mod cli;
mod splitter;
mod writer;

use anyhow::Result;
use clap::Parser;

fn main() -> Result<()> {
    let args = cli::Args::parse();

    println!("Input      : {}", args.input.display());
    println!("Chunk size : {} <{}> elements", args.chunk_size, args.element);
    if args.gzip {
        println!("Output     : gzip-compressed");
    }

    let gz = splitter::open_gz(&args.input)?;
    let mut reader = quick_xml::reader::Reader::from_reader(gz);
    reader.config_mut().trim_text(false);

    let preamble = splitter::read_preamble(&mut reader)?;

    let stats = splitter::split(
        &mut reader,
        &preamble,
        args.element.as_bytes(),
        args.chunk_size,
        &args.output_prefix,
        args.gzip,
    )?;

    println!(
        "Done. {} entries written across {} chunk file(s).",
        stats.total_entries, stats.chunks
    );

    Ok(())
}
