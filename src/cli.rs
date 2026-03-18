use std::path::PathBuf;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(about = "Split a gzipped XML file into chunks")]
pub struct Args {
    /// Input gzipped XML file
    #[arg(short, long)]
    pub input: PathBuf,

    /// Output file prefix
    /// e.g. "out" => out_00001.xml, out_00002.xml, etc.
    #[arg(short, long, default_value = "part")]
    pub output_prefix: String,

    /// Number of entry elements per output file
    #[arg(short = 'n', long, default_value_t = 100000)]
    pub chunk_size: usize,

    /// XML element tag name to split on
    #[arg(short, long, default_value = "entry")]
    pub element: String,
}
