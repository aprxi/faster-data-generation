use std::env;
use std::time::Instant;
use std::path::PathBuf;

use clap::{CommandFactory, Parser};

mod generate;
mod extract;

use generate::generate_from_json;
use extract::write_dataframe_to_parquet;

const SCHEMA_FILE: &str = "schema.json";
const DEFAULT_NO_ROWS: usize = 10000;
const RAYON_NUM_THREADS: usize = 1;

/// rsfake - Generate fake data
///
/// Example:
/// rsfake -s schema.json -r 10000 -t 1
#[derive(Parser, Debug)]
#[command(author, version)]
struct Args {
    /// Number of rows to generate
    #[arg(short, long, value_name = "NO_ROWS", default_value_t = DEFAULT_NO_ROWS)]
    rows: usize,

    /// Number of threads to use
    #[arg(short, long, value_name = "NO_THREADS", default_value_t = RAYON_NUM_THREADS, env = "RAYON_NUM_THREADS")]
    threads: usize,

    /// JSON file to describe column names and types
    #[arg(short, long, default_value = SCHEMA_FILE)]
    schema: PathBuf,
}

fn main() {
    let args = Args::parse();

    // check if schema file exists
    if !args.schema.exists() {
        println!("Schema file {} does not exist", args.schema.display());
        let _ = Args::command().print_help();
        std::process::exit(1);
    }

    // set RAYON_NUM_THREADS in env
    env::set_var("RAYON_NUM_THREADS", args.threads.to_string());

    let start_time = Instant::now();
    let mut df = generate_from_json(&args.schema, args.rows).unwrap();
    let elapsed = start_time.elapsed().as_secs_f64();
    println!("{:?}", df);
    println!("Time taken to generate {} people into a dataframe:", args.rows);
    println!("--- {:.3} seconds ---", elapsed);

    // write to Parquet
    let parquet_file = "people.parquet";
    let start_time = Instant::now();
    write_dataframe_to_parquet(&mut df, parquet_file).unwrap();
    let elapsed = start_time.elapsed().as_secs_f64();
    println!("Time taken to write to Parquet:");
    println!("--- {:.3} seconds ---", elapsed);
}
