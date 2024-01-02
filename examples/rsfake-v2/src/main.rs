use std::env;
use std::time::Instant;

use clap::{value_parser, Arg, Command};

mod generate;
mod extract;

use generate::generate_from_json;
use extract::write_dataframe_to_parquet;

const PROGRAM_NAME: &str = "rsfake";
const SCHEMA_FILE: &str = "schema.json";
const DEFAULT_NO_ROWS: usize = 10000;
const RAYON_NUM_THREADS: usize = 1;

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut app = Command::new(PROGRAM_NAME)
        .version(env!("CARGO_PKG_VERSION"))
        .arg_required_else_help(false)
        .about(format!(
            "{} - Generate fake data\n\nExample:\n {} -s {} -r {} -t {}",
            PROGRAM_NAME, PROGRAM_NAME,
            SCHEMA_FILE, DEFAULT_NO_ROWS, RAYON_NUM_THREADS
        ))
        .arg(
            Arg::new("rows")
                .value_parser(value_parser!(usize))
                .short('r')
                .long("rows")
                .value_name("NO_ROWS")
                .help("Number of rows to generate"),
        )
        .arg(
            Arg::new("threads")
                .value_parser(value_parser!(usize))
                .short('t')
                .long("threads")
                .value_name("NO_THREADS")
                .help("Number of threads to use"),
        )
        .arg(
            Arg::new("schema")
                .short('s')
                .long("schema")
                .help("JSON file to describe column names and types")
        );

    let matches = app.clone().try_get_matches_from(args).unwrap_or_else(|e| {
        e.exit();
    });


    let schema_file = matches
        .get_one::<String>("schema")
        .cloned()
        .unwrap_or(SCHEMA_FILE.to_string());

    // check if schema file exists
    if !std::path::Path::new(&schema_file).exists() {
        println!("Schema file {} does not exist", schema_file);
        let _ = app.print_help().unwrap();
        std::process::exit(1);
    }

    // get RAYON_NUM_THREADS from env
    // if not set, use RAYON_NUM_THREADS constant
    let no_threads = env::var("RAYON_NUM_THREADS")
        .unwrap_or_else(|_| RAYON_NUM_THREADS.to_string())
        .parse::<usize>()
        .unwrap();

    let no_rows = matches
        .get_one::<usize>("rows")
        .cloned()
        .unwrap_or(DEFAULT_NO_ROWS);

    // override no_threads if defined in command line
    let no_threads = matches
        .get_one::<usize>("threads")
        .cloned()
        .unwrap_or(no_threads);
    // set RAYON_NUM_THREADS in env
    env::set_var("RAYON_NUM_THREADS", no_threads.to_string());

    let start_time = Instant::now();
    let mut df = generate_from_json(SCHEMA_FILE, no_rows).unwrap();
    let elapsed = start_time.elapsed().as_secs_f64();
    println!("{:?}", df);
    println!("Time taken to generate {no_rows} people into a dataframe:");
    println!("--- {:.3} seconds ---", elapsed);

    // write to Parquet
    let parquet_file = "people.parquet";
    let start_time = Instant::now();
    write_dataframe_to_parquet(&mut df, parquet_file).unwrap();
    let elapsed = start_time.elapsed().as_secs_f64();
    println!("Time taken to write to Parquet:");
    println!("--- {:.3} seconds ---", elapsed);
}
