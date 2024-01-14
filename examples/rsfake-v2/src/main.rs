use std::env;
use std::time::Instant;

use clap::{Arg, Command};

mod generate;
mod extract;

use generate::generate_from_json;
use extract::write_dataframe_to_parquet;

const PROGRAM_NAME: &str = "rsfake";
const DEFAULT_SCHEMA_FILE: &str = "schema.json";
const DEFAULT_NO_ROWS: &str = "10000";
const RAYON_NUM_THREADS: &str = "1";


fn parse_cli_arguments() -> Command {
    Command::new(PROGRAM_NAME)
        .version(env!("CARGO_PKG_VERSION")) // set version from Cargo.toml
        .about("Generates fake data based on the provided schema file.")
        .long_about(format!(
            "This program generates fake data based on a JSON schema file. \
            You can specify the number of rows, the number of threads for \
            parallel processing, and the schema file to be used.\n\n\
            Example usage:\n    {} -s schema.json -r {} -t {}"
        , PROGRAM_NAME, DEFAULT_NO_ROWS, RAYON_NUM_THREADS))
        .arg(
            Arg::new("schema")
                .short('s')
                .long("schema")
                .env("FAKER_SCHEMA_FILE")
                .value_name("SCHEMA_FILE")
                .help("JSON file to describe column names and types")
                .default_value(DEFAULT_SCHEMA_FILE),
        )
        .arg(
            Arg::new("rows")
                .short('r')
                .long("rows")
                .env("FAKER_NUM_ROWS")
                .value_name("NUM_ROWS")
                .help("Number of rows to generate")
                .default_value(DEFAULT_NO_ROWS)
        )
        .arg(
            Arg::new("threads")
                .short('t')
                .long("threads")
                .env("RAYON_NUM_THREADS")
                .value_name("NO_THREADS")
                .help("Number of threads to use")
                .default_value(RAYON_NUM_THREADS),
        )
}


fn main() {
    let args: Vec<String> = env::args().collect();
    let app = parse_cli_arguments();
    let matches = app.try_get_matches_from(args).unwrap_or_else(|e| {
        e.exit();
    });

    let schema_file = matches
        .get_one::<String>("schema")
        .expect("Failed to parse schema file");

    // additional check to see if chema file exists
    if !std::path::Path::new(&schema_file).exists() {
        println!("Schema file \"{}\" does not exist", schema_file);
        parse_cli_arguments().print_help().unwrap();
        std::process::exit(1);
    }

    let no_threads = matches
        .get_one::<String>("threads")
        .map(|s| s.parse::<usize>().expect("Failed to parse thread count"))
        .expect("Failed to parse default thread count");

    let no_rows = matches
        .get_one::<String>("rows")
        .map(|s| s.parse::<usize>().expect("Failed to parse row count"))
        .expect("Failed to parse default row count");


    // set RAYON_NUM_THREADS in env for Rayon to pick it up
    env::set_var("RAYON_NUM_THREADS", no_threads.to_string());

    let start_time = Instant::now();
    let mut df = generate_from_json(DEFAULT_SCHEMA_FILE, no_rows).unwrap();
    let elapsed = start_time.elapsed().as_secs_f64();
    println!("{:?}", df);
    println!("Time taken to generate {no_rows} people into a dataframe using \
        {no_threads} threads:");
    println!("--- {:.3} seconds ---", elapsed);

    // write to Parquet
    let parquet_file = "people.parquet";
    let start_time = Instant::now();
    write_dataframe_to_parquet(&mut df, parquet_file).unwrap();
    let elapsed = start_time.elapsed().as_secs_f64();
    println!("Time taken to write to Parquet:");
    println!("--- {:.3} seconds ---", elapsed);
}
