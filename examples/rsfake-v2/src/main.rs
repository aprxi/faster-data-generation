use std::env;
use std::path::Path;
use std::time::Instant;

use clap::{Arg, Command};
use polars::frame::DataFrame;

mod extract;
mod generate;

use extract::{
    read_partitioned_parquet, read_single_parquet_file, write_dataframe_to_multi_parquet,
    write_dataframe_to_single_parquet,
};
use generate::generate_from_json;

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
            Example usage:\n    {} -s schema.json -r {} -t {}",
            PROGRAM_NAME, DEFAULT_NO_ROWS, RAYON_NUM_THREADS
        ))
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
                .default_value(DEFAULT_NO_ROWS),
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
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .env("FAKER_OUTPUT_PATH")
                .value_name("OUTPUT_PATH")
                .help("Output path to write to"),
        )
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .env("FAKER_INPUT_PATH")
                .value_name("INPUT_PATH")
                .help("Input path to read from"),
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

    // additional check to see if schema file exists
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

    let output_path = matches.get_one::<String>("output");
    let input_path = matches.get_one::<String>("input");

    // set RAYON_NUM_THREADS in env for Rayon to use
    env::set_var("RAYON_NUM_THREADS", no_threads.to_string());

    let mut df: DataFrame;

    // read from parquet if input_path is specified
    if let Some(input_path) = input_path {
        let start_time = Instant::now();
        let path = Path::new(input_path);

        df = if path.is_dir() {
            match read_partitioned_parquet(input_path) {
                Ok(data) => data,
                Err(e) => {
                    println!("Error reading partitioned Parquet: {:?}", e);
                    return;
                }
            }
        } else if path.is_file() {
            match read_single_parquet_file(input_path) {
                Ok(data) => data,
                Err(e) => {
                    println!("Error reading single Parquet file: {:?}", e);
                    return;
                }
            }
        } else {
            // input path is neither a file nor a directory
            println!(
                "Error: Input path \"{}\" is neither a file nor a directory",
                input_path
            );
            return;
        };

        let elapsed = start_time.elapsed().as_secs_f64();
        println!("{:?}", df);
        println!("Time taken to read from Parquet: {:.3} seconds", elapsed);
    } else {
        let start_time = Instant::now();
        df = generate_from_json(DEFAULT_SCHEMA_FILE, no_rows).unwrap();
        let elapsed = start_time.elapsed().as_secs_f64();
        println!("{:?}", df);
        println!(
            "Time taken to generate {no_rows} people into a dataframe using \
            {no_threads} threads:"
        );
        println!("--- {:.3} seconds ---", elapsed);
    }

    // write to parquet if output_path is specified
    if let Some(output_path) = output_path {
        let path = Path::new(output_path);
        let mut is_partitioned = false;

        // Check if the path contains a "/" indicating a multi-parquet file
        if path.to_str().unwrap_or("").contains("/") {
            is_partitioned = true;

            // Check if a file with the same base name already exists
            let base_path = Path::new(output_path.trim_end_matches('/'));
            if base_path.exists() && base_path.is_file() {
                println!(
                    "Error: A file with the name '{}' already exists.",
                    base_path.display()
                );
                return;
            }
        }

        let start_time: Instant;
        let elapsed: f64;

        if is_partitioned {
            // partitioned parquet file
            println!(
                "Output directory for multi-parquet file data: {}",
                output_path
            );
            let dataset_id = "0";
            let chunk_size = no_rows / no_threads;
            start_time = Instant::now();
            let _ = write_dataframe_to_multi_parquet(&mut df, dataset_id, &output_path, chunk_size)
                .unwrap();
            elapsed = start_time.elapsed().as_secs_f64();
        } else {
            // single parquet file
            println!("Output file for single-parquet file data: {}", output_path);
            start_time = Instant::now();
            let _ = write_dataframe_to_single_parquet(&mut df, &output_path).unwrap();
            elapsed = start_time.elapsed().as_secs_f64();
        }
        println!("Time taken to write to Parquet: {:.3} seconds", elapsed);
    }
}
