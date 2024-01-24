use std::error::Error;
use std::fs::{self, File};
use std::path::Path;

use polars::prelude::*;
use std::io::BufWriter;

pub fn write_dataframe_to_single_parquet(
    df: &mut DataFrame,
    file_path: &str,
) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(df)?;
    Ok(())
}

pub fn cleanup_dataset_parquet_files(dataset_dir: &str) -> Result<(), Box<dyn Error>> {
    if Path::new(&dataset_dir).exists() {
        for entry in fs::read_dir(&dataset_dir)? {
            let path = entry?.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                fs::remove_file(path)?;
            }
        }
    }

    Ok(())
}

pub fn write_dataframe_chunk_to_parquet(
    df_chunk: &mut DataFrame,
    dataset_id: &str,
    base_dir: &str,
    part_number: usize,
) -> Result<(), Box<dyn Error>> {
    // Path for the dataset directory
    let dataset_dir = format!("{}/dataset={}", base_dir, dataset_id);

    // Ensure the dataset directory exists
    if !std::path::Path::new(&dataset_dir).exists() {
        fs::create_dir_all(&dataset_dir)?;
    }
    // Generate the part file name
    let file_path = format!("{}/part-{:05}.parquet", dataset_dir, part_number);

    // Write the DataFrame chunk to the Parquet file
    let file = File::create(&file_path)?;
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(df_chunk)?;
    Ok(())
}

pub fn write_dataframe_to_multi_parquet(
    df: &DataFrame,
    dataset_id: &str,
    base_dir: &str,
    chunk_size: usize,
) -> Result<(), Box<dyn Error>> {
    // Ensure the base directory and dataset directory exist
    let dataset_dir = format!("{}/dataset={}", base_dir, dataset_id);

    // create dataset directory if not exist, else clean up
    if !std::path::Path::new(&dataset_dir).exists() {
        fs::create_dir_all(&dataset_dir)?;
    } else {
        cleanup_dataset_parquet_files(&dataset_dir)?;
    }

    let n_rows = df.height();
    let mut part_number = 0;

    for start in (0..n_rows).step_by(chunk_size) {
        let end = std::cmp::min(start + chunk_size, n_rows);
        let chunk = df.slice(start as i64, end - start);

        // Convert chunk to mutable for writing
        let mut chunk_mut = chunk.clone();

        // write the chunk
        write_dataframe_chunk_to_parquet(&mut chunk_mut, dataset_id, base_dir, part_number)?;
        part_number += 1;
    }
    Ok(())
}

pub fn read_single_parquet_file(file_path: &str) -> Result<DataFrame, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let df = ParquetReader::new(file).finish()?;
    Ok(df)
}

pub fn read_partitioned_parquet(base_dir: &str) -> Result<DataFrame, Box<dyn Error>> {
    let mut dataframes: Vec<DataFrame> = Vec::new();

    fn read_parquet_files(
        path: &Path,
        dataframes: &mut Vec<DataFrame>,
    ) -> Result<(), Box<dyn Error>> {
        if path.is_dir() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    // Recursively read nested directories
                    read_parquet_files(&path, dataframes)?;
                } else if path.is_file()
                    && path.extension().and_then(|s| s.to_str()) == Some("parquet")
                {
                    let df = ParquetReader::new(File::open(path)?).finish()?;
                    dataframes.push(df);
                }
            }
        }
        Ok(())
    }

    let base_path = Path::new(base_dir);
    read_parquet_files(base_path, &mut dataframes)?;

    // Iteratively vstack DataFrames
    let mut combined_df = match dataframes.get(0) {
        Some(df) => df.clone(),
        None => return Err("No dataframes found".into()),
    };

    for df in dataframes.iter().skip(1) {
        combined_df = combined_df.vstack(df)?;
    }

    Ok(combined_df)
}
