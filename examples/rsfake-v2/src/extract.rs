use std::fs::{self, File};
use std::io::BufReader;
use std::error::Error;

use std::io::BufWriter;
use polars::prelude::*;


pub fn write_dataframe_to_parquet(
    df: &mut DataFrame,
    file_path: &str
) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let writer = BufWriter::new(file);
    ParquetWriter::new(writer).finish(df)?;
    Ok(())
}

#[allow(dead_code)]
pub fn write_dataframe_to_partitioned_parquet(
    df: &mut DataFrame, 
    partition_column: &str, 
    base_dir: &str,
) -> Result<(), Box<dyn Error>> {
    // Ensure the base directory exists
    fs::create_dir_all(base_dir)?;

    // Get unique values in the partition column
    let unique_values = df.column(partition_column)?.unique()?;

    for value in unique_values.utf8()?.into_iter() {
        if let Some(value_str) = value {
            // Create a boolean mask for filtering
            let mask = df.column(partition_column)?.equal(value_str)?;

            // Filter the DataFrame based on the mask
            let mut partition_df = df.filter(&mask)?;

            // Create a directory for the partition
            let partition_dir = format!("{}/{}", base_dir, value_str);
            fs::create_dir_all(&partition_dir)?;

            // Define the file path
            let file_path = format!("{}/data.parquet", partition_dir);

            // Write the partition DataFrame to a Parquet file
            let file = File::create(file_path)?;
            let writer = BufWriter::new(file);
            ParquetWriter::new(writer).finish(&mut partition_df)?;
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub fn read_dataframe_from_parquet(file_path: &str) -> Result<DataFrame, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let df = ParquetReader::new(reader).finish()?;
    Ok(df)
}

#[allow(dead_code)]
pub fn read_partitioned_parquet(base_dir: &str) -> Result<DataFrame, Box<dyn Error>> {
    let mut dataframes: Vec<DataFrame> = Vec::new();

    // List all subdirectories in the base directory
    for entry in fs::read_dir(base_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            // Assuming there's only one Parquet file per partition in each subdirectory
            let parquet_path = path.join("data.parquet");
            if parquet_path.exists() {
                // Read the Parquet file into a DataFrame
                let dataframe = ParquetReader::new(fs::File::open(parquet_path)?).finish()?;
                dataframes.push(dataframe);
            }
        }
    }

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
