use fake::{Fake, Faker};
use std::fs;

use serde_json::Value;

use polars::prelude::*;
use rayon::prelude::*;

use fake::faker::address::raw::*;
use fake::faker::company::raw::*;
use fake::faker::internet::raw::*;
use fake::faker::name::raw::*;
use fake::faker::phone_number::raw::*;
use fake::locales::*;

pub fn load_json(json_file: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let json_str = fs::read_to_string(json_file)?;
    let json: Value = serde_json::from_str(&json_str)?;
    Ok(json)
}

pub fn generate_from_json(
    json_file: &str,
    no_rows: usize,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let json = load_json(json_file)?;

    let mut columns = Vec::new();

    if let Some(columns_def) = json.get("columns").and_then(|c| c.as_array()) {
        for col_def in columns_def {
            let col_name = col_def
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default();
            let col_type = col_def
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or_default();

            let series_en = create_series_from_type(col_type, col_name, no_rows, EN);
            columns.push(series_en);
        }
    }
    Ok(DataFrame::new(columns)?)
}

fn create_series_from_type<L>(
    type_name: &str,
    col_name: &str,
    no_rows: usize,
    locale: L,
) -> Series
where
    L: Data + Sync + Send + Copy,
{
    match type_name {
        "u64" => {
            let data = (0..no_rows)
                .into_par_iter()
                .map(|_| Faker.fake::<u64>())
                .collect::<Vec<u64>>();
            Series::new(col_name, data)
        }
        "FirstName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FirstName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "LastName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| LastName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "FreeEmail" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FreeEmail(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CompanyName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CompanyName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "PhoneNumber" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| PhoneNumber(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "StreetName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| StreetName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        _ => panic!("Unsupported type: {}", type_name),
    }
}
