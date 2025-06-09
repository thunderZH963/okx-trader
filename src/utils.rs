use std::time::{SystemTime, UNIX_EPOCH};
use crate::models::OperationType;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;


pub fn get_timestamp() -> u128 {
    let time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
    time
}

pub fn generate_nanoid() -> String {
    let alphabet: &[char] = &[
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];
    nanoid::nanoid!(29, alphabet)
}


pub fn spot_generate_client_order_id(operation: &OperationType, nanoid: &str) -> String {
    if *operation == OperationType::Open2 {
        format!("so2{}", nanoid)
    } else {
        format!("sc2{}", nanoid)
    }
}

pub fn futures_generate_client_order_id(operation: &OperationType, nanoid: &str) -> String {
    if *operation == OperationType::Open2 {
        format!("fo2{}", nanoid)
    } else {
        format!("fc2{}", nanoid)
    }
}

pub fn read_symbols(file_path: &str) -> io::Result<Vec<String>> {
    let path = Path::new(file_path);
    let file = File::open(path)?;
    let reader = io::BufReader::new(file);

    let mut symbols = Vec::new();

    for line in reader.lines() {
        match line {
            Ok(line_content) => {
                let trimmed = line_content.trim(); // 去除两端的空格和换行符
                if !trimmed.is_empty() {
                    symbols.push(trimmed.to_string());
                }
            },
            Err(e) => eprintln!("Error reading line: {}", e),
        }
    }

    Ok(symbols)
}

