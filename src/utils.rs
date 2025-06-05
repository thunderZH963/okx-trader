use std::time::{SystemTime, UNIX_EPOCH};
use crate::{globals::{INST2BESTASK, INST2BESTASK_DEPTH, INST2BESTBID, INST2BESTBID_DEPTH}, models::{OperationType, OrderBook}};

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

