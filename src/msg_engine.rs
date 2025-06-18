use futures_util::StreamExt;
use serde_json::{Value};
use tokio_tungstenite::tungstenite::Message;
use crate::globals::*;
use crate::okx_client::connect_okx_account;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use log::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn process_instruments_message(data: &Value, spot_inst_ids: Vec<&str>) {
    let mut inst2lotsz = INST2LOTSZ.lock().await;
    let mut inst2minsz = INST2MINSZ.lock().await;
    let mut inst2ctval = INST2CTVAL.lock().await;
    for instrument in data.as_array().unwrap_or(&vec![]) {
        let inst_id = instrument["instId"].to_string();
        if spot_inst_ids.contains(&inst_id.as_str()) == false {
            continue;
        }
        let lotSz = instrument["lotSz"].to_string();
        let minSz = instrument["minSz"].to_string();  
        let ct_val = instrument["ctVal"].to_string();
        inst2lotsz.insert(inst_id.clone(), Decimal::from_str(&lotSz).unwrap_or(Decimal::ZERO));
        inst2minsz.insert(inst_id.clone(), Decimal::from_str(&minSz).unwrap_or(Decimal::ZERO));
        inst2ctval.insert(inst_id.clone(), Decimal::from_str(&ct_val).unwrap_or(Decimal::ZERO));
        info!("Instruments handler handles update msg: inst_id {:?}, lotsz {:?}, minsz {:?} and ctval {:?}", inst_id, lotSz, minSz, ct_val);
    }
}

pub async fn process_account_message(data: &Value) {
    let mut ccy2bal = CCY2BAL.lock().await;
    for json_item in data[0]["balData"].as_array().unwrap_or(&vec![]) {
        match Decimal::from_str(json_item["cashBal"].as_str().unwrap()) {
            Ok(decimal_value) => {
                let mut tmp = json_item["ccy"].as_str().unwrap().to_string();
                if tmp == "USDT" {
                    info!("Account Message Updater: USDT cashBal is {:?}", decimal_value);
                    ccy2bal.insert(tmp.clone(), decimal_value);
                } else {
                    tmp = tmp + "-USDT";
                    ccy2bal.insert(tmp.clone(), decimal_value);
                    info!("Account Message Updater: ccy {:?} and cashBal {:?} are inserted", tmp.clone(), decimal_value);
                }
            }
            Err(e) => {
                info!("Account Message Updater: Error converting cashBal to Decimal: {}", e);
            }
        }
    }
    for json_item in data[0]["posData"].as_array().unwrap_or(&vec![]) {
        match Decimal::from_str(json_item["pos"].as_str().unwrap()) {
            Ok(decimal_value) => {
                ccy2bal.insert(json_item["instId"].as_str().unwrap().to_string(), decimal_value);
                info!("Account Message Updater: ccy {:?} and cashBal {:?} are inserted", json_item["instId"].to_string(), decimal_value);
            }
            Err(e) => {
                info!("Account Message Updater: Error converting cashBal to Decimal: {}", e);
            }
        }
    }
}

/*
 * FIX: asks和bids值数组举例说明： ["411.8", "10", "0", "4"]
    - 411.8为深度价格
    - 10为此价格的数量 （合约交易为张数，现货/币币杠杆为交易币的数量
    - 0该字段已弃用(始终为0)
    - 4为此价格的订单数量
 */
pub async fn process_books5_message(inst_id: String, data: &Value) {
    let mut res_ask: Vec<Vec<Decimal>> = Vec::new();
    for ask in data["asks"].as_array().unwrap_or(&vec![]) {
        let price = ask.get(0).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());
        let nums = ask.get(1).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());
        let orders = ask.get(3).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());
        if let (Some(price), Some(nums), Some(orders)) = (price, nums, orders) {
            res_ask.push(vec![price, nums, orders]);
        }
    }

    let mut res_bid: Vec<Vec<Decimal>> = Vec::new();

    for ask in data["bids"].as_array().unwrap_or(&vec![]) {
        let price = ask.get(0).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());
        let nums = ask.get(1).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());
        let orders = ask.get(3).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());

        if let (Some(price), Some(nums), Some(orders)) = (price, nums, orders) {
            res_bid.push(vec![price, nums, orders]); 
        }
    }
    
    let mut depth_map = DEPTH_MAP_Books5.lock().await;
    depth_map.insert(inst_id.clone(), (res_ask.clone(), res_bid.clone()));
    let mut inst2bestbid = INST2BESTBID_DEPTH.lock().await;
    match res_bid.clone().get(0) {
        Some(bid) => {
            inst2bestbid.insert(inst_id.clone(), bid.clone());
        },
        None => {
            println!("Error: No bid found {}", data);
        },
    }

    let mut inst2bestask = INST2BESTASK_DEPTH.lock().await;
    // inst2bestask.insert(inst_id.clone(), res_ask.clone().get(0).unwrap().clone());
    match res_ask.clone().get(0) {
        Some(ask) => {
            inst2bestask.insert(inst_id.clone(), ask.clone());
        },
        None => {
            println!("Error: No ask found {}", data);
        },
    }

    // info!("BOOKS5 update inst_id {:?},  inst2bestbid{:?} and inst2bestask", inst2bestbid, inst2bestask);
}

pub async fn process_bbotbt_message(inst_id: String, data: &Value) {
    let mut inst2bestbid = INST2BESTBID.lock().await;
    let mut inst2bestask = INST2BESTASK.lock().await;

    for ask in data["asks"].as_array().unwrap_or(&vec![]) {
        let price = ask.get(0).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());
        let nums = ask.get(1).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());
        let orders = ask.get(3).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());

        if let (Some(price), Some(nums), Some(orders)) = (price, nums, orders) {
            inst2bestask.insert(inst_id.clone(), vec![price, nums, orders]);
        }
    }

    for ask in data["bids"].as_array().unwrap_or(&vec![]) {
        let price = ask.get(0).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());
        let nums = ask.get(1).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());
        let orders = ask.get(3).and_then(Value::as_str)
            .and_then(|s| Decimal::from_str(s).ok());

        if let (Some(price), Some(nums), Some(orders)) = (price, nums, orders) {
            inst2bestbid.insert(inst_id.clone(), vec![price, nums, orders]);
        }
    }
    // info!("BOOKS TBT update inst_id {:?},  inst2bestbid {:?} and inst2bestask {:?}", inst_id, inst2bestbid, inst2bestask);

}

// #[tokio::test]
// async fn test_balance_and_position() {
//     let (mut write_private, mut read_private) = connect_okx_account(key.clone(), secret.clone(), passphrase.clone()).await;
//     let msg = read_private.next().await.unwrap();
//     match msg { 
//          Message::Text(text) => {
//             let parsed_msg: Value = serde_json::from_str(&text).expect("Failed to parse JSON");
//             let channel: String = parsed_msg["arg"]["channel"].as_str().unwrap_or("Unknown").to_string();
//             let data = &parsed_msg["data"];
//             if data.is_null() {
//                 info!("Receiving unprocessed msg from private websocket balance_and_position channel {:?}", parsed_msg);
//             } else {
//                 if channel == "balance_and_position" {
//                     process_account_message(data).await;
//                 } else {
//                     panic!("Receiving unknown channel msg from private websocket balance_and_position channel {:?}", parsed_msg);
//                 }
//             }
//         }
//     }
               
// }

