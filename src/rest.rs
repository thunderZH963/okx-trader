use log::info;
use okx_rs::api::{v5::GetInstruments, Options, Production, Rest}; // for REST, others for websocket
use okx_rs::api::v5::model::InstrumentType::{Spot, Swap};
use serde_json::{Value};
use crate::globals::{INST2LOTSZ, INST2MINSZ, INST2CTVAL};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

pub async fn getInstruments(spot_inst_ids: Vec<&str>, swap_inst_ids: Vec<&str>) {
    /*
     * 查询instruments信息Rest
     * Rest API for GET instruments full data
     */
    let mut client_rest = Rest::new(Options::new(Production)); // 先用redis获取instruments数据的全量
    
    let response_spot = client_rest.request(GetInstruments {
            inst_type: Spot,
            uly: None,
            inst_family: None,
            inst_id: None,
        }).await.unwrap();
    let response_swap = client_rest.request(GetInstruments {
        inst_type: Swap,
        uly: None,
        inst_family: None,
        inst_id: None,
    }).await.unwrap();
    let response_spot_str = serde_json::to_string_pretty(&response_spot).unwrap();
    let response_swap_str = serde_json::to_string_pretty(&response_swap).unwrap();
    let response_spot_parsed_msg: Value = serde_json::from_str(&response_spot_str).expect("Failed to parse JSON");
    let response_swap_parsed_msg: Value = serde_json::from_str(&response_swap_str).expect("Failed to parse JSON");
    {
        let mut inst2lotsz = INST2LOTSZ.lock().await;
        let mut inst2minsz = INST2MINSZ.lock().await;
        let mut inst2ctval = INST2CTVAL.lock().await;
        for response_spot in response_spot_parsed_msg.as_array().unwrap_or(&vec![]) {
            let inst_id = response_spot["instId"].as_str().unwrap_or("");
            let lot_sz = response_spot["lotSz"].as_str().unwrap_or("");
            let lot_sz: Decimal = Decimal::from_str(lot_sz).unwrap_or(Decimal::ZERO);
            let min_sz = response_spot["minSz"].as_str().unwrap_or("");
            let min_sz: Decimal = Decimal::from_str(min_sz).unwrap_or(Decimal::ZERO);
            let ct_val = response_spot["ctVal"].as_str().unwrap_or("");
            let ct_val: Decimal = Decimal::from_str(ct_val).unwrap_or(Decimal::ZERO);
            if spot_inst_ids.contains(&inst_id) {
                inst2lotsz.insert(inst_id.to_string(), lot_sz);
                inst2minsz.insert(inst_id.to_string(), min_sz);
                inst2ctval.insert(inst_id.to_string(), ct_val);
            }
        }
        for response_swap in response_swap_parsed_msg.as_array().unwrap_or(&vec![]) {
            let inst_id = response_swap["instId"].as_str().unwrap_or("");
            let lot_sz = response_swap["lotSz"].as_str().unwrap_or("");
            let lot_sz: Decimal = Decimal::from_str(lot_sz).unwrap_or(Decimal::ZERO);
            let min_sz = response_swap["minSz"].as_str().unwrap_or("");
            let ct_val = response_swap["ctVal"].as_str().unwrap_or("");
            let ct_val: Decimal = Decimal::from_str(ct_val).unwrap_or(Decimal::ZERO);
            let min_sz: Decimal = Decimal::from_str(min_sz).unwrap_or(Decimal::ZERO);
            if swap_inst_ids.contains(&inst_id) {
                inst2lotsz.insert(inst_id.to_string(), lot_sz);
                inst2minsz.insert(inst_id.to_string(), min_sz);
                inst2ctval.insert(inst_id.to_string(), ct_val);
            }
        }
        info!("Rest API for GET instruments full data: lotsz is {:?}, minsz is {:?} and ctval is {:?}", inst2lotsz, inst2minsz, inst2ctval);
    }
}