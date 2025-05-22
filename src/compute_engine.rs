use crate::{globals::{INST2BESTASK, INST2BESTASK_DEPTH, INST2BESTBID, INST2BESTBID_DEPTH}, models::{OperationType, OrderBook}};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

pub async fn calculate_basis_and_signal(spot_inst_id: String, swap_inst_id: String, depth: bool, threshold_2_open: Option<f64>, threshold_2_close: Option<f64>) -> OrderBook {
    let mut order_book = OrderBook {
        operation_type: OperationType::NoOP,
        ps_best_bid: 0.0,                
        ps_best_bid_qty: 0.0,           
        ps_best_ask: 0.0,              
        ps_best_ask_qty: 0.0,          
        pp_best_bid: 0.0,         
        pp_best_bid_qty: 0.0, 
        pp_best_ask: 0.0,
        pp_best_ask_qty: 0.0, 
    };
    
    let mut inst2bestbid;
    let mut inst2bestask;
    {
        if depth == true {
            inst2bestbid = INST2BESTBID_DEPTH.lock().await;
            inst2bestask = INST2BESTASK_DEPTH.lock().await;
        } else {
            inst2bestbid = INST2BESTBID.lock().await;
            inst2bestask = INST2BESTASK.lock().await;
        }
    }

    if let Some(vec) = inst2bestbid.get(&swap_inst_id) {
        if let Some(value) = vec.get(0).and_then(|v| v.to_f64()) {
            order_book.pp_best_bid = value;
        }
        if let Some(value) = vec.get(1).and_then(|v| v.to_f64()) {
            order_book.pp_best_bid_qty = value;
        }
    }
    if let Some(vec) = inst2bestask.get(&swap_inst_id) {
        if let Some(value) = vec.get(0).and_then(|v| v.to_f64()) {
            order_book.pp_best_ask = value;
        }
        if let Some(value) = vec.get(1).and_then(|v| v.to_f64()) {
            order_book.pp_best_ask_qty = value;
        }
    }

    if let Some(vec) = inst2bestbid.get(&spot_inst_id) {
        if let Some(value) = vec.get(0).and_then(|v| v.to_f64()) {
            order_book.ps_best_bid = value;
        }
        if let Some(value) = vec.get(1).and_then(|v| v.to_f64()) {
            order_book.ps_best_bid_qty = value;
        }
    }
    if let Some(vec) = inst2bestask.get(&spot_inst_id) {
        if let Some(value) = vec.get(0).and_then(|v| v.to_f64()) {
            order_book.ps_best_ask = value;
        }
        if let Some(value) = vec.get(1).and_then(|v| v.to_f64()) {
            order_book.ps_best_ask_qty = value;
        }
    }


    let basis_open2 = order_book.pp_best_bid.ln() -  order_book.ps_best_ask.ln();
    let basis_close2 = order_book.pp_best_ask.ln() - order_book.ps_best_bid.ln();
    
    let mut depth_operation = OperationType::NoOP;
    
    if let Some(threshold) = threshold_2_open {
        if basis_open2 >= threshold {
            depth_operation = OperationType::Open2;
        }
    }
    if let Some(threshold) = threshold_2_close {
        if basis_close2 >= threshold {
            depth_operation = OperationType::Open2;
        }
    }
    order_book.operation_type = depth_operation;
    order_book
}