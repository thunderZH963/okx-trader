/*
 * importing self-developed external libs
 */
mod utils;
mod models;
mod globals;
mod msg_engine;
mod compute_engine;
use utils::{generate_nanoid, spot_generate_client_order_id, futures_generate_client_order_id};
use models::OperationType;
use globals::*;
use msg_engine::*;
use compute_engine::*;

/*
 * importing okx rust API
 */
use okx_rs::api::Rest; // for REST, others for websocket
use okx_rs::api::{Production, DemoTrading, OKXEnv};
use okx_rs::api::v5::BalanceAndPositionChannel;
use okx_rs::api::Options;
use okx_rs::websocket::OKXAuth;
use okx_rs::websocket::WebsocketChannel;
use okx_rs::api::v5::model::InstrumentType::{Spot, Swap};
use okx_rs::api::v5::GetInstruments;
use okx_rs::websocket::conn::{Books5, BboTbt, Instruments, Order};

/*
 * importing std or external well-known libs
 */
use dotenv; //env loader
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, BTreeMap};
use serde_json::{Value};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::sync::mpsc;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

/*
 * Define for Production or DemoTrading
 */
static TEST: i32 = 0;


#[tokio::main]
async fn main() {
    /*
     * Some Necessary Initialization
     */
    env_logger::init();
    dotenv::dotenv().ok();
    let key = std::env::var("OKX_API_KEY").unwrap();
    let secret = std::env::var("OKX_API_SECRET").unwrap();
    let passphrase = std::env::var("OKX_API_PASSPHRASE").unwrap();

    /*
     * Some Necessary Values Initialization
     */
    let spot_inst_ids = vec!["TRUMP-USDT",]; //现货类型
    let swap_inst_ids = vec!["TRUMP-USDT-SWAP", ]; //合约类型
    init_ccy2bal(spot_inst_ids.clone()).await;
    init_ccy2bal(swap_inst_ids.clone()).await;
    {
        let mut depth_map = DEPTH_MAP.lock().unwrap(); //记录盘口深度数据（from books channel）
        let mut local_deltas_spot = LOCAL_DELTAS_SPOT.lock().await; //记录更新交易的spot的开仓量的变量(两次策略更新之间)
        for id in spot_inst_ids.clone() {
            depth_map.insert(id.to_string(), (BTreeMap::new(), BTreeMap::new()));
            local_deltas_spot.insert(id.to_string(), 0.0);
        }
        for id in swap_inst_ids.clone() {
            depth_map.insert(id.to_string(), (BTreeMap::new(), BTreeMap::new()));
        }
    }
    let (tx_books, mut rx_compute) = mpsc::channel::<String>(32); // a thread communication channel for books_handle and compute_handle
    let (tx_compute, mut rx_order) = mpsc::channel::<(Order, Order)>(32); // a thread communication channel for compute_handle and order_handle


    /*
     * Loading trade signals
     */
    init_trade_signals().await; // TODO: It will be replaced by subscribing redis later


    /*
     * A private websocket channel initialization for account info
     */
    let mut options_private = Options::new_with(Production, key.clone(), secret.clone(), passphrase.clone());
    let (mut client_private, mut response_private) = connect_async(Production.private_websocket()).await.unwrap();
    if TEST == 1 {
        options_private = Options::new_with(DemoTrading, key.clone(), secret.clone(), passphrase.clone());
        (client_private, response_private) = connect_async(DemoTrading.private_websocket()).await.unwrap();
    }
    let (mut write_private, mut read_private) = client_private.split();
    let auth_msg_private = OKXAuth::ws_auth(options_private).unwrap();
    write_private.send(auth_msg_private.into()).await.unwrap();    
    let auth_resp_private = read_private.next().await.unwrap();
    println!("A private websocket channel auth: {:?}", auth_resp_private);
    let _ = write_private.send(BalanceAndPositionChannel.subscribe_message().into()).await;
    let account_channel_handle = tokio::spawn(async move {
        loop {
            let msg = match read_private.next().await {
                Some(Ok(Message::Text(msg))) => msg,
                Some(Err(err)) => {
                    panic!("{:?}", err);
                }
                _ => continue,
            };
            let parsed_msg: Value = serde_json::from_str(&msg).expect("Failed to parse JSON");
            let channel: String = parsed_msg["arg"]["channel"].as_str().unwrap_or("Unknown").to_string();
            let data = &parsed_msg["data"];
            if data.is_null() {
                println!("Receiving unprocessed msg from private websocket balance_and_position channel {:?}", msg);
            } else {
                if channel == "balance_and_position" {
                    process_account_message(data).await;
                } else {
                    panic!("Receiving unknown channel msg from private websocket balance_and_position channel {:?}", msg);
                }
            }
        }
    });

     /*
     * A private websocket channel initialization for order
     */
    let mut options_order = Options::new_with(Production, key.clone(), secret.clone(), passphrase.clone());
    let (mut client_order, mut response_order) = connect_async(Production.private_websocket()).await.unwrap();
    if TEST == 1 {
        options_order = Options::new_with(DemoTrading, key.clone(), secret.clone(), passphrase.clone());
        (client_order, response_order) = connect_async(DemoTrading.private_websocket()).await.unwrap();
    }
    let (mut write_order, mut read_order) = client_order.split();
    let auth_msg_order = OKXAuth::ws_auth(options_order).unwrap();
    write_order.send(auth_msg_order.into()).await.unwrap();    
    let auth_resp_order = read_order.next().await.unwrap();
    println!("A private order websocket channel auth: {:?}", auth_resp_order);


     /*
     * A public websocket channel initialization for books asks/bids and instruments incrementing
     */
    let mut options = Options::new_with(Production, key.clone(), secret.clone(), passphrase.clone());
    let (mut client, mut response) = connect_async(Production.public_websocket()).await.unwrap();
    if TEST == 1 {
        options = Options::new_with(DemoTrading, key.clone(), secret.clone(), passphrase.clone());
        (client, response) = connect_async(DemoTrading.public_websocket()).await.unwrap();
    }
    let (mut write, mut read) = client.split();
    let auth_msg = OKXAuth::ws_auth(options).unwrap();
    write.send(auth_msg.into()).await.unwrap();    
    let auth_resp = read.next().await.unwrap();
    println!("A public websocket channel auth: {:?}", auth_resp);
    for inst_id in spot_inst_ids.clone() { // 遍历SPOT_inst_ids，创建Books并发送订阅消息，同时订阅books5（depth is 5）和bookstbt（just best）的数据
        let books = Books5 {
            inst_id: String::from(inst_id),
        };
        let books_tbt = BboTbt {
            inst_id: String::from(inst_id),
        };
        let _ = write.send(books.subscribe_message().into()).await;
        let _ = write.send(books_tbt.subscribe_message().into()).await;
    }
    for inst_id in swap_inst_ids.clone() { // 遍历SWAP_inst_ids，创建Books并发送订阅消息
        let books = Books5 {
            inst_id: String::from(inst_id),
        };
        let books_tbt = BboTbt {
            inst_id: String::from(inst_id),
        };
        let _ = write.send(books.subscribe_message().into()).await;
        let _ = write.send(books_tbt.subscribe_message().into()).await;
    }
    let instruments = Instruments {
        instType: "SPOT".to_string(),
    };
    let _ = write.send(instruments.subscribe_message().into()).await; //订阅spot的instruments数据(只推送增量)
    let instruments = Instruments {
        instType: "SWAP".to_string(),
    };
    let _ = write.send(instruments.subscribe_message().into()).await; //订阅swap的instruments数据(只推送增量)


    /*
     * Rest API for GET instruments full data
     */
    let mut client_rest = Rest::new(Options::new(Production)); // 先用redis获取instruments数据的全量
    if TEST == 1 {
        client_rest = Rest::new(Options::new(DemoTrading));
    }
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
        for response_spot in response_spot_parsed_msg.as_array().unwrap_or(&vec![]) {
            let inst_id = response_spot["instId"].as_str().unwrap_or("");
            let lot_sz = response_spot["lotSz"].as_str().unwrap_or("");
            let lot_sz: Decimal = Decimal::from_str(lot_sz).unwrap_or(Decimal::ZERO);
            let min_sz = response_spot["minSz"].as_str().unwrap_or("");
            let min_sz: Decimal = Decimal::from_str(min_sz).unwrap_or(Decimal::ZERO);
            if spot_inst_ids.contains(&inst_id) {
                inst2lotsz.insert(inst_id.to_string(), lot_sz);
                inst2minsz.insert(inst_id.to_string(), min_sz);
            }
        }
        for response_swap in response_swap_parsed_msg.as_array().unwrap_or(&vec![]) {
            let inst_id = response_swap["instId"].as_str().unwrap_or("");
            let lot_sz = response_swap["lotSz"].as_str().unwrap_or("");
            let lot_sz: Decimal = Decimal::from_str(lot_sz).unwrap_or(Decimal::ZERO);
            let min_sz = response_swap["minSz"].as_str().unwrap_or("");
            let min_sz: Decimal = Decimal::from_str(min_sz).unwrap_or(Decimal::ZERO);
            if swap_inst_ids.contains(&inst_id) {
                inst2lotsz.insert(inst_id.to_string(), lot_sz);
                inst2minsz.insert(inst_id.to_string(), min_sz);
            }
        }
        println!("Rest API for GET instruments full data: lotsz is {:?} and minsz is {:?}", inst2lotsz, inst2minsz);
    }
    
    /*
     * Books/Instruments handler
     * Handling depth/best ask/bids data and instruments updating
     */
    let books_channel_handle = tokio::spawn(async move {
        for n in 1..32 { // TODO: modify to loop later
        //loop {
            let msg = match read.next().await {
                Some(Ok(Message::Text(msg))) => msg,
                Some(Err(err)) => {
                    panic!("{:?}", err);
                }
                _ => continue,
            };
            let parsed_msg: Value = serde_json::from_str(&msg).expect("Failed to parse JSON");
            let channel = parsed_msg["arg"]["channel"].as_str().unwrap_or("Unknown").to_string();
            let inst_id = parsed_msg["arg"]["instId"].as_str().unwrap_or("Unknown").to_string();
            let data = &parsed_msg["data"][0];
            if data.is_null() {
                println!("***************Books/Instruments handler reveives first msg with non-data {:?}", msg);
            } else {
                if channel == "instruments" {
                    let inst_type = parsed_msg["arg"]["instType"].as_str().unwrap_or("Unknown").to_string();
                    println!("***************Instruments handler handles update msg: inst_type {:?}", inst_type);
                    process_instruments_message(&parsed_msg["data"]).await;
                } else if channel == "books5" {
                    println!("***************Books handler handles BOOKS5 update msg: inst_id {:?}", inst_id);
                    process_books5_message(inst_id.clone(), data).await;
                    tx_books.send(inst_id.clone()).await.unwrap();
                } else if channel == "bbo-tbt" {
                    println!("***************Books handler handles BBO-TBT update msg: inst_id {:?}", inst_id);
                    process_bbotbt_message(inst_id.clone(), data).await;
                    tx_books.send(inst_id.clone()).await.unwrap();
                } else {
                    panic!("***************Books/Instruments handler receives msg from unknown channel: {:?}", msg);
                }
            }
        }
    });
    
    /*
     * Compute Engine
     * Receiving msgs from books/instruments thread
     * Checking and Calculating whether we can order swap/spot
     */
    let compute_handle =  tokio::spawn(async move {
        let max_open_value = 400.0;
        let max_close_value = 1000.0;

        while let Some(msg) = rx_compute.recv().await {
            /*
             * Message Receiving Processor
             * Get the swap/spot insts ready for calculating further
             */
            let swap_inst_id: String;
            let spot_inst_id: String;
            if msg.contains("SWAP") {
                swap_inst_id = msg.clone();
                spot_inst_id = msg.trim_end_matches("-SWAP").to_string();
            } else {
                swap_inst_id = format!("{}-SWAP", msg);
                spot_inst_id = msg.clone();
            }
            println!("###############Message Receiving Processor: computing engine begins to process swap/spot: {:?} {:?}", swap_inst_id, spot_inst_id);

            /*
             * Trade Signal Acquirer
             * It contains four values serving for operation check
             */
            let mut threshold_2_open = None;
            let mut threshold_2_close = None;
            let mut threshold_2_caution = None;
            let mut threshold_2_number = None;
            {
                let trade_signals = GLOBAL_TRADE_SIGNALS.lock().await;
                let signal = trade_signals.get(&spot_inst_id);
                threshold_2_open = signal.unwrap().threshold_2_open;
                threshold_2_close = signal.unwrap().threshold_2_close;
                threshold_2_caution = signal.unwrap().threshold_2_caution;
                threshold_2_number = signal.unwrap().threshold_2_number;
            }
            println!("###############Trade Signal Acquirer: trade signal is threshold_2_open={:?}, threshold_2_close={:?}, threshold_2_caution={:?}, threshold_2_number={:?}", threshold_2_open, threshold_2_close, threshold_2_caution, threshold_2_number);

            /*
             * Order Book Calculator
             * It uses booked depth and non-depth channels' best swap/spot ask/bid
             */
            let orderbook_depth =  calculate_basis_and_signal(spot_inst_id.clone(), swap_inst_id.clone(), true, threshold_2_open, threshold_2_close).await;
            let orderbook = calculate_basis_and_signal(spot_inst_id.clone(), swap_inst_id.clone(), false, threshold_2_open, threshold_2_close).await;
            println!("###############Order Book Calculator: orderbook for depth is {:?}, orderbook for non-depth is {:?}", orderbook_depth, orderbook);

            if orderbook_depth.operation_type == OperationType::NoOP || orderbook.operation_type == OperationType::NoOP { // skip noop signal
                continue;
            }

            if orderbook_depth.operation_type == orderbook.operation_type {
                /*
                 * Get account's balance(USDT, 账户余额约束) and position(by spot_inst_id， 持仓数量约束)
                 * Get spot/swap trading's min size(最小下单量约束) and lot size(下单数量精度约束)
                 */

                let mut spot_lot_size = Decimal::new(0, 0);
                let mut swap_lot_size = Decimal::new(0, 0);
                let mut lot_size = Decimal::new(0, 0);
                let mut spot_min_size = Decimal::new(0, 0);
                let mut swap_min_size = Decimal::new(0, 0);
                let mut balance_usdt = Decimal::new(0, 0);
                let mut position_spot = Decimal::new(0, 0);
                
                {
                    let mut inst2lotsz = INST2LOTSZ.lock().await;
                    let mut inst2minsz = INST2MINSZ.lock().await;
                    
                    spot_lot_size = inst2lotsz.get(&spot_inst_id).unwrap_or(&Decimal::zero()).clone();
                    swap_lot_size = inst2lotsz.get(&swap_inst_id).unwrap_or(&Decimal::zero()).clone();
                    // println!("debug, {:?} {:?}", inst2lotsz, spot_lot_size);
                    spot_min_size = inst2minsz.get(&spot_inst_id).unwrap_or(&Decimal::zero()).clone();
                    swap_min_size = inst2minsz.get(&swap_inst_id).unwrap_or(&Decimal::zero()).clone();
                    lot_size = spot_lot_size.max(swap_lot_size);
                }
                {
                    let ccy2bal = CCY2BAL.lock().await;
                    balance_usdt = ccy2bal.get("USDT").unwrap_or(&Decimal::zero()).clone();
                    position_spot = ccy2bal.get(&spot_inst_id).unwrap_or(&Decimal::zero()).clone();
                    // println!("debug {:?} {:?}", spot_inst_id, ccy2bal);
                }
                let is_open = orderbook.operation_type == OperationType::Open2;
                let is_close = !is_open;
                let mut constraint_percent_qty = 0.2;
                if let Some(caution) = threshold_2_caution { //0/1, if 1: 谨慎策略
                    if caution > 0.0 {
                        constraint_percent_qty = 0.1
                    }
                };
                
                println!("###############Basic Order Para Calculator: spot_inst_id is {:?}, spot_lot_sz is {:?}, swap_lot_sz is {:?}, lot_sz is {:?}, spot_min_sz is {:?}, swap_min_sz is {:?}, balance_usdt is {:?}, position_spot is {:?} and constraint_percent_qty is {:?}", 
                                                                spot_inst_id, spot_lot_size, swap_lot_size, lot_size, spot_min_size, swap_min_size, balance_usdt, position_spot, constraint_percent_qty);
                
                /*
                 * Get basis trade qty
                 * Using the min value between depth and non-depth
                 * Using constraint_percent_qty
                 * Using the min value between swap and spot
                 */
                let ps_best_ask_qty = f64::min(
                    orderbook.ps_best_ask_qty,
                    orderbook_depth.ps_best_ask_qty,
                );
                let ps_best_bid_qty = f64::min(
                    orderbook.ps_best_bid_qty,
                    orderbook_depth.ps_best_bid_qty,
                );
                let pp_best_ask_qty = f64::min(
                    orderbook.pp_best_ask_qty,
                    orderbook_depth.pp_best_ask_qty,
                );
                let pp_best_bid_qty = f64::min(
                    orderbook.pp_best_bid_qty,
                    orderbook_depth.pp_best_bid_qty,
                );

                let trade_qty_spot = if is_open {
                    ps_best_ask_qty * constraint_percent_qty
                } else {
                    ps_best_bid_qty * constraint_percent_qty
                };

                let trade_qty_swap = if is_open {
                    pp_best_bid_qty * constraint_percent_qty
                } else {
                    pp_best_ask_qty * constraint_percent_qty
                };
                let mut trade_qty = f64::min(trade_qty_spot, trade_qty_swap);

                let spot_best_ask = orderbook.ps_best_ask;
                let spot_best_bid = orderbook.ps_best_bid;
                let swap_best_ask = orderbook.pp_best_ask;
                let swap_best_bid = orderbook.pp_best_bid;
  
                let unit_price_spot = if is_open {
                    spot_best_ask
                } else {
                    spot_best_bid
                };

                let unit_price_swap = if is_open {
                    swap_best_bid
                } else {
                    swap_best_ask
                };
                println!("###############Basic Order Qty Calculator: trade_qty={:?}, unit_price_spot={:?} and unit_price_swap={:?}", trade_qty, unit_price_spot, unit_price_swap);

                if is_open {
                    let mut spot_delta = 0.0;
                    {
                        let mut local_deltas_spot = LOCAL_DELTAS_SPOT.lock().await;
                        if let Some(v) = local_deltas_spot.get(&spot_inst_id) {
                            spot_delta = *v;
                        } else {
                            continue;
                        }
                    }
                    let open_qty_max = match threshold_2_number {
                        Some(num) => num,
                        None => {
                            continue;
                        }
                    };

                    if trade_qty + spot_delta > open_qty_max { // Maximum order quantity constraint
                        trade_qty = open_qty_max - spot_delta
                    }
                    
                    
                    let open2_max_volume = max_open_value;
                    if trade_qty * unit_price_spot > open2_max_volume { // Maximum open price constraint
                        trade_qty = open2_max_volume / unit_price_spot
                    }

                    let total_price_spot = trade_qty * unit_price_spot;
                    let total_price_swap = trade_qty * unit_price_swap;
                    let trade_value = f64::min(total_price_spot, total_price_swap);
                    let constraint_percent_usdt = 0.9;
                    let constraint_value = balance_usdt.to_f64().unwrap() * constraint_percent_usdt;
                    if trade_value > constraint_value {
                        let unit_price_max = f64::max(unit_price_spot, unit_price_swap);
                        trade_qty = constraint_value / unit_price_max;
                    }
                    println!("###############Basic Order Qty Adjusting for OPEN2: spot_delta={:?}, open_qty_max={:?}, trade_value={:?}, constraint_value={:?} and trade_qty={:?}", spot_delta, open_qty_max, trade_value, constraint_value, trade_qty);
                }

                let mut trade_qty_decimal = Decimal::from_f64(trade_qty).unwrap();
                if is_close {
                    if position_spot.to_f64().unwrap() <= 0.0 {
                        continue;
                    }
                    if trade_qty * unit_price_spot > max_close_value // Maximum close price constraint
                    {
                        trade_qty_decimal =
                            Decimal::from_f64(max_close_value / unit_price_spot).unwrap();
                    }
                    
                    if position_spot.to_f64().unwrap() < trade_qty_decimal.to_f64().unwrap() { // Maximum position constraint
                        trade_qty_decimal = position_spot;
                    }
                    if trade_qty_decimal < spot_min_size { //skip when spot_min_sz is not saisfied
                        continue;
                    }
                    println!("###############Basic Order Qty Adjusting for CLOSE2: trade_qty_decimal={:?}", trade_qty_decimal); 
                }

                
                trade_qty_decimal = Decimal::new(1, 0); //TODO: set by hard code

                if trade_qty_decimal.to_f64().unwrap() <= 0.0 { // Check if quantity is valid
                    continue;
                }
                trade_qty_decimal = (trade_qty_decimal / lot_size).floor() * lot_size;
                trade_qty_decimal = Decimal::new(1, 0); //TODO: set by hard code
                if trade_qty_decimal < spot_min_size // Lot size constraint
                    || trade_qty_decimal < swap_min_size
                {
                    continue;
                }

                println!(
                    "###############Final Order Calculator: inst_id={:?}, trade_qty={:?}, All checking passes!",
                    spot_inst_id, trade_qty_decimal
                );

                
                // let timestamp = get_timestamp(SystemTime::now()).unwrap();
                let nanoid = generate_nanoid();

                if is_open { // Spot: BUY, Future: SELL
                    let order_spot = Order { 
                        id: spot_generate_client_order_id(&OperationType::Open2, &nanoid),
                        side: "buy".to_string(),
                        inst_id: spot_inst_id,
                        tdMode: "cross".to_string(),
                        ordType: "market".to_string(),
                        sz: trade_qty_decimal.to_string(),
                        // sz: "1.0".to_string(),
                    };
                    let order_swap = Order {
                        id: futures_generate_client_order_id(&OperationType::Open2, &nanoid),
                        side: "sell".to_string(),
                        inst_id: swap_inst_id,
                        tdMode: "cross".to_string(),
                        ordType: "market".to_string(),
                        sz: trade_qty_decimal.to_string(),
                    };
                    tx_compute.send((order_spot, order_swap)).await.unwrap();

                } else {
                    let order_spot = Order {
                        id: spot_generate_client_order_id(&OperationType::Close2, &nanoid),
                        side: "sell".to_string(),
                        inst_id: spot_inst_id,
                        tdMode: "cross".to_string(),
                        ordType: "market".to_string(),
                        sz: trade_qty_decimal.to_string(),
                    };
                    let order_swap = Order {
                        id: futures_generate_client_order_id(&OperationType::Close2, &nanoid),
                        side: "buy".to_string(),
                        inst_id: swap_inst_id,
                        tdMode: "cross".to_string(),
                        ordType: "market".to_string(),
                        sz: trade_qty_decimal.to_string(),
                    };
                    tx_compute.send((order_spot, order_swap)).await.unwrap();
                }
            }
        }
    });

    /*
     * Order Engine
     * Processing order request from compute engine
     */
    let order_handle =  tokio::spawn(async move {
        while let Some(msg) = rx_order.recv().await {
            let order_spot = msg.0;
            let order_swap = msg.1;
            println!("$$$$$$$$$$$$$$$Order Engine: recv order_spot is {:?}, order_swap is {:?}", order_spot, order_swap);
            let _ = write_order.send(order_spot.subscribe_message().into()).await;
            let _ = write_order.send(order_swap.subscribe_message().into()).await;
        }
    });

    let order_recv_handle =  tokio::spawn(async move {
        loop {
            let msg = match read_order.next().await {
                Some(Ok(Message::Text(msg))) => msg,
                Some(Err(err)) => {
                    panic!("{:?}", err);
                }
                _ => continue,
            };
            println!("$$$$$$$$$$$$$$$Order Recv {}", msg);
        }
    });

    books_channel_handle.await.unwrap();
    compute_handle.await.unwrap();
    account_channel_handle.await.unwrap();
    order_handle.await.unwrap();
    order_recv_handle.await.unwrap();
}

