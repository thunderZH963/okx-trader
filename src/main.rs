/*
 * importing self-developed external libs
 */
mod utils;
mod models;
mod globals;
mod msg_engine;
mod compute_engine;
mod redis_subscribe;
mod core;
mod rest;
mod okx_client;

use globals::*;
use msg_engine::*;
use redis_subscribe::redis_subscribe;
use core::core_compute;
use rest::getInstruments;
use utils::{get_timestamp, read_symbols};
/*
 * importing okx rust API
 */
use okx_rs::websocket::WebsocketChannel;
use okx_rs::websocket::conn::Order;

/*
 * importing std or external well-known libs
 */
use dotenv; //env loader
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value};
use tokio_tungstenite:: tungstenite::protocol::Message;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};
use log4rs::init_file;
use log::*;

use crate::okx_client::{connect_okx_account, connect_okx_books5, connect_okx_books_tbt, connect_okx_order, connect_okx_order_info};

#[tokio::main]
async fn main() {
    /*
     * Some Necessary Initialization
     */
    dotenv::dotenv().ok();
    init_file("log4rs.yml", Default::default()).unwrap(); // for Production
    // env_logger::init(); // for debug by develpers

    /*
     * Some Necessary Values Initialization
     */
    let file_path = "./src/symbol_spot.txt";
    let mut spot_inst_ids: Vec<&'static str> = vec![]; //现货类型 //TODO: Maintain it for new symbols
    match read_symbols(file_path) {
        Ok(symbols) => {
            for symbol in symbols {
                spot_inst_ids.push(Box::leak(symbol.into_boxed_str()));  // 将 String 转换为 &'static str
            }
        },
        Err(e) => eprintln!("Error reading file: {}", e),
    }

    let file_path = "./src/symbol_swap.txt";
    let mut swap_inst_ids: Vec<&'static str> = vec![]; //现货类型 //TODO: Maintain it for new symbols
    match read_symbols(file_path) {
        Ok(symbols) => {
            for symbol in symbols {
                swap_inst_ids.push(Box::leak(symbol.into_boxed_str()));  // 将 String 转换为 &'static str
            }
        },
        Err(e) => eprintln!("Error reading file: {}", e),
    }
    let spot_inst_ids_clone = spot_inst_ids.clone();
    let swap_inst_ids_clone = swap_inst_ids.clone();
    let spot_inst_ids_clone1 = spot_inst_ids.clone();
    let swap_inst_ids_clone1= swap_inst_ids.clone();
    let spot_inst_ids_clone2 = spot_inst_ids.clone();
    let swap_inst_ids_clone2= swap_inst_ids.clone();
    info!("Init Spot Inst IDs: {:?}", spot_inst_ids);
    info!("Init Swap Inst IDs: {:?}", swap_inst_ids);
     
    init_ccy2bal(spot_inst_ids.clone()).await;
    init_ccy2bal(swap_inst_ids.clone()).await;
    {
        let mut local_deltas_spot = LOCAL_DELTAS_SPOT.lock().await; //FIX: 记录更新交易的spot的开仓量的变量(两次策略更新之间), update its logic
        for id in spot_inst_ids.clone() {
            local_deltas_spot.insert(id.to_string(), 0.0);
        }
    }
    let (tx_books, mut rx_compute) = mpsc::channel::<String>(100); // a thread communication channel for books_handle and compute_handle
    let (tx_books_tbt, mut rx_compute_tbt) = mpsc::channel::<String>(100); // a thread communication channel for books_handle and compute_handle
    let (tx_compute, mut rx_order) = mpsc::channel::<(Order, Order)>(100); // a thread communication channel for compute_handle and order_handle

    /*
     * Loading trade signals
     */
    // init_trade_signals().await;
    let redis_handle = tokio::spawn(async move {
        redis_subscribe().await;
    });

    /*
     * A private websocket channel initialization for account info
     */
    let account_channel_handle = tokio::spawn(async move {
        let (mut write_private, mut read_private) = connect_okx_account(key.clone(), secret.clone(), passphrase.clone()).await;
        let mut alive_interval = tokio::time::interval(Duration::from_secs(*PING_TIMEOUT));
        loop {
            tokio::select! {
                msg = timeout(Duration::from_secs(*PING_TIMEOUT), read_private.next()) => {
                    match msg {
                        Ok(Some(Ok(ws_msg))) => {
                            match ws_msg {
                                Message::Text(text) => {
                                    if text == "pong" {
                                        println!("Account Channel received a pong message, Alive!");
                                        continue;
                                    }
                                    let parsed_msg: Value = serde_json::from_str(&text).expect("Failed to parse JSON");
                                    let channel: String = parsed_msg["arg"]["channel"].as_str().unwrap_or("Unknown").to_string();
                                    let data = &parsed_msg["data"];
                                    if data.is_null() {
                                        info!("Receiving unprocessed msg from private websocket balance_and_position channel {:?}", parsed_msg);
                                    } else {
                                        if channel == "balance_and_position" {
                                            process_account_message(data).await;
                                        } else {
                                            panic!("Receiving unknown channel msg from private websocket balance_and_position channel {:?}", parsed_msg);
                                        }
                                    }
                                }
                                other => {
                                    info!("Account channel Received non-text WebSocket message: {:?}", other);
                                    (write_private, read_private) = connect_okx_account(key.clone(), secret.clone(), passphrase.clone()).await;
                                }
                            }
                        }
                        Ok(Some(Err(e))) => {
                            info!("Account WebSocket error: {:?}", e);
                            (write_private, read_private) = connect_okx_account(key.clone(), secret.clone(), passphrase.clone()).await;
                        }
                        Ok(None) => {
                            info!("OrdersInfoChannel Recv Channel: return None, the receiver has been closed.");
                            (write_private, read_private) = connect_okx_account(key.clone(), secret.clone(), passphrase.clone()).await;
                        }
                        Err(_) => {
                            println!("Account Recv Channel: No msgs received for {} seconds, ping it", *PING_TIMEOUT);
                            let _ = write_private.send("ping".into()).await;
                        }              
                    }
                }
                _ = alive_interval.tick() => {
                    println!("Account Recv Channel Timeout, sending ping message.");
                    let _ = write_private.send("ping".into()).await;
                }
                else => {
                    info!("Account not match");
                }
            }
        }
    });

    /*
     * 查询订单信息WebSocket
     * A private websocket channel initialization for orders info
     */
    let orders_info_channel_handle = tokio::spawn(async move {
        let (mut write_order_info, mut read_order_info) = connect_okx_order_info(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids.clone(), swap_inst_ids.clone()).await;
        let mut alive_interval = tokio::time::interval(Duration::from_secs(*PING_TIMEOUT));

        loop {
            tokio::select! {
                msg = timeout(Duration::from_secs(*PING_TIMEOUT), read_order_info.next()) => {
                    match msg {
                        Ok(Some(Ok(ws_msg))) => {
                            match ws_msg {
                                Message::Text(text) => {
                                    if text == "pong" {
                                        println!("OrdersInfoChannel Recv Channel received a pong message, Alive!");
                                        continue;
                                    }
                                    let parsed_msg: Value = serde_json::from_str(&text).expect("Failed to parse JSON");
                                    let instId = &parsed_msg["arg"]["instId"];
                                    let ordId = &parsed_msg["data"][0]["ordId"];
                                    let state = &parsed_msg["data"][0]["state"];
                                    let fillPx = &parsed_msg["data"][0]["fillPx"];
                                    let fillSz = &parsed_msg["data"][0]["fillSz"];
                                    let ctime = &parsed_msg["data"][0]["cTime"];
                                    let utime = &parsed_msg["data"][0]["uTime"];
                                    if state.is_null() {
                                        info!("!!!!!!!!!!!!!!!Orders Info Channel receives msg with unknown status {:?}", parsed_msg);
                                    } else if state == "partially_filled" {
                                        info!("<<<<<<<Orders Info Channel receives partially filled order: instId: {}, ordId: {}, fillPx: {}, fillSz: {}, ctime: {}, utime: {}", 
                                            instId, ordId, fillPx, fillSz, ctime, utime);
                                    } else if state == "filled" {
                                        info!("<<<<<<<Orders Info Channel receives filled order: instId: {}, ordId: {}, fillPx: {}, fillSz: {}, ctime: {}, utime: {}", 
                                            instId, ordId, fillPx, fillSz, ctime, utime);
                                    } else {
                                        info!("!!!!!!!!!!!!!!!Orders Info Channel receives msg with un-processed status {:?}", parsed_msg);
                                    }
                                }
                                other => {
                                    info!("OrdersInfoChannel Received non-text WebSocket message: {:?}", other);
                                    (write_order_info, read_order_info) = connect_okx_order_info(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids.clone(), swap_inst_ids.clone()).await;
                                }
                            }
                        }
                        Ok(Some(Err(e))) => {
                            info!("OrdersInfoChannel WebSocket error: {:?}", e);
                            (write_order_info, read_order_info) = connect_okx_order_info(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids.clone(), swap_inst_ids.clone()).await;
                        }
                        Ok(None) => {
                            info!("OrdersInfoChannel Recv Channel: return None, the receiver has been closed.");
                            (write_order_info, read_order_info) = connect_okx_order_info(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids.clone(), swap_inst_ids.clone()).await;
                        }
                        Err(_) => {
                            println!("OrdersInfoChannel Recv Channel: No msgs received for {} seconds, ping it", *PING_TIMEOUT);
                            let _ = write_order_info.send("ping".into()).await;
                        }              
                    }
                }
                _ = alive_interval.tick() => {
                    println!("OrdersInfoChannel Recv Channel Timeout, sending ping message.");
                    let _ = write_order_info.send("ping".into()).await;
                }
                else => {
                    info!("OrdersInfoChannel not match");
                }
            }
        }
    });

    /*
     * 查询instruments信息Rest
     * Rest API for GET instruments full data
     */
    getInstruments(spot_inst_ids_clone.clone(), swap_inst_ids_clone.clone()).await;

    /*
     * Books/Instruments handler
     * Handling depth/best ask/bids data and instruments updating
     */
    let books5_channel_handle = tokio::spawn(async move {
        let (mut write_books5, mut read_books5) = connect_okx_books5(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids_clone1.clone(), swap_inst_ids_clone1.clone()).await;

        let mut alive_interval = tokio::time::interval(Duration::from_secs(*PING_TIMEOUT));

        loop {
            tokio::select! {
                msg = timeout(Duration::from_secs(*PING_TIMEOUT), read_books5.next()) => {
                    match msg {
                        Ok(Some(Ok(ws_msg))) => {
                            match ws_msg {
                                Message::Text(text) => {
                                    if text == "pong" {
                                        println!("Books5/Instruments Recv Channel received a pong message, Alive!");
                                        continue;
                                    }
                                    let parsed_msg: Value = serde_json::from_str(&text).expect("Failed to parse JSON");
                                    let channel = parsed_msg["arg"]["channel"].as_str().unwrap_or("Unknown").to_string();
                                    let inst_id = parsed_msg["arg"]["instId"].as_str().unwrap_or("Unknown").to_string();
                                    let data = &parsed_msg["data"][0];
                                    if data.is_null() {
                                        info!("***************Books5/Instruments handler receives first msg with non-data {:?}", parsed_msg);
                                    } else {
                                        if channel == "instruments" {
                                            let inst_type = parsed_msg["arg"]["instType"].as_str().unwrap_or("Unknown").to_string();
                                            process_instruments_message(&parsed_msg["data"], spot_inst_ids_clone1.clone()).await;
                                        } else if channel == "books5" {
                                            process_books5_message(inst_id.clone(), data).await;
                                            tx_books.send(inst_id.clone()).await.unwrap();
                                        } else {
                                            panic!("***************Books5/Instruments handler receives msg from unknown channel: {:?}", parsed_msg);
                                        }
                                    }
                                }
                                other => { 
                                    info!("Books5/Instruments Received non-text WebSocket message: {:?}", other);
                                    (write_books5, read_books5) = connect_okx_books5(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids_clone1.clone(), swap_inst_ids_clone1.clone()).await;
                                }
                            }
                        }
                        Ok(Some(Err(e))) => {
                            info!("Books5/Instruments WebSocket error: {:?}", e);
                            (write_books5, read_books5) = connect_okx_books5(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids_clone1.clone(), swap_inst_ids_clone1.clone()).await;
                        }
                        Ok(None) => {
                            info!("Books5/Instruments Recv Channel: return None, the receiver has been closed.");
                            (write_books5, read_books5) = connect_okx_books5(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids_clone1.clone(), swap_inst_ids_clone1.clone()).await;
                        }
                        Err(_) => {
                            println!("Books5/Instruments Recv Channel: No msgs received for {} seconds, ping it", *PING_TIMEOUT);
                            let _ = write_books5.send("ping".into()).await;
                        }
                    }
                }
                _ = alive_interval.tick() => {
                    println!("Books5 Recv Channel Timeout, sending ping message.");
                    let _ = write_books5.send("ping".into()).await;
                }
                else => {
                    info!("books5 not match");
                }
            }
        }
    });


    let books_tbt_channel_handle = tokio::spawn(async move {
        let (mut write_books_tbt, mut read_books_tbt) = connect_okx_books_tbt(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids_clone2.clone(), swap_inst_ids_clone2.clone()).await;

        let mut alive_interval = tokio::time::interval(Duration::from_secs(*PING_TIMEOUT));

        loop {
            tokio::select! {
                msg = timeout(Duration::from_secs(*PING_TIMEOUT), read_books_tbt.next()) => {
                    match msg {
                        Ok(Some(Ok(ws_msg))) => {
                            match ws_msg {
                                Message::Text(text) => {
                                    if text == "pong" {
                                        println!("Books_tbt Recv Channel received a pong message, Alive!");
                                        continue;
                                    }
                                    let parsed_msg: Value = serde_json::from_str(&text).expect("Failed to parse JSON");
                                    let channel = parsed_msg["arg"]["channel"].as_str().unwrap_or("Unknown").to_string();
                                    let inst_id = parsed_msg["arg"]["instId"].as_str().unwrap_or("Unknown").to_string();
                                    let data = &parsed_msg["data"][0];
                                    if data.is_null() {
                                        info!("***************Books_tbt handler receives first msg with non-data {:?}", parsed_msg);
                                    } else {
                                        if channel == "bbo-tbt" {
                                            process_bbotbt_message(inst_id.clone(), data).await;
                                            tx_books_tbt.send(inst_id.clone()).await.unwrap();
                                        } else {
                                            panic!("***************Books/Instruments handler receives msg from unknown channel: {:?}", parsed_msg);
                                        }
                                    }
                                }
                                other => {
                                    info!("Books_tbt Received non-text WebSocket message: {:?}", other);
                                    (write_books_tbt, read_books_tbt) = connect_okx_books_tbt(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids_clone2.clone(), swap_inst_ids_clone2.clone()).await;
                                }
                            }
                        }
                        Ok(Some(Err(e))) => {
                            info!("Books_tbt WebSocket error: {:?}", e);
                            (write_books_tbt, read_books_tbt) = connect_okx_books_tbt(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids_clone2.clone(), swap_inst_ids_clone2.clone()).await;
                        }
                        Ok(None) => {
                            info!("Books_tbt Recv Channel: return None, the receiver has been closed.");
                            (write_books_tbt, read_books_tbt) = connect_okx_books_tbt(key.clone(), secret.clone(), passphrase.clone(), spot_inst_ids_clone2.clone(), swap_inst_ids_clone2.clone()).await;
                        }
                        Err(_) => {
                            println!("Books_tbt Recv Channel: No msgs received for {} seconds, ping it", *PING_TIMEOUT);
                            let _ = write_books_tbt.send("ping".into()).await;
                        }              
                    } 
                }
                _ = alive_interval.tick() => {
                    // This block will execute every 20 seconds
                    println!("Books_tbt Recv Channel Timeout, sending ping message.");
                    let _ = write_books_tbt.send("ping".into()).await;
                }
                else => {
                    info!("books_tbt not match");
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
        loop {
            tokio::select! {
                Some(msg) = rx_compute.recv() => {
                    // println!("Compute Engine non-tbt: Received msg: {:?}", msg);
                    match core_compute(msg).await {
                        Some((order_spot, order_swap)) => {
                            tx_compute.send((order_spot, order_swap)).await.unwrap();
                        },
                        None => {
                            // info!("###############Order Book Calculator: No orders were created.");
                        }
                    }
                },
                Some(msg) = rx_compute_tbt.recv() => {
                    // println!("Compute Engine tbt: Received msg: {:?}", msg);
                    match core_compute(msg).await {
                        Some((order_spot, order_swap)) => {
                            tx_compute.send((order_spot, order_swap)).await.unwrap();
                        },
                        None => {
                            // info!("###############Order Book Calculator: No orders were created.");
                        }
                    }
                },
                else => {
                    info!("compute engine not match");
            },
            }
        }
            
    });
    
    /*
     * Order Engine
     * Processing order request from compute engine
     */
    let order_handle = tokio::spawn(async move {
        let (mut write_order, mut read_order) = connect_okx_order(key.clone(), secret.clone(), passphrase.clone()).await;
        let mut alive_interval = tokio::time::interval(Duration::from_secs(*PING_TIMEOUT));
        loop {
            tokio::select! {
                // Use `timeout` to wait for a message or timeout after 20 seconds
                msg = timeout(Duration::from_secs(*PING_TIMEOUT), rx_order.recv())=> {
                    match msg {
                        Ok(Some(msg)) => {
                            let order_spot = msg.0;
                            let order_swap = msg.1;
                            {
                                let inst_state_map = INST_STATE_MAP.read().await;
                                if inst_state_map.contains_key(&(order_spot.clone().inst_id.to_string())) || inst_state_map.contains_key(&(order_swap.clone().inst_id.to_string())) {
                                    info!("$$$$$$$$$$$$$$Skipping this inst_id in order_handle{:?} {:?}", order_spot.inst_id.to_string(), inst_state_map);
                                    continue;
                                }
                            }
            
                            // info!("$$$$$$$$$$$$$$$Order Engine: recv order_spot is {:?}, order_swap is {:?}", order_spot.clone(), order_swap);
            
                            {
                                let mut inst_state_map = INST_STATE_MAP.write().await;
                                inst_state_map.insert(order_spot.clone().inst_id.to_string(), order_spot.clone().id);
                                inst_state_map.insert(order_swap.clone().inst_id.to_string(), order_swap.clone().id);
            
                                let mut orderid2inst = ORDERID2INST.write().await;
                                orderid2inst.insert(order_spot.clone().id, order_spot.clone().inst_id.to_string());
                                orderid2inst.insert(order_swap.clone().id, order_swap.clone().inst_id.to_string());
            
                                // info!("$$$$$$$$$$$$$$Update global set {:?} {:?} 开始下单", inst_state_map, orderid2inst);
                            }
                            info!("<<<<<<<processing spot and swap order: spot_client_id: {:?}, swap_client_id: {:?}", 
                                order_spot.clone().id, order_swap.clone().id);
                            info!("<<<<<<<send order_spot: clientId: {:?}, instId: {:?}, sz: {:?}, price: {:?}, timestamp: {:?}, threshold: {:?}", 
                                order_spot.clone().id, order_spot.clone().inst_id, order_spot.clone().sz, order_spot.clone().price, get_timestamp(), order_spot.clone().threshold);
                            let _ = write_order.send(order_spot.clone().subscribe_message().into()).await;
                            info!("<<<<<<<send order_swap: clientId: {:?}, instId: {:?}, sz: {:?}, price: {:?}, timestamp: {:?}, threshold: {:?}", 
                                order_swap.clone().id, order_swap.clone().inst_id, order_swap.clone().sz, order_swap.clone().price, get_timestamp(), order_swap.clone().threshold);
                            // info!("[DEBUG] {}", order_swap_info);
                            let _ = write_order.send(order_swap.clone().subscribe_message().into()).await;
                            //let _ = write_order.send("ping".into()).await; //TODO: remove later
                        },
                        Ok(None) => {
                            // This means `rx_order.recv()` returned `None`, i.e., the receiver has been closed.
                            panic!("$$$$$$$$$$$$$$ rx_order return None, the receiver has been closed.");
                        },
                        Err(_) => {
                            // Timeout happened after 20 seconds
                            println!("$$$$$$$$$$$$$$Order Engine: No order received for {} seconds, sending ping message.", *PING_TIMEOUT);
                            let _ = write_order.send("ping".into()).await;
                        },
                    }
                }
                msg = timeout(Duration::from_secs(*PING_TIMEOUT), read_order.next()) => {
                    match msg {
                        Ok(Some(Ok(ws_msg))) => {
                            match ws_msg {
                                Message::Text(text) => {
                                    if text == "pong" {
                                        println!("Order Recv Channel received a pong message, Alive!");
                                        continue;
                                    }
                                    let parsed_msg: Value = serde_json::from_str(&text).expect("Failed to parse JSON");
                                    let clientId = &parsed_msg["id"];
                                    let ts = &parsed_msg["data"][0]["ts"];
                                    let orderId = &parsed_msg["data"][0]["ordId"];
                                    info!("<<<<<<<Order successfully placed: clientId: {}, ts: {}, orderId: {}, msg is {}", 
                                        clientId, ts, orderId, &parsed_msg["data"][0]["sMsg"]);
                                    // info!("[DEBUG] msg is {}", parsed_msg);
                                    info!("$$$$$$$$$$$$$$Order Channel: recv order msg: {:?}", parsed_msg);
                                    {
                                        let orderid2inst = ORDERID2INST.read().await;
                                        let inst_id = if let Some(client_id_str) = clientId.as_str() {
                                            orderid2inst.get(&(client_id_str.to_string())).unwrap()
                                        } else {
                                            // 处理非字符串类型
                                            panic!("Error: clientId is not a string");
                                        };
                                        let mut inst_state_map = INST_STATE_MAP.write().await;
                                        inst_state_map.remove(inst_id); //TODO: only allow trade one time
                                    }
                                    {
                                        let mut orderid2inst = ORDERID2INST.write().await;
                                        orderid2inst.remove(&clientId.to_string());
                                    }
                                }
                                other => {
                                    info!("$$$$$$$$$$$$$$ Order Recv Channel: received non-text WebSocket message: {:?}", other);
                                    (write_order, read_order) = connect_okx_order(key.clone(), secret.clone(), passphrase.clone()).await;
                                }
                            }
                        }
                        Ok(Some(Err(e))) => {
                            info!("$$$$$$$$$$$$$$ Order Recv Channel: WebSocket error: {:?}", e);
                            (write_order, read_order) = connect_okx_order(key.clone(), secret.clone(), passphrase.clone()).await;
                        }
                        Ok(None) => {
                            info!("$$$$$$$$$$$$$$ Order Recv Channel: return None, the receiver has been closed.");
                            (write_order, read_order) = connect_okx_order(key.clone(), secret.clone(), passphrase.clone()).await;
                        }
                        Err(_) => {
                            info!("$$$$$$$$$$$$$$ Order Recv Channel: No order received for timeout, ping");
                            let _ = write_order.send("ping".into()).await;
                        }
                    }
                }
                _ = alive_interval.tick() => {
                    println!("Order Recv Channel Alive");
                }
            }
        }
    });

    books5_channel_handle.await.unwrap();
    books_tbt_channel_handle.await.unwrap();
    compute_handle.await.unwrap();
    account_channel_handle.await.unwrap();
    order_handle.await.unwrap();
    orders_info_channel_handle.await.unwrap();
    redis_handle.await.unwrap();
}

