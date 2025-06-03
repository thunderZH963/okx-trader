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

use globals::*;
use msg_engine::*;
use redis_subscribe::redis_subscribe;
use core::core_compute;
use rest::getInstruments;
/*
 * importing okx rust API
 */
use okx_rs::api::{Production, DemoTrading, OKXEnv};
use okx_rs::api::v5::{BalanceAndPositionChannel, InstrumentType, OrdersInfoChannel};
use okx_rs::api::Options;
use okx_rs::websocket::OKXAuth;
use okx_rs::websocket::WebsocketChannel;
use okx_rs::websocket::conn::{Books5, BboTbt, Instruments, Order};

/*
 * importing std or external well-known libs
 */
use dotenv; //env loader
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};
use log4rs::init_file;
use log::*;

/*
 * Define for Production(0) or DemoTrading(1) 
 */
static TEST: i32 = 0; //TODO: 1 is not okay?


#[tokio::main]
async fn main() {
    /*
     * Some Necessary Initialization
     */
    dotenv::dotenv().ok();
    init_file("log4rs.yml", Default::default()).unwrap(); // for Production
    // env_logger::init(); // for debug by develpers
    let key = std::env::var("OKX_API_KEY").unwrap();
    let secret = std::env::var("OKX_API_SECRET").unwrap();
    let passphrase = std::env::var("OKX_API_PASSPHRASE").unwrap();

    /*
     * Some Necessary Values Initialization
     */
    let spot_inst_ids = vec!["KAITO-USDT",]; //现货类型 //TODO: Maintain it for new symbols
    let swap_inst_ids = vec!["KAITO-USDT-SWAP", ]; //合约类型 //TODO: Maintain it for new symbols
    init_ccy2bal(spot_inst_ids.clone()).await;
    init_ccy2bal(swap_inst_ids.clone()).await;
    {
        let mut local_deltas_spot = LOCAL_DELTAS_SPOT.lock().await; //FIX: 记录更新交易的spot的开仓量的变量(两次策略更新之间), update its logic
        for id in spot_inst_ids.clone() {
            local_deltas_spot.insert(id.to_string(), 0.0);
        }
    }
    let (tx_books, mut rx_compute) = mpsc::channel::<String>(32); // a thread communication channel for books_handle and compute_handle
    let (tx_compute, mut rx_order) = mpsc::channel::<(Order, Order)>(32); // a thread communication channel for compute_handle and order_handle


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
    let mut options_private = Options::new_with(Production, key.clone(), secret.clone(), passphrase.clone());
    let (mut client_private, mut response_private) = connect_async(Production.private_websocket()).await.unwrap();
    let (mut write_private, mut read_private) = client_private.split();
    let auth_msg_private = OKXAuth::ws_auth(options_private).unwrap();
    write_private.send(auth_msg_private.into()).await.unwrap();
    let auth_resp_private = read_private.next().await.unwrap();
    info!("A private websocket channel auth: {:?}", auth_resp_private);
    let _ = write_private.send(BalanceAndPositionChannel.subscribe_message().into()).await;
    let account_channel_handle = tokio::spawn(async move {
        loop {
            let msg = read_private.next().await.unwrap();
            let parsed_msg: Value = match msg {
                Ok(msg) => {
                    let json_str = if let Message::Text(txt) = msg {
                        txt
                    } else {
                        panic!("BalanceAndPositionChannel expected a text message, {}", msg)
                    };
                    if json_str == "pong" {
                        println!("BalanceAndPositionChannel received a pong message, Alive!");
                        continue;
                    }
                    serde_json::from_str(&json_str).expect("Failed to parse JSON")
                }
                Err(_) => panic!("Error receiving message"),
            };
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
    });

    /*
     * 下单WebSocket
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
    info!("A private order websocket channel auth: {:?}", auth_resp_order);

    /*
     * 查询订单信息WebSocket
     * A private websocket channel initialization for orders info
     */
    let mut options_order_info = Options::new_with(Production, key.clone(), secret.clone(), passphrase.clone());
    let (mut client_order_info, mut response_order_info) = connect_async(Production.private_websocket()).await.unwrap();
    if TEST == 1 {
        options_order_info = Options::new_with(DemoTrading, key.clone(), secret.clone(), passphrase.clone());
        (client_order_info, response_order_info) = connect_async(DemoTrading.private_websocket()).await.unwrap();
    }
    let (mut write_order_info, mut read_order_info) = client_order_info.split();
    let auth_msg_order_info = OKXAuth::ws_auth(options_order_info).unwrap();
    write_order_info.send(auth_msg_order_info.into()).await.unwrap();    
    let auth_resp_order_info = read_order_info.next().await.unwrap();
    info!("A private order_info websocket channel auth: {:?}", auth_resp_order_info);
    for inst_id in spot_inst_ids.clone() { 
        let orders_info = OrdersInfoChannel {
            inst_id: String::from(inst_id),
            inst_type: InstrumentType::Spot,
        };
        let _ = write_order_info.send(orders_info.subscribe_message().into()).await;
    }
    for inst_id in swap_inst_ids.clone() {
        let orders_info = OrdersInfoChannel {
            inst_id: String::from(inst_id),
            inst_type: InstrumentType::Swap,
        };
        let _ = write_order_info.send(orders_info.subscribe_message().into()).await;
    }
    let orders_info_channel_handle = tokio::spawn(async move {
        loop {
            let msg = read_order_info.next().await.unwrap();
            let parsed_msg: Value = match msg {
                Ok(msg) => {
                    let json_str = if let Message::Text(txt) = msg {
                        txt
                    } else {
                        panic!("OrdersInfoChannel expected a text message")
                    };
                    if json_str == "pong" {
                        println!("OrdersInfoChannel received a pong message, Alive!");
                        continue;
                    }
                    serde_json::from_str(&json_str).expect("Failed to parse JSON")
                }
                Err(_) => panic!("Error receiving message {:?}", msg),
            };
            info!("!!!!!!!!!!!!!!!Orders Update {}", parsed_msg);
        }
    });

     /*
     * 盘口数据查询WebSocket
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
    info!("A public websocket channel auth: {:?}", auth_resp);
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
     * 查询instruments信息Rest
     * Rest API for GET instruments full data
     */
    getInstruments(spot_inst_ids.clone(), swap_inst_ids.clone()).await;

    /*
     * Books/Instruments handler
     * Handling depth/best ask/bids data and instruments updating
     */
    let books_channel_handle = tokio::spawn(async move {
        // for n in 1..15 { // TODO: modify to loop later
        loop {
            let msg = read.next().await.unwrap();
            let parsed_msg: Value = match msg {
                Ok(msg) => {
                    let json_str = if let Message::Text(txt) = msg {
                        txt
                    } else {
                        panic!("Expected a text message")
                    };
                    if json_str == "pong" {
                        continue;
                    }
                    
                    serde_json::from_str(&json_str).expect("Failed to parse JSON")
                }
                Err(_) => panic!("Error receiving message"),
            };
            let channel = parsed_msg["arg"]["channel"].as_str().unwrap_or("Unknown").to_string();
            let inst_id = parsed_msg["arg"]["instId"].as_str().unwrap_or("Unknown").to_string();
            let data = &parsed_msg["data"][0];
            if data.is_null() {
                info!("***************Books/Instruments handler reveives first msg with non-data {:?}", parsed_msg);
            } else {
                if channel == "instruments" {
                    let inst_type = parsed_msg["arg"]["instType"].as_str().unwrap_or("Unknown").to_string();
                    info!("***************Instruments handler handles update msg: inst_type {:?}", inst_type);
                    process_instruments_message(&parsed_msg["data"]).await;
                } else if channel == "books5" {
                    info!("***************Books handler handles BOOKS5 update msg: inst_id {:?}", inst_id);
                    process_books5_message(inst_id.clone(), data).await;
                    tx_books.send(inst_id.clone()).await.unwrap();
                } else if channel == "bbo-tbt" {
                    info!("***************Books handler handles BBO-TBT update msg: inst_id {:?}", inst_id);
                    process_bbotbt_message(inst_id.clone(), data).await;
                    tx_books.send(inst_id.clone()).await.unwrap();
                } else {
                    panic!("***************Books/Instruments handler receives msg from unknown channel: {:?}", parsed_msg);
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
        while let Some(msg) = rx_compute.recv().await {
            match core_compute(msg).await {
                Some((order_spot, order_swap)) => {
                    tx_compute.send((order_spot, order_swap)).await.unwrap();
                },
                None => {
                    info!("###############Order Book Calculator: No orders were created.");
                }
            }
        }
            
    });

    /*
     * Order Engine
     * Processing order request from compute engine
     */
    let order_handle = tokio::spawn(async move {
        loop {
            // Use `timeout` to wait for a message or timeout after 20 seconds
            let msg = timeout(Duration::from_secs(*PING_TIMEOUT), rx_order.recv()).await;
    
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
    
                    info!("$$$$$$$$$$$$$$$Order Engine: recv order_spot is {:?}, order_swap is {:?}", order_spot.clone(), order_swap);
    
                    {
                        let mut inst_state_map = INST_STATE_MAP.write().await;
                        inst_state_map.insert(order_spot.clone().inst_id.to_string(), order_spot.clone().id);
                        inst_state_map.insert(order_swap.clone().inst_id.to_string(), order_swap.clone().id);
    
                        let mut orderid2inst = ORDERID2INST.write().await;
                        orderid2inst.insert(order_spot.clone().id, order_spot.clone().inst_id.to_string());
                        orderid2inst.insert(order_swap.clone().id, order_swap.clone().inst_id.to_string());
    
                        info!("$$$$$$$$$$$$$$Update global set {:?} {:?} 开始下单", inst_state_map, orderid2inst);
                    }
    
                    let _ = write_order.send(order_spot.clone().subscribe_message().into()).await; // Send messages to `write_order`
                    let _ = write_order.send(order_swap.clone().subscribe_message().into()).await; // Send messages to `write_order`
                },
                Ok(None) => {
                    // This means `rx_order.recv()` returned `None`, i.e., the receiver has been closed.
                    error!("$$$$$$$$$$$$$$ rx_order return None, the receiver has been closed.");
                    break;
                },
                Err(_) => {
                    // Timeout happened after 20 seconds
                    info!("$$$$$$$$$$$$$$Order Engine: No order received for 20 seconds, sending ping message.");
                    let _ = write_order.send("ping".into()).await;
                },
            }
        }
    });
    

    let order_recv_handle =  tokio::spawn(async move {
        loop {
            let msg = timeout(Duration::from_secs(*PING_TIMEOUT), read_order.next()).await;
            match msg {
                Ok(Some(Ok(ws_msg))) => {
                    match ws_msg {
                        Message::Text(text) => {
                            info!("$$$$$$$$$$$$$$$Order Recv msg {:?}", text);
                            if text == "pong" {
                                println!("Order Recv Channel received a pong message, Alive!");
                                continue;
                            }
                            let parsed_msg: Value = serde_json::from_str(&text).expect("Failed to parse JSON");
                
                            let id = parsed_msg["id"].as_str().unwrap_or("Unknown").to_string();
                            {
                                let orderid2inst = ORDERID2INST.read().await;
                                let mut inst_id = match orderid2inst.get(&id) {
                                    Some(inst) => inst.to_string(),
                                    None => String::new(),
                                };
                                let mut inst_state_map = INST_STATE_MAP.write().await;
                                // inst_state_map.remove(&inst_id); //TODO: only allow trade one time
                                info!("$$$$$$$$$$$$$$$Order Recv: Update inst_id state map {:?} order map {:?}", inst_state_map, orderid2inst);
                            }
                        }
                        other => {
                            info!("Received non-text WebSocket message: {:?}", other);
                        }
                    }
                }
                Ok(Some(Err(e))) => {
                    error!("WebSocket error: {:?}", e);
                }
                Ok(None) => {
                    error!("$$$$$$$$$$$$$$ Order Recv Channel: return None, the receiver has been closed.");
                    break;
                }
                Err(_) => {
                    info!("$$$$$$$$$$$$$$ Order Recv Channel: No order received for 20 seconds.");
                }
            }  
        }
    });

    /*
     * Timeout Control to ping each ws
     */
    let ping_handle = tokio::spawn(async move {
        loop {
            let _ = write.send("ping".into()).await;
            let _ = write_order_info.send("ping".into()).await;
            let _ = write_private.send("ping".into()).await;
            sleep(Duration::from_secs(*PING_TIMEOUT)).await;
        }
       
    });

    books_channel_handle.await.unwrap();
    compute_handle.await.unwrap();
    account_channel_handle.await.unwrap();
    order_handle.await.unwrap();
    order_recv_handle.await.unwrap();
    orders_info_channel_handle.await.unwrap();
    ping_handle.await.unwrap();
    redis_handle.await.unwrap();
}

