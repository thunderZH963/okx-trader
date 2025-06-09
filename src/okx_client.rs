use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use futures_util::StreamExt;
use futures_util::SinkExt;
use log::info;
use okx_rs::api::{Production, OKXEnv};
use okx_rs::api::v5::{InstrumentType, OrdersInfoChannel};
use okx_rs::api::Options;
use okx_rs::websocket::conn::BboTbt;
use okx_rs::websocket::conn::Books5;
use okx_rs::websocket::conn::Instruments;
use okx_rs::websocket::OKXAuth;
use okx_rs::websocket::WebsocketChannel;
use tokio::net::TcpStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;

pub async fn connect_okx_order_info(key: String, secret: String, passphrase: String, spot_inst_ids: Vec<&str>, swap_inst_ids: Vec<&str>) -> (SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>, SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>){
    let mut options_order_info = Options::new_with(Production, key.clone(), secret.clone(), passphrase.clone());
    let (mut client_order_info, mut response_order_info) = connect_async(Production.private_websocket()).await.unwrap();
    let (mut write_order_info, mut read_order_info) = client_order_info.split();
    let auth_msg_order_info = OKXAuth::ws_auth(options_order_info).unwrap();
    write_order_info.send(auth_msg_order_info.into()).await.unwrap();    
    let auth_resp_order_info = read_order_info.next().await.unwrap();
    println!("A private order_info websocket channel auth: {:?}", auth_resp_order_info);
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
    info!("Connecting or Reconnecting to OKX order info websocket channel.");
    (write_order_info, read_order_info)
}

pub async fn connect_okx_books5(key: String, secret: String, passphrase: String, spot_inst_ids: Vec<&str>, swap_inst_ids: Vec<&str>) -> (SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>, SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>) {
    let mut options_books5 = Options::new_with(Production, key.clone(), secret.clone(), passphrase.clone());
    let (mut client_books5, mut response_books5) = connect_async(Production.public_websocket()).await.unwrap();
    let (mut write_books5, mut read_books5) = client_books5.split();
    let auth_msg_books5 = OKXAuth::ws_auth(options_books5).unwrap();
    write_books5.send(auth_msg_books5.into()).await.unwrap();    
    let auth_resp_books5 = read_books5.next().await.unwrap();
    println!("A public websocket books5 channel auth: {:?}", auth_resp_books5);
    for inst_id in spot_inst_ids.clone() { 
        let books = Books5 {
            inst_id: String::from(inst_id),
        };
        let _ = write_books5.send(books.subscribe_message().into()).await;
    }
    for inst_id in swap_inst_ids.clone() {
        let books = Books5 {
            inst_id: String::from(inst_id),
        };
        let _ = write_books5.send(books.subscribe_message().into()).await;
    }
    let instruments = Instruments {
        instType: "SPOT".to_string(),
    };
    let _ = write_books5.send(instruments.subscribe_message().into()).await; //订阅spot的instruments数据(只推送增量)
    let instruments = Instruments {
        instType: "SWAP".to_string(),
    };
    let _ = write_books5.send(instruments.subscribe_message().into()).await; //订阅swap的instruments数据(只推送增量)
    info!("Connecting or Reconnecting to OKX books5 websocket channel.");
    (write_books5, read_books5)
}

pub async fn connect_okx_books_tbt(key: String, secret: String, passphrase: String, spot_inst_ids: Vec<&str>, swap_inst_ids: Vec<&str>) -> (SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>, SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>) {
    let mut options_books_tbt = Options::new_with(Production, key.clone(), secret.clone(), passphrase.clone());
    let (mut client_books_tbt, mut response_books_tbt) = connect_async(Production.public_websocket()).await.unwrap();
    let (mut write_books_tbt, mut read_books_tbt) = client_books_tbt.split();
    let auth_msg_books_tbt = OKXAuth::ws_auth(options_books_tbt).unwrap();
    write_books_tbt.send(auth_msg_books_tbt.into()).await.unwrap();    
    let auth_resp_books_tbt = read_books_tbt.next().await.unwrap();
    println!("A public websocket books_tbt channel auth: {:?}", auth_resp_books_tbt);

    for inst_id in spot_inst_ids.clone() {
        let books_tbt = BboTbt {
            inst_id: String::from(inst_id),
        };
        let _ = write_books_tbt.send(books_tbt.subscribe_message().into()).await;
    }
    for inst_id in swap_inst_ids.clone() { 
        let books_tbt = BboTbt {
            inst_id: String::from(inst_id),
        };
        let _ = write_books_tbt.send(books_tbt.subscribe_message().into()).await;
    }
    info!("Connecting or Reconnecting to OKX books_tbt websocket channel.");
    (write_books_tbt, read_books_tbt)
}