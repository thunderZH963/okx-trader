use okx_rs::api::v5::ws_convert::TryParseEvent;
use okx_rs::api::OKXEnv;
use okx_rs::api::Production;
use okx_rs::websocket::conn::Books5;
use okx_rs::websocket::WebsocketChannel;

fn main() {
    let (mut client, response) = tungstenite::connect(Production.public_websocket()).unwrap();
    info!("Connected to the server");
    info!("Response HTTP code: {}", response.status());
    info!("Response contains the following headers:");
    info!("{:?}", response.headers());

    let symbols = vec!["BTC-USDT-SWAP", "BTC-USDT", "ETH-USDT-SWAP", "ETH-USDT"];

    for symbol in symbols {
        let channel = Books5 {
            inst_id: symbol.into(),
        };
        client.send(channel.subscribe_message().into()).unwrap();
    }

    loop {
        let msg = client.read().unwrap();
        let data = msg.into_text().unwrap();

        match Books5::try_parse(&data) {
            Ok(Some(resp)) => match resp.data {
                Some([book_update, ..]) => {
                    info!("book_update: {:?}", book_update);
                }
                None => info!("other response: {:?}", resp),
            },
            Ok(None) => continue,
            Err(err) => {
                info!("Error parsing response: {:?}", err);
            }
        }
    }
}
