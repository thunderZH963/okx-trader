use crate::api::v5::BookUpdate;
use crate::websocket::WebsocketChannel;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// FIXME: each book type can largely be combined into single Enum

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookChannelArg<'a> {
    pub channel: Option<&'a str>,
    pub inst_id: Option<&'a str>,
}

pub struct OrderChannelArg<'a> {
    pub op: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentsChannelArg<'a> {
    pub channel: Option<&'a str>,
    pub instType: Option<&'a str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Books5 {
    pub inst_id: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub id: String,
    pub side: String,
    pub inst_id: String,
    pub tdMode: String,
    pub ordType: String,
    pub sz: String,
    pub tgtCcy: String,
    pub reduceOnly: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Books {
    pub inst_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BboTbt {
    pub inst_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BooksL2Tbt {
    pub inst_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Instruments {
    pub instType: String,
}

impl WebsocketChannel for Books {
    #[cfg(feature = "vip")]
    const CHANNEL: &'static str = "books-l2-tbt";
    #[cfg(not(feature = "vip"))]
    const CHANNEL: &'static str = "books";

    type Response<'de> = [BookUpdate<'de>; 1];
    type ArgType<'de> = BookChannelArg<'de>;

    fn subscribe_message(&self) -> String {
        let Books { inst_id } = self;
        json!({
            "op": "subscribe",
            "args": [
                {
                    "channel": Self::CHANNEL,
                    "instId": inst_id,
                }
            ]
        })
        .to_string()
    }

    fn unsubscribe_message(&self) -> String {
        todo!()
    }
}

impl WebsocketChannel for Books5 {
    const CHANNEL: &'static str = "books5";
    type Response<'de> = [BookUpdate<'de>; 1];
    type ArgType<'de> = BookChannelArg<'de>;

    fn subscribe_message(&self) -> String {
        let Books5 { inst_id } = self;
        json!({
            "op": "subscribe",
            "args": [
                {
                    "channel": Self::CHANNEL,
                    "instId": inst_id,
                }
            ]
        })
        .to_string()
    }

    fn unsubscribe_message(&self) -> String {
        todo!()
    }
}

impl WebsocketChannel for BboTbt {
    const CHANNEL: &'static str = "bbo-tbt";
    type Response<'de> = [BookUpdate<'de>; 1];
    type ArgType<'de> = BookChannelArg<'de>;

    fn subscribe_message(&self) -> String {
        let BboTbt { inst_id } = self;
        json!({
            "op": "subscribe",
            "args": [
                {
                    "channel": Self::CHANNEL,
                    "instId": inst_id,
                }
            ]
        })
        .to_string()
    }

    fn unsubscribe_message(&self) -> String {
        todo!()
    }
}
impl WebsocketChannel for BooksL2Tbt {
    const CHANNEL: &'static str = "books-l2-tbt";
    type Response<'de> = [BookUpdate<'de>; 1];
    type ArgType<'de> = BookChannelArg<'de>;

    fn subscribe_message(&self) -> String {
        let BooksL2Tbt { inst_id } = self;
        json!({
            "op": "subscribe",
            "args": [
                {
                    "channel": Self::CHANNEL,
                    "instId": inst_id,
                }
            ]
        })
        .to_string()
    }

    fn unsubscribe_message(&self) -> String {
        todo!()
    }
}

impl WebsocketChannel for Instruments {
    const CHANNEL: &'static str = "instruments";
    type Response<'de> = [BookUpdate<'de>; 1];
    type ArgType<'de> = InstrumentsChannelArg<'de>;

    fn subscribe_message(&self) -> String {
        let Instruments { instType } = self;
        json!({
            "op": "subscribe",
            "args": [
                {
                    "channel": Self::CHANNEL,
                    "instType": instType,
                }
            ]
        })
        .to_string()
    }

    fn unsubscribe_message(&self) -> String {
        todo!()
    }
}

impl WebsocketChannel for Order {
    const CHANNEL: &'static str = "order";

    type Response<'de> = [BookUpdate<'de>; 1];
    type ArgType<'de> = BookChannelArg<'de>;

    fn subscribe_message(&self) -> String {
        let Order { id, side, inst_id, tdMode, ordType, sz, tgtCcy, reduceOnly} = self;
        if tgtCcy == "1" {
            json!({
                "id": id,
                "op": Self::CHANNEL,
                "args": [
                    {
                        "side": side,
                        "instId": inst_id,
                        "tdMode": tdMode,
                        "ordType": ordType,
                        "sz": sz,
                        "tgtCcy": "base_ccy",
                    }
                ]
            })
            .to_string()
        } 
        else if reduceOnly == "1" {
            json!({
                "id": id,
                "op": Self::CHANNEL,
                "args": [
                    {
                        "side": side,
                        "instId": inst_id,
                        "tdMode": tdMode,
                        "ordType": ordType,
                        "sz": sz,
                        "reduceOnly:": "true",
                    }
                ]
            })
            .to_string()
        }
        else {
            json!({
                "id": id,
                "op": Self::CHANNEL,
                "args": [
                    {
                        "side": side,
                        "instId": inst_id,
                        "tdMode": tdMode,
                        "ordType": ordType,
                        "sz": sz,
                    }
                ]
            })
            .to_string()
        }
    }

    fn unsubscribe_message(&self) -> String {
        todo!()
    }
}

