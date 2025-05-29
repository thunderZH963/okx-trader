use std::collections::HashMap;
use futures_util::StreamExt;
use crate::globals::{GLOBAL_TRADE_SIGNALS, LOCAL_DELTAS_SPOT, STRATEGY_REDIS_CLIENT, redis_channel};
pub type _ThresArray = [Option<f64>; 3];
use crate::models::TradeSignal;
use log::{error, info};
use std::fs;

pub async fn redis_subscribe() {
    let json_str = fs::read_to_string("./src/symbol_list.json").unwrap();
    
    // 解析为 HashMap
    let symbol_list: HashMap<String, HashMap<String, String>> =
    serde_json::from_str(&json_str).expect("Failed to parse JSON");
    loop{
        if let Ok(async_conn) = STRATEGY_REDIS_CLIENT.get_async_connection().await {
            let mut pubsub = async_conn.into_pubsub();
            match pubsub.subscribe(redis_channel.to_string()).await {
                Ok(_) => {
                    let mut msg_stream = pubsub.into_on_message();
                    info!("@@@@@@@@@@@@@@@Redis Handle: subscribe redis ok");
                    while let Some(msg) = msg_stream.next().await {
                        let payload: String = msg.get_payload().unwrap_or_default();
                        match serde_json::from_str::<HashMap<String, _ThresArray>>(&payload) {
                            Ok(thresholds_map) => {
                                let updated_signals = thresholds_map.into_iter().filter_map(|(symbol, thresholds)| {
                                    let new_key = symbol_list.get(&symbol).and_then(|v| v.get("spot")).cloned();
                                    new_key.map(|mapped_key| {
                                        let sig = TradeSignal {
                                            threshold_2_open: thresholds[0],
                                            threshold_2_close: thresholds[1],
                                            threshold_2_number: thresholds[2],
                                            threshold_2_caution: Some(0.0),
                                        };
                                        info!(
                                            "@@@@@@@@@@@@@@@Redis Handle: Updating signal for {}: open={:?}, close={:?}, number={:?}, caution={:?}",
                                            mapped_key,
                                            sig.threshold_2_open,
                                            sig.threshold_2_close,
                                            sig.threshold_2_number,
                                            sig.threshold_2_caution
                                        );
                                        (mapped_key, sig)
                                    })
                                });
                        
                                {
                                    let mut trade_signals = GLOBAL_TRADE_SIGNALS.lock().await;
                                    for (key, value) in updated_signals {
                                        trade_signals.insert(key, value);
                                    }
                                    info!("@@@@@@@@@@@@@@@Redis Handle: Trade Signals updated: {:?}", trade_signals);
                                }
                                {
                                    let mut local_deltas_spot = LOCAL_DELTAS_SPOT.lock().await;
                                    for id in local_deltas_spot.clone().keys() {
                                        local_deltas_spot.insert(id.to_string(), 0.0);
                                    }
                                    info!("@@@@@@@@@@@@@@@Redis Handle: Local deltas updated: {:?}", local_deltas_spot);
                                }
                            }
                            Err(e) => {
                                error!("@@@@@@@@@@@@@@@Redis Handle: Failed to parse JSON: {}", e);
                                error!("@@@@@@@@@@@@@@@Redis Handle: Raw payload: {}", payload);
                            }
                        }
                        
                    }
                }
                Err(e) => {
                }
            }
        }
        else {
        }
    }
} 



