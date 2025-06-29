use std::collections::{HashMap, BTreeMap};
use rust_decimal::Decimal;
use crate::models::TradeSignal;
use std::sync::Mutex;
use tokio::sync::RwLock;

use lazy_static::lazy_static;



lazy_static::lazy_static! {
    pub static ref GLOBAL_TRADE_SIGNALS:tokio::sync::Mutex<HashMap<String, TradeSignal>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static::lazy_static! {
    pub static ref CCY2BAL: tokio::sync::Mutex<HashMap<String, Decimal>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static::lazy_static! {
    pub static ref LOCAL_DELTAS_SPOT: tokio::sync::Mutex<HashMap<String, f64>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static! {
    pub static ref INST2LOTSZ: tokio::sync::Mutex<HashMap<String, Decimal>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static! {
    pub static ref INST2MINSZ: tokio::sync::Mutex<HashMap<String, Decimal>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static! {
    pub static ref INST2CTVAL: tokio::sync::Mutex<HashMap<String, Decimal>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static! {
    pub static ref DEPTH_MAP: Mutex<HashMap<String, (BTreeMap<Decimal, (Decimal, Decimal)>, BTreeMap<Decimal, (Decimal, Decimal)>)>> = Mutex::new(HashMap::new());
}

lazy_static! {
    pub static ref DEPTH_MAP_Books5: tokio::sync::Mutex<HashMap<String, (Vec<Vec<Decimal>>, Vec<Vec<Decimal>>)>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static! {
    pub static ref INST2BESTBID_DEPTH: tokio::sync::Mutex<HashMap<String, Vec<Decimal>>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static! {
    pub static ref INST2BESTASK_DEPTH: tokio::sync::Mutex<HashMap<String, Vec<Decimal>>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static! {
    pub static ref INST2BESTBID: tokio::sync::Mutex<HashMap<String, Vec<Decimal>>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static! {
    pub static ref INST2BESTASK: tokio::sync::Mutex<HashMap<String, Vec<Decimal>>> = {
        let mut map = HashMap::new();
        tokio::sync::Mutex::new(map)
    };
}

lazy_static! {
    #[derive(Debug)]
    pub static ref INST_STATE_MAP: RwLock<HashMap<String, String>> = RwLock::new(HashMap::new());
}

lazy_static::lazy_static! {
    pub static ref ORDERID2INST: RwLock<HashMap<String, String>> = RwLock::new(HashMap::new());
}

lazy_static! {
    pub static ref PING_TIMEOUT: u64 = 10;
}

lazy_static! {
    pub static ref MAX_OPEN_VALUE: f64 = 400.0; //最大开仓价格
    pub static ref  MAX_CLOSE_VALUE: f64 = 1000.0; //最大关仓价格
    pub static ref  SPOT_TRADE_RATIO: Decimal = Decimal::new(100015, 5);
}

lazy_static! {
    // 连接 Redis 服务器
    pub static ref redis_url: String = std::env::var("REDIS_URL").unwrap().to_string();
    pub static ref redis_password: String = std::env::var("REDIS_PASSWD").unwrap().to_string();
    pub static ref redis_channel: String  = std::env::var("REDIS_CHANNEL").unwrap().to_string();
    pub static ref redis_port: i32 = 6379;
    pub static ref redis_db: i32 = 1;
   

    pub static ref STRATEGY_REDIS_CLIENT: redis::Client = redis::Client::open(format!(
        "redis://:{}@{}:{}/{}", 
        *redis_password, 
        *redis_url, 
        *redis_port, 
        *redis_db
    ))
    .unwrap();
}

lazy_static! {
    pub static ref key: String = std::env::var("OKX_API_KEY").unwrap();
    pub static ref secret: String = std::env::var("OKX_API_SECRET").unwrap();
    pub static ref passphrase: String = std::env::var("OKX_API_PASSPHRASE").unwrap();
}

pub type _ThresArray = [Option<f64>; 3];

// TODO: Just for developing, drop it later
pub async fn init_trade_signals() {
    let mut trade_signals = GLOBAL_TRADE_SIGNALS.lock().await;
    trade_signals.insert("BTC-USDT".to_string(), TradeSignal {
        threshold_2_open: Some(-0.00019783602790533885),
        threshold_2_close: Some(-0.0010639728302795882),
        threshold_2_number: Some(6032261.039185599),
        threshold_2_caution: Some(0.0),
    });

    trade_signals.insert("ETH-USDT".to_string(), TradeSignal {
        threshold_2_open: Some(-0.00019783602790533885),
        threshold_2_close: Some(-0.0010639728302795882),
        threshold_2_number: Some(6032261.039185599),
        threshold_2_caution: Some(0.0),
    });

    trade_signals.insert("TRUMP-USDT".to_string(), TradeSignal {
        threshold_2_open: Some(-1.0),
        threshold_2_close: Some(-0.0010639728302795882),
        threshold_2_number: Some(6032261.039185599),
        threshold_2_caution: Some(0.0),
    });

    trade_signals.insert("ADA-USDT".to_string(), TradeSignal {
        threshold_2_open: Some(-1.0),
        threshold_2_close: Some(-0.0010639728302795882),
        threshold_2_number: Some(6032261.039185599),
        threshold_2_caution: Some(0.0),
    });
}

pub async fn init_ccy2bal(ccy_ids: Vec<&str>) {
    let mut ccy2bal = CCY2BAL.lock().await;
    ccy2bal.insert("USDT".to_string(), Decimal::new(0, 0));
    for id in &ccy_ids {
        ccy2bal.insert(id.to_string().clone(), Decimal::new(0, 0));
    }
}


