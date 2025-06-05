// redis.rs
// "symbol": (thres_2_open, thres_2_close, thres_2_number)
#[derive(Debug)]
pub struct TradeSignal {
    pub threshold_2_open: Option<f64>,
    pub threshold_2_close: Option<f64>,
    pub threshold_2_number: Option<f64>,
    pub threshold_2_caution: Option<f64>,
}

#[derive(Debug, PartialEq)]
pub enum OperationType {
    Open2,
    Close2,
    NoOP,
}

#[derive(Debug)]
pub struct OrderBook {
    pub operation_type: OperationType,
    pub ps_best_bid: f64,
    pub ps_best_bid_qty: f64,
    pub ps_best_ask: f64,
    pub ps_best_ask_qty: f64,
    pub pp_best_bid: f64,
    pub pp_best_bid_qty: f64,
    pub pp_best_ask: f64,
    pub pp_best_ask_qty: f64,
    pub basis_open2: f64,
    pub basis_close2: f64,
}