use log::info;
use okx_rs::websocket::conn::Order;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use crate::globals::CCY2BAL;
use crate::globals::LOCAL_DELTAS_SPOT;
use crate::globals::MAX_CLOSE_VALUE;
use crate::globals::MAX_OPEN_VALUE;
use crate::globals::SPOT_TRADE_RATIO;
use crate::utils::futures_generate_client_order_id;
use crate::utils::spot_generate_client_order_id;
use crate::{compute_engine::calculate_basis_and_signal, globals::{GLOBAL_TRADE_SIGNALS, INST2CTVAL, INST2MINSZ, INST_STATE_MAP}, models::OperationType, INST2LOTSZ};
use crate::utils::generate_nanoid;

pub async fn core_compute(msg: String) -> Option<(Order, Order)>{
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
    // info!("###############Message Receiving Processor: computing engine begins to process swap/spot: {:?} {:?}", swap_inst_id, spot_inst_id);
    
    let inst_state_map = INST_STATE_MAP.read().await;
    if inst_state_map.contains_key(&spot_inst_id) || inst_state_map.contains_key(&swap_inst_id) {
        // info!("###############Skipping this inst_id in compute_handle{:?}", inst_state_map);
        return None;
    }

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
        if !trade_signals.contains_key(&spot_inst_id) {
            // info!("###############Trade Signal Acquirer: No Signal coming, Skipping");
            return None;
        }
        let signal = trade_signals.get(&spot_inst_id);
        threshold_2_open = signal.unwrap().threshold_2_open;
        threshold_2_close = signal.unwrap().threshold_2_close;
        threshold_2_caution = signal.unwrap().threshold_2_caution;
        threshold_2_number = signal.unwrap().threshold_2_number;
    }
    let mut allow_open = !threshold_2_open.is_none(); //TODO: process None in TRADE_SIGNALS
    let mut allow_close = !threshold_2_close.is_none();  //TODO: process None in TRADE_SIGNALS
    // info!("###############Trade Signal Acquirer: trade signal is threshold_2_open={:?}, threshold_2_close={:?}, threshold_2_caution={:?}, threshold_2_number={:?}", threshold_2_open, threshold_2_close, threshold_2_caution, threshold_2_number);
    if !allow_open && !allow_close {
        // info!("###############Skipping this inst_id in compute_handle because none signal");
        return None;
    }

    /*
        * Order Book Calculator
        * It uses booked depth and non-depth channels' best swap/spot ask/bid
        */
    let orderbook_depth =  calculate_basis_and_signal(spot_inst_id.clone(), swap_inst_id.clone(), true, threshold_2_open, threshold_2_close).await;
    let orderbook = calculate_basis_and_signal(spot_inst_id.clone(), swap_inst_id.clone(), false, threshold_2_open, threshold_2_close).await;
    
    
    if orderbook_depth.operation_type == OperationType::NoOP || orderbook.operation_type == OperationType::NoOP { // skip noop signal
        // info!("###############Order Book Calculator: skipping because of NOOP");
        return None;
    }

    if orderbook_depth.operation_type != orderbook.operation_type {
        // info!("###############Skipping this inst_id in compute_handle because not matched op type or none signal");
        return None;
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
        let mut position_swap = Decimal::new(0, 0);
        let mut position_spot = Decimal::new(0, 0);
        let mut true_position_spot = Decimal::new(0, 0);
        let mut swap_ctval = Decimal::new(0, 0);
        {
            let mut inst2lotsz = INST2LOTSZ.lock().await;
            let mut inst2minsz = INST2MINSZ.lock().await;
            let mut inst2ctval = INST2CTVAL.lock().await;
            
            spot_lot_size = inst2lotsz.get(&spot_inst_id).unwrap_or(&Decimal::zero()).clone();
            swap_lot_size = inst2lotsz.get(&swap_inst_id).unwrap_or(&Decimal::zero()).clone();
            spot_min_size = inst2minsz.get(&spot_inst_id).unwrap_or(&Decimal::zero()).clone();
            swap_min_size = inst2minsz.get(&swap_inst_id).unwrap_or(&Decimal::zero()).clone();
            swap_ctval = inst2ctval.get(&swap_inst_id).unwrap_or(&Decimal::zero()).clone();
            lot_size = spot_lot_size.max(swap_lot_size * swap_ctval);
        }
        {
            let ccy2bal = CCY2BAL.lock().await;
            balance_usdt = ccy2bal.get("USDT").unwrap_or(&Decimal::zero()).clone();
            position_swap = ccy2bal.get(&swap_inst_id).unwrap_or(&Decimal::zero()).clone();
            let true_position_spot = ccy2bal.get(&spot_inst_id).unwrap_or(&Decimal::zero()).clone();
            position_spot = (position_swap * swap_ctval).abs().min(true_position_spot.abs());
        }
        let is_open = orderbook.operation_type == OperationType::Open2;
        let is_close = !is_open;
        let mut constraint_percent_qty = 0.5;
        if let Some(caution) = threshold_2_caution { //0/1, if 1: 谨慎策略
            if caution > 0.0 {
                constraint_percent_qty = 0.1
            }
        };
        
        
        info!("###############Signal {:?} Basic Order Para Calculator: spot_inst_id is {:?}, spot_lot_sz is {:?}, swap_lot_sz is {:?}, lot_sz is {:?}, spot_min_sz is {:?}, swap_min_sz is {:?}, swap_ct_val is {:?}, balance_usdt is {:?}, position_swap is {:?}, position_spot is {:?}, true_posision_spot is {:?} and constraint_percent_qty is {:?}", 
                                                         is_open, spot_inst_id, spot_lot_size, swap_lot_size, lot_size, spot_min_size, swap_min_size, swap_ctval, balance_usdt, position_swap, position_spot, true_position_spot, constraint_percent_qty);
        
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
        // info!("###############Signal {:?} Basic Order Qty Calculator: ps_best_ask_qty = {:?}, ps_best_bid_qty = {:?}, pp_best_bid_qty = {:?}, pp_best_ask_qty = {:?}, trade_qty={:?}, unit_price_spot={:?} and unit_price_swap={:?}", is_open, ps_best_ask_qty, ps_best_bid_qty, pp_best_ask_qty, pp_best_bid_qty, trade_qty, unit_price_spot, unit_price_swap);

        if is_open {
            let spot_delta;
            {
                let local_deltas_spot = LOCAL_DELTAS_SPOT.lock().await;
                if let Some(v) = local_deltas_spot.get(&spot_inst_id) {
                    spot_delta = *v;
                } else {
                    info!("unsatisfied [local_deltas_spot] for spot_inst_id: {:?}", spot_inst_id);
                    info!("###############Order Book Calculator: orderbook for depth is {:?}, orderbook for non-depth is {:?}", orderbook_depth, orderbook);
                    return None;
                }
            }
            let open_qty_max = match threshold_2_number {
                Some(num) => num,
                None => {
                    info!("unsatisfied [threshold_2_number] {:?} for spot_inst_id: {:?}", threshold_2_number, spot_inst_id);
                    info!("###############Order Book Calculator: orderbook for depth is {:?}, orderbook for non-depth is {:?}", orderbook_depth, orderbook);
                    return None;
                }
            };

            if trade_qty + spot_delta > open_qty_max { // Maximum order quantity constraint
                trade_qty = open_qty_max - spot_delta
            }
            
            
            let open2_max_volume = *MAX_OPEN_VALUE;
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
            info!("###############Basic Order Qty Adjusting for OPEN2: spot_delta={:?}, open_qty_max={:?}, trade_value={:?}, constraint_value={:?} and trade_qty={:?}", spot_delta, open_qty_max, trade_value, constraint_value, trade_qty);
        }

        let mut trade_qty_decimal = Decimal::from_f64(trade_qty).unwrap();
        if is_close {
            if position_spot.to_f64().unwrap() <= 0.0 {
                info!("unsatisfied [position_spot] {:?} for spot_inst_id: {:?}", position_spot, spot_inst_id);
                return None;
            }
            if trade_qty * unit_price_spot > *MAX_CLOSE_VALUE // Maximum close price constraint
            {
                trade_qty_decimal =
                    Decimal::from_f64(*MAX_CLOSE_VALUE / unit_price_spot).unwrap();
            }
            
            if position_spot.to_f64().unwrap() < trade_qty_decimal.to_f64().unwrap() { // Maximum position constraint
                trade_qty_decimal = position_spot;
            }
            if trade_qty_decimal < spot_min_size { //skip when spot_min_sz is not saisfied
                info!("unsatisfied [spot_min_size] {:?} for spot_inst_id: {:?}", spot_min_size, spot_inst_id);
                info!("###############Order Book Calculator: orderbook for depth is {:?}, orderbook for non-depth is {:?}", orderbook_depth, orderbook);
                return None;
            }
            info!("###############Basic Order Qty Adjusting for CLOSE2: trade_qty_decimal={:?}", trade_qty_decimal); 
        }

        
        // trade_qty_decimal = Decimal::new(10, 0); //TODO: set by hard code

        if trade_qty_decimal.to_f64().unwrap() <= 0.0 { // Check if quantity is valid
            info!("unsatisfied [zero trade_qty_decimal] {:?} for spot_inst_id: {:?}", trade_qty_decimal, spot_inst_id);
            info!("###############Order Book Calculator: orderbook for depth is {:?}, orderbook for non-depth is {:?}", orderbook_depth, orderbook);
            return None;
        }
        trade_qty_decimal = trade_qty_decimal * (*SPOT_TRADE_RATIO);
        trade_qty_decimal = (trade_qty_decimal / lot_size).floor() * lot_size;
        // trade_qty_decimal = Decimal::new(10, 0); //TODO: set by hard code
        if trade_qty_decimal < spot_min_size // Lot size constraint
            || trade_qty_decimal / swap_ctval < swap_min_size
        {
            info!(
                "unsatisfied trade_qty_decimal {:?} for spot_inst_id: {:?}, [spot_min_size]: {:?}, [swap_min_size]: {:?}",
                trade_qty_decimal, spot_inst_id, spot_min_size, swap_min_size
            );
            info!("###############Order Book Calculator: orderbook for depth is {:?}, orderbook for non-depth is {:?}", orderbook_depth, orderbook);
            return None;
        }

        info!(
            "###############Final Order Calculator: inst_id={:?}, trade_qty={:?}, All checking passes!",
            spot_inst_id, trade_qty_decimal
        );

        
        // let timestamp = get_timestamp(SystemTime::now()).unwrap();
        let nanoid = generate_nanoid();

        if is_open { // Spot: BUY, Future: SELL
            {
                let mut local_deltas_spot = LOCAL_DELTAS_SPOT.lock().await; //update local spot
                local_deltas_spot.insert(spot_inst_id.clone(), trade_qty_decimal.to_f64().unwrap());
                // info!(
                //     "###############Update Local Delta: {:?}",
                //     local_deltas_spot
                // );
            }
            let order_spot = Order {
                id: spot_generate_client_order_id(&OperationType::Open2, &nanoid),
                side: "buy".to_string(),
                inst_id: spot_inst_id,
                tdMode: "cross".to_string(),
                ordType: "market".to_string(),
                sz: trade_qty_decimal.to_string(),
                tgtCcy: "1".to_string(),
                reduceOnly: "0".to_string(),
                price: unit_price_spot.to_string(),
                threshold: threshold_2_open.unwrap_or(0.0).to_string(),
                // sz: "1.0".to_string(),
            };
            let trade_qty_decimal_swap = (trade_qty_decimal / swap_ctval / swap_lot_size).floor() * swap_lot_size;
            let order_swap = Order {
                id: futures_generate_client_order_id(&OperationType::Open2, &nanoid),
                side: "sell".to_string(),
                inst_id: swap_inst_id,
                tdMode: "cross".to_string(),
                ordType: "market".to_string(),
                sz: trade_qty_decimal_swap.to_string(),
                tgtCcy: "0".to_string(),
                reduceOnly: "0".to_string(),
                price: unit_price_swap.to_string(),
                threshold: threshold_2_open.unwrap_or(0.0).to_string(),
            };
            return Some((order_spot, order_swap));
        } else {
            let mut trade_qty_spot = trade_qty_decimal;
            if trade_qty_spot > position_spot {
                trade_qty_spot = (position_spot / lot_size).floor() * lot_size;
                info!("re-adjust trade_qty_spot to position_spot");
            }
            let order_spot = Order {
                id: spot_generate_client_order_id(&OperationType::Close2, &nanoid),
                side: "sell".to_string(),
                inst_id: spot_inst_id,
                tdMode: "cross".to_string(),
                ordType: "market".to_string(),
                sz: trade_qty_spot.to_string(),
                tgtCcy: "0".to_string(),
                reduceOnly: "0".to_string(),
                price: unit_price_spot.to_string(),
                threshold: threshold_2_close.unwrap_or(0.0).to_string(),
            };
            let trade_qty_decimal_swap = (trade_qty_decimal / swap_ctval / swap_lot_size).floor() * swap_lot_size;
            let order_swap = Order {
                id: futures_generate_client_order_id(&OperationType::Close2, &nanoid),
                side: "buy".to_string(),
                inst_id: swap_inst_id,
                tdMode: "cross".to_string(),
                ordType: "market".to_string(),
                sz: trade_qty_decimal_swap.to_string(),
                tgtCcy: "0".to_string(),
                reduceOnly: "0".to_string(),
                price: unit_price_swap.to_string(),
                threshold: threshold_2_close.unwrap_or(0.0).to_string(),
            };
            return Some((order_spot, order_swap));
        }
    }
    return None;
}