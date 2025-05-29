# okx-trader
A Rust trader system using the OKX V5 API.

# Components
## Threads
1.  redis_handle
    - GLOBAL_TRADE_SIGNALS: Spot_ID<->Threshold
    - LOCAL_DELTAS_SPOT: Spot_ID<->Qty
2.  account_channel_handle
    - **ws: balance_and_position**
      - CCY2BAL: 币种<->币种余额
      - CCY2BAL: 交易产品ID<->持仓数量
3.  orders_info_channel_handle
    - **ws: orders**
      - logging
4.  books_channel_handle
    - **ws: instruments**
      - INST2LOTSZ(下单数量精度)
      - INST2MINSZ(最小下单数)
      - INST2CTVAL(合约面值)
    - **ws: books5/bbo-tbt**
      - INST2BESTBID_**DEPTH**: INST_ID<->BEST BID(price, qty, orders)
      - INST2BESTASK_**DEPTH**: INST_ID<->BEST ASK(price, qty, orders)
      - INST2BESTBID: INST_ID<->BEST BID(price, qty, orders)
      - INST2BESTASK: INST_ID<->BEST ASK(price, qty, orders)
    - tx_books: send msg[盘口变动] to compute_handle
5.  compute_handle
    - rx_compute: receive msg from books_channel_handle
    - Trade Signal Acquirer: GLOBAL_TRADE_SIGNALS
    - Order Book Calculator
      - best bids/asks
      - basis
    - Order Qty Calculator
      - OPEN2
        - LOCAL_DELTAS_SPOT + threshold_2_number: 最大下单数量
        - MAX_OPEN_VALUE: 最大开仓价格
        - constraint_percent_qty: 买入盘口的百分比
        - balance_usdt(CCY2BAL) + constraint_percent_usdt: 账户余额
        - INST2LOTSZ: 下单数量精度
        - INST2CTVAL(合约面值)
      - CLOSE2
        - posision_spot(CCY2BAL): 持仓数量
        - constraint_percent_qty: 买入盘口的百分比
        - MAX_CLOSE_VALUE: 最大关仓价格
        - INST2LOTSZ: 下单数量精度
        - INST2CTVAL(合约面值)
6.  order_handle
    - **ws: order**
    - INST_STATE_MAP: **Lock** the inst_id
    - ORDERID2INST: ORDERID<->INST_ID
    - **ping**

7.  order_recv_handle
    - ORDERID2INST
    - INST_STATE_MAP: **UnLock** the inst_id

8. ping_handle:
    - PING_TIMEOUT
    - **ws: orders**
    - **ws: books5/bbo-tbt/ws: instruments**
    - **ws: balance_and_position**






