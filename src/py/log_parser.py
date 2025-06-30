import re
import csv
import os
import math
skip_ids = []
with open("csv/okx_trade_info_final.csv", newline='', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        skip_ids.append(row[3])
        skip_ids.append(row[4])

log_dir = './log/'  # 替换为你的log目录路径
file_paths = []
for root, dirs, files in os.walk(log_dir):
    for file in files:
        file_paths.append(os.path.join(root, file))
for log_file in file_paths:
    print(log_file)
    fileHandler = open(log_file, "r")
    orderId2clientId = dict({})
    spotId2swapId = dict({})
    clientId2info = {}

    csv_file = open("csv/okx_trade_info.csv", mode="a+", newline="")
    csv_writer = csv.DictWriter(csv_file, fieldnames=["timestamp", "clientId", "instId", "size", "price", "threshold", "send_time", "signal", "create_time", "finish_time", "executed_avg_price", "executed_size", "status", "related_clientId"])
    if os.stat("csv/okx_trade_info.csv").st_size == 0:
        csv_writer.writeheader()

    csv_file_final = open("csv/okx_trade_info_final.csv", mode="a+", newline="")
    csv_writer_final = csv.DictWriter(csv_file_final, fieldnames=["timestamp", "symbol", "signal", "spot_clientId", "swap_clientId", "threshold", "expected_spot_price", "expected_swap_price", "expected_basis", "expected_volume", "executed_spot_price", "executed_swap_price", "executed_basis", "executed_volume", "spot_status", "swap_status", "spot_send_time", "swap_send_time", "spot_create_time", "swap_create_time", "spot_finish_time", "swap_finish_time", "gain"])
    if os.stat("csv/okx_trade_info_final.csv").st_size == 0:
        csv_writer_final.writeheader()

    def get_spot_swap_id(clientId):
        if clientId[0:3] in ["so2", "sc2"]:
            spot_id = clientId
            swap_id = clientId2info[clientId].get("related_clientId")
        else:
            spot_id = clientId2info[clientId].get("related_clientId")
            swap_id = clientId
        return swap_id, spot_id

    def write_one_trade(clientId, check = False):
        if not check or (check and clientId2info[clientId]["status"] != "filled"):
            if clientId in skip_ids:
                return
            client_info = {
                "timestamp": clientId2info[clientId]["timestamp"],
                "clientId": clientId,
                "instId": clientId2info[clientId]["instId"],
                "size": clientId2info[clientId]["size"],
                "price": clientId2info[clientId]["price"],
                "threshold": clientId2info[clientId]["threshold"],
                "send_time": clientId2info[clientId]["send_time"],
                "signal": clientId2info[clientId]["signal"],
                "create_time": clientId2info[clientId].get("create_time", ""),
                "finish_time": clientId2info[clientId].get("finish_time", ""),
                "executed_avg_price": clientId2info[clientId].get("executed_avg_price", 0),
                "executed_size": clientId2info[clientId].get("executed_size", 0),
                "related_clientId": clientId2info[clientId].get("related_clientId", ""),
                "status": clientId2info[clientId].get("status", "unknown"),
            }
            csv_writer.writerow(client_info)
        

    def write_merge_two_trade(spot_id, swap_id, delete=True):
        spot_info = clientId2info[spot_id]
        swap_info = clientId2info[swap_id]
        if spot_id in skip_ids or swap_id in skip_ids:
                return
        trade_info = {
            "timestamp": spot_info["timestamp"],
            "symbol": spot_info["instId"],
            "signal": spot_info["signal"],
            "spot_clientId": spot_id,
            "swap_clientId": swap_id,
            "threshold": spot_info["threshold"],
            "expected_spot_price": spot_info["price"],
            "expected_swap_price": swap_info["price"],
            "expected_basis": math.log(float(swap_info["price"])) -  math.log(float(spot_info["price"])),
            "expected_volume": spot_info["size"],
            "executed_spot_price": spot_info.get("executed_avg_price", 0),
            "executed_swap_price": swap_info.get("executed_avg_price", 0),
            # "executed_basis": math.log(float(swap_info.get("executed_avg_price", 0))) -  math.log(float(spot_info.get("executed_avg_price", 0))),
            "executed_volume": spot_info.get("executed_size"),
            "spot_status": spot_info["status"],
            "swap_status": swap_info["status"],
            "spot_send_time": spot_info["send_time"],
            "swap_send_time": swap_info["send_time"],
            "spot_create_time": spot_info.get("create_time", ""),
            "swap_create_time": swap_info.get("create_time", ""),
            "spot_finish_time": spot_info.get("finish_time", ""),
            "swap_finish_time": swap_info.get("finish_time", ""),
        }
        try:
            executed_basis = math.log(float(swap_info.get("executed_avg_price", 0))) -  math.log(float(spot_info.get("executed_avg_price", 0)))
        except:
            executed_basis = "error"
        trade_info["executed_basis"] = executed_basis
        try:
            if spot_info["signal"] == "OPEN2":
                gain = (float(trade_info["executed_basis"]) - float(trade_info["threshold"])) * float(trade_info["executed_volume"]) * float(trade_info["executed_spot_price"])
            else:
                gain = -1 * (float(trade_info["executed_basis"]) - float(trade_info["threshold"])) * float(trade_info["executed_volume"]) * float(trade_info["executed_spot_price"])
        except:
            gain = "error"
    
        
    
        trade_info["gain"] = gain
        csv_writer_final.writerow(trade_info)
        if delete:
            del clientId2info[spot_id]
            del clientId2info[swap_id]
            del spotId2swapId[spot_id]
            del spotId2swapId[swap_id]


    while True:
        line = fileHandler.readline()
        if not line:
            break
        if "<<<<<<" in line:
            if "processing spot and swap order:" in line:
                pattern = r'spot_client_id: "([^"]+)", swap_client_id: "([^"]+)"'
                match = re.search(pattern, line.strip())
                if match:
                    spot_client_id = match.group(1)
                    swap_client_id = match.group(2)
                    spotId2swapId[spot_client_id] = swap_client_id
                    spotId2swapId[swap_client_id] = spot_client_id
            elif "<<<<<<<send order_" in line:
                pattern = r'clientId: "([^"]+)", instId: "([^"]+)", sz: "([^"]+)", price: "([^"]+)", timestamp: (\d+), threshold: "\s*(-?\d*\.\d+)"'
                match = re.search(pattern, line.strip())
                if match:
                    clientId = match.group(1)
                    instId = match.group(2)
                    sz = match.group(3)
                    price = match.group(4)
                    timestamp = match.group(5)
                    threshold = match.group(6)
                    clientId2info[clientId] = {
                        "timestamp": line.split("+")[0],
                        "instId": instId,
                        "size": sz,
                        "price": price,
                        "send_time": timestamp,
                        "signal": "OPEN2" if clientId[0:3] in ["so2", "fo2"] else "CLOSE2",
                        "trade": [],
                        "related_clientId": spotId2swapId.get(clientId, ""),
                        "status": "sending",
                        "threshold": threshold if threshold else "0",
                    }
            elif "Order successfully placed" in line:
                pattern = r'clientId: "([^"]+)", ts: "([^"]+)", orderId: "([^"]+)", msg is "([^"]+)"'
                match = re.search(pattern, line.strip())
                if match:
                    clientId = match.group(1)
                    ts = match.group(2)
                    orderId = match.group(3)
                    if orderId != "":
                        orderId2clientId.update({orderId: clientId})
                        clientId2info[clientId]["create_time"] = ts
                        clientId2info[clientId]["status"] = "created"
                    else:
                        clientId2info[clientId]["create_time"] = ts
                        clientId2info[clientId]["status"] = "failed, " + line.split(" msg is")[-1]

    fileHandler.close()
    fileHandler = open(log_file, "r")
    while True:
        line = fileHandler.readline()
        if not line:
            break
        if "<<<<<<" in line:
            if "Orders Info Channel receives" in line:
                pattern = r'ordId: "(\d+)", fillPx: "([\d\.]*)", fillSz: "([\d\.]*)", ctime: "(\d+)", utime: "(\d+)"'
                match = re.search(pattern, line.strip())
                if match:
                    ordId = match.group(1)
                    try:
                        clientId = orderId2clientId[ordId]
                    except:
                        print("error[not found ordid]", line)
                        continue
                    fillPx = match.group(2)
                    fillSz = match.group(3)
                    if fillPx == "":
                        fillPx = "0"
                    if fillSz == "":
                        fillSz = "0"
                    utime = match.group(4)
                    clientId2info[clientId]["trade"].append({
                        "fillPx": fillPx,
                        "fillSz": fillSz,
                        "utime": utime,
                    })
                    if "reveives partial filled order" in line:
                        clientId2info[clientId]["status"] = "partially filled"
                    if "receives filled order" in line:
                        clientId2info[clientId]["finish_time"] = utime
                        px = 0
                        sz = 0
                        for trade in clientId2info[clientId]["trade"]:
                            px += float(trade["fillPx"]) * float(trade["fillSz"])
                            sz += float(trade["fillSz"])
                        clientId2info[clientId]["executed_avg_price"] = px / sz if sz != 0 else 0
                        clientId2info[clientId]["executed_size"] = sz
                        clientId2info[clientId]["status"] = "filled"
                        write_one_trade(clientId)
                        del orderId2clientId[ordId]
                        if clientId2info[clientId2info[clientId].get("related_clientId")]["status"] == "filled":
                            swap_id, spot_id = get_spot_swap_id(clientId)
                            write_merge_two_trade(spot_id, swap_id)
                            

    skip = []
    for clientId, info in clientId2info.items():
        if clientId in skip:
            continue
        spot_id, swap_id = get_spot_swap_id(clientId)
        write_one_trade(spot_id, check=True)
        write_one_trade(swap_id, check=True)
        write_merge_two_trade(spot_id, swap_id, delete=False)
        skip.append(spot_id)
        skip.append(swap_id)


    # 关闭文件处理器
    fileHandler.close()
    csv_file.close()
    csv_file_final.close()
