import re
import csv
from bidict import bidict

fileHandler = open("./log/active.log", "r")
orderId2clientId = dict({})
spotId2swapId = dict({})
clientId2info = {}

csv_file = open("csv/okx_trade_info.csv", mode="w", newline="")
csv_writer = csv.DictWriter(csv_file, fieldnames=["clientId", "instId", "size", "price", "threshold", "send_time", "signal", "create_time", "finish_time", "executed_avg_price", "executed_size", "status", "related_clientId"])


csv_writer.writeheader()

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
            pattern = r'clientId: "([^"]+)", instId: "([^"]+)", sz: "([^"]+)", price: "([^"]+)", timestamp: (\d+), threshold: "[+-]?\d+(\.\d+)?"'
            match = re.search(pattern, line.strip())
            if match:
                clientId = match.group(1)
                instId = match.group(2)
                sz = match.group(3)
                price = match.group(4)
                timestamp = match.group(5)
                threshold = match.group(6)
                clientId2info[clientId] = {
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
        elif "<<<<<<<Order sucessully placed" in line:
            pattern = r'clientId: "([^"]+)", ts: "([^"]+)", orderId: "([^"]+)"'
            match = re.search(pattern, line.strip())
            if match:
                clientId = match.group(1)
                ts = match.group(2)
                orderId = match.group(3)
                orderId2clientId.update({orderId: clientId})
                clientId2info[clientId]["create_time"] = ts
                clientId2info[clientId]["status"] = "created"
        elif "Orders Info Channel receives" in line:
            pattern = r'ordId: "(\d+)", fillPx: "([\d\.]*)", fillSz: "(\d*)", ctime: "(\d+)", utime: "(\d+)"'
            match = re.search(pattern, line.strip())
            if match:
                ordId = match.group(1)
                clientId = orderId2clientId.get(ordId, None)
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

                    client_info = {
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
                    del clientId2info[clientId]
                    del orderId2clientId[ordId]

# 关闭文件处理器
fileHandler.close()
csv_file.close()

print("Data has been written to client_info.csv")
