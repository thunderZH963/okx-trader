import redis

# 连接到 Redis 服务器
r = redis.Redis(host='r-6wexuqqdgsn4wmcurcpd.redis.japan.rds.aliyuncs.com', port=6379, db=1, password="Al289Rj681")

# 创建订阅对象
pubsub = r.pubsub()

# 订阅指定频道
pubsub.subscribe('FakeSignalOKX')  # 将 'my_channel' 替换为你要订阅的频道

# 监听消息
print("Subscribed to 'my_channel'... Waiting for messages.")
for message in pubsub.listen():
    if message['type'] == 'message':
        print(f"Received message: {message['data']}")