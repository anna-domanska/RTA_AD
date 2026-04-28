from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='anomaly-detector-group',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transactions = defaultdict(deque)

print("Nasłuchuję anomalii: >3 transakcje jednego usera w ciągu 60 sekund...")

for message in consumer:
    event = message.value

    user_id = event["user_id"]
    tx_id = event["tx_id"]
    amount = event["amount"]
    timestamp = datetime.fromisoformat(event["timestamp"])

    user_transactions[user_id].append(timestamp)

    window_start = timestamp - timedelta(seconds=60)

    while user_transactions[user_id] and user_transactions[user_id][0] < window_start:
        user_transactions[user_id].popleft()

    if len(user_transactions[user_id]) > 3:
        print(
            f"ALERT: user {user_id} wykonał "
            f"{len(user_transactions[user_id])} transakcje w ciągu 60 sekund | "
            f"ostatnia transakcja: {tx_id} | {amount:.2f} PLN"
        )
    else:
        print(
            f"OK: {user_id} | {tx_id} | "
            f"transakcje w ostatnich 60s: {len(user_transactions[user_id])}"
        )
