from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='stats-group',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {
    "count": 0,
    "sum": 0,
    "min": float('inf'),
    "max": float('-inf')
})

msg_count = 0

for message in consumer:
    event = message.value

    category = event["category"]
    amount = event["amount"]

    stats[category]["count"] += 1
    stats[category]["sum"] += amount
    stats[category]["min"] = min(stats[category]["min"], amount)
    stats[category]["max"] = max(stats[category]["max"], amount)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- STATYSTYKI PER KATEGORIA ---")
        print("Kategoria | Liczba | Suma | Min | Max")

        for cat, s in stats.items():
            print(
                f"{cat} | {s['count']} | "
                f"{s['sum']:.2f} | "
                f"{s['min']:.2f} | "
                f"{s['max']:.2f}"
            )
