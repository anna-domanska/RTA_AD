from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    event = message.value

    amount = event["amount"]

    if amount > 3000:
        print(
            f"ALERT: {event['tx_id']} | "
            f"{amount:.2f} PLN | "
            f"{event['store']} | "
            f"{event['category']}"
        )
