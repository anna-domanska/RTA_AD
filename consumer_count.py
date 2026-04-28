from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

for message in consumer:
    event = message.value

    store = event["store"]
    amount = event["amount"]

    store_counts[store] += 1

    if store not in total_amount:
        total_amount[store] = 0
    total_amount[store] += amount

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n--- PODSUMOWANIE ---")
        print("Sklep | Liczba | Suma | Średnia")

        for s in store_counts:
            count = store_counts[s]
            total = total_amount[s]
            avg = total / count

            print(f"{s} | {count} | {total:.2f} | {avg:.2f}")
