from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='risk-enricher-group',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def get_risk_level(amount):
    if amount > 3000:
        return "HIGH"
    elif amount > 1000:
        return "MEDIUM"
    else:
        return "LOW"

for message in consumer:
    event = message.value

    amount = event["amount"]

    event["risk_level"] = get_risk_level(amount)

    print(event)
