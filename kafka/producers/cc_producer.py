"""
Credit Card Swipe Stream Producer
Publishes synthetic CC swipe events to Kafka topic at CC_STREAM_RATE_MS interval.
"""
import json, os, time, uuid, random
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()
BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC  = os.getenv("KAFKA_TOPIC", "credit-card-swipes")
RATE_MS = int(os.getenv("STREAM_RATE_MS", 200))

MERCHANTS = ["Amazon", "Walmart", "Starbucks", "McDonald's", "Shell", "Netflix",
             "Apple Store", "Target", "Home Depot", "Uber", "Lyft", "DoorDash"]
CARD_TYPES = ["VISA", "MASTERCARD", "AMEX", "RUPAY", "DISCOVER"]
MCC_CODES = {"5411": "Grocery", "5812": "Restaurant", "5541": "Gas",
             "5999": "Miscellaneous", "4816": "Streaming", "7011": "Hotel",
             "4111": "Transport", "5945": "Electronics"}

def make_swipe():
    mcc, mcc_desc = random.choice(list(MCC_CODES.items()))
    amount = round(random.uniform(0.99, 9999.99), 2)
    is_fraud = random.random() < 0.02  # 2% fraud rate
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "card_token": f"TOK_{uuid.uuid4().hex[:16].upper()}",
        "card_type": random.choice(CARD_TYPES),
        "last_four": str(random.randint(1000, 9999)),
        "cardholder_name": fake.name(),
        "merchant_name": random.choice(MERCHANTS),
        "merchant_id": f"MID_{uuid.uuid4().hex[:8].upper()}",
        "mcc": mcc,
        "mcc_description": mcc_desc,
        "amount": amount,
        "currency": "USD",
        "terminal_id": f"TERM_{random.randint(1000, 9999)}",
        "pos_entry_mode": random.choice(["CHIP", "TAP", "SWIPE", "MANUAL"]),
        "country": fake.country_code(),
        "city": fake.city(),
        "zip_code": fake.zipcode(),
        "approval_code": f"{random.randint(100000, 999999)}",
        "response_code": "00" if not is_fraud else random.choice(["05", "14", "51"]),
        "is_declined": is_fraud,
        "fraud_score": round(random.uniform(0.7, 1.0) if is_fraud else random.uniform(0.0, 0.3), 4),
        "network_latency_ms": random.randint(50, 800),
    }

def wait_for_kafka(retries=20, delay=5):
    for i in range(retries):
        try:
            p = KafkaProducer(bootstrap_servers=BROKER,
                              value_serializer=lambda v: json.dumps(v, default=str).encode())
            print(f"[CC] Connected to Kafka at {BROKER}")
            return p
        except NoBrokersAvailable:
            print(f"[CC] Kafka not ready, retry {i+1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")

producer = wait_for_kafka()
count = 0
print(f"[CC] Streaming to topic '{TOPIC}' at {RATE_MS}ms intervals...")

while True:
    event = make_swipe()
    producer.send(TOPIC, value=event, key=event["card_token"].encode())
    count += 1
    if count % 100 == 0:
        producer.flush()
        print(f"[CC] {count} events published to {TOPIC}")
    time.sleep(RATE_MS / 1000)