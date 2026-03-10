"""
UPI Batch Generator
Writes raw JSON files to /data/raw/upi every BATCH_INTERVAL_SECONDS seconds.
Each file = one micro-batch of UPI transactions.
"""
import json, os, time, uuid
from datetime import datetime, timezone
from faker import Faker
from pathlib import Path

fake = Faker("en_IN")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/data/raw/upi"))
INTERVAL = int(os.getenv("BATCH_INTERVAL_SECONDS", 60))
RECORDS = int(os.getenv("RECORDS_PER_BATCH", 500))

BANKS = ["HDFC", "SBI", "ICICI", "AXIS", "KOTAK", "PNB", "BOB", "CANARA"]
STATUS = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"]
CATEGORIES = ["GROCERY", "UTILITIES", "FOOD", "TRANSPORT", "ENTERTAINMENT",
               "MEDICAL", "EDUCATION", "SHOPPING", "FUEL", "RENT"]

def generate_txn():
    sender_bank = fake.random_element(BANKS)
    return {
        "txn_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sender_upi": f"{fake.user_name()}@{sender_bank.lower()}",
        "receiver_upi": f"{fake.user_name()}@{fake.random_element(BANKS).lower()}",
        "sender_bank": sender_bank,
        "receiver_bank": fake.random_element(BANKS),
        "amount": round(fake.pyfloat(min_value=1, max_value=50000, right_digits=2), 2),
        "currency": "INR",
        "status": fake.random_element(STATUS),
        "category": fake.random_element(CATEGORIES),
        "device_type": fake.random_element(["MOBILE", "WEB", "POS"]),
        "ip_address": fake.ipv4(),
        "city": fake.city(),
        "state": fake.state(),
        "remarks": fake.sentence(nb_words=4),
        "batch_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
print(f"UPI generator started → {OUTPUT_DIR} | {RECORDS} records every {INTERVAL}s")

while True:
    batch = [generate_txn() for _ in range(RECORDS)]
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = OUTPUT_DIR / f"upi_batch_{ts}.json"
    with open(file_path, "w") as f:
        json.dump(batch, f, default=str)
    print(f"[UPI] Wrote {RECORDS} records → {file_path.name}")
    time.sleep(INTERVAL)