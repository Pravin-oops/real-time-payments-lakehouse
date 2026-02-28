import os
import json
import random
import time
from datetime import datetime
import boto3
from dotenv import load_dotenv

# ==========================================
# 0. DYNAMIC PATH RESOLUTION
# ==========================================
# Finds the directory where this script lives (.../scripts)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Goes up one level to the project root (.../REAL-TIME-PAYMENTS-LAKEHOUSE)
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

# ==========================================
# 1. CONFIGURATION & SECRETS
# ==========================================
# Explicitly load the .env file from the root directory
load_dotenv(os.path.join(ROOT_DIR, '.env'))

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FOLDER_NAME = "raw_upi_data/"

# Initialize the AWS S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# ==========================================
# 2. LOCAL DATA FOLDER SETUP
# ==========================================
# Create the temporary data folder inside the root 'data' folder
OUTPUT_DIR = os.path.join(ROOT_DIR, "data", "temp_upi_batches")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==========================================
# 3. UPI DATA GENERATOR LOGIC
# ==========================================
def generate_upi_batch(batch_size=5):
    banks = ["okicici", "ybl", "paytm", "okhdfcbank", "sbi"]
    names = ["pravin", "rahul", "priya", "amit", "neha", "suresh", "kavita", "arjun"]
    merchants = ["amazon", "flipkart", "zomato", "swiggy", "ubereats", "reliance"]
    
    transactions = []
    
    for _ in range(batch_size):
        sender = f"{random.choice(names)}{random.randint(100,999)}@{random.choice(banks)}"
        receiver = f"{random.choice(merchants)}@{random.choice(banks)}"
        
        txn = {
            "txn_id": f"UPI-{random.randint(1000000000, 9999999999)}",
            "sender_vpa": sender,
            "receiver_vpa": receiver,
            "amount": round(random.uniform(50.0, 15000.0), 2),
            "currency": "INR",
            "status": random.choices(["SUCCESS", "FAILED", "PENDING"], weights=[85, 10, 5], k=1)[0],
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        transactions.append(txn)
        
    return transactions

# ==========================================
# 4. THE CLOUD GENERATOR LOOP
# ==========================================
print("🚀 Starting Cloud UPI Generator... Press Ctrl+C to stop.")

try:
    while True:
        # Generate 5 transactions
        batch_data = generate_upi_batch(5)
        
        # Create a unique filename based on the current time
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"upi_batch_{timestamp_str}.json"
        local_path = os.path.join(OUTPUT_DIR, filename)
        s3_path = f"{S3_FOLDER_NAME}{filename}"
        
        # 1. Save to a local temporary JSON file
        with open(local_path, 'w') as f:
            json.dump(batch_data, f, indent=4)
            
        # 2. Upload that file to AWS S3
        s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_path)
        print(f"☁️ Uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_path}")
        
        # 3. Delete the local temp file to save space
        os.remove(local_path)
        
        # Wait 10 seconds before generating the next file
        time.sleep(10)

except KeyboardInterrupt:
    print("\n🛑 Generator stopped. No more files will be created.")
except Exception as e:
    print(f"\n❌ An error occurred: {e}")