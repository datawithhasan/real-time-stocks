import json
import time
import uuid
import boto3
from botocore.client import Config
from kafka import KafkaConsumer

# ----------------------------
# MinIO Connection (S3 API)
# ----------------------------
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",  # MinIO S3 endpoint mapped to host
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)

bucket_name = "real-time-stocks-raw"

# Ensure bucket exists (idempotent)
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' already exists.")
except Exception:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket '{bucket_name}'.")

# ----------------------------
# Kafka Consumer (running on Mac host)
# ----------------------------
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=["localhost:29092"],   # host machine uses localhost
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze-consumer1",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("Consuming from Kafka and saving to MinIO...")

for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")

    ts = record.get("fetched_at", int(time.time()))
    # Add uniqueness so we never overwrite
    unique = uuid.uuid4().hex

    # Optional: partition-like foldering (good habit)
    date_str = time.strftime("%Y%m%d", time.gmtime(ts))
    hour_str = time.strftime("%H", time.gmtime(ts))

    key = f"raw/date={date_str}/hour={hour_str}/symbol={symbol}/{ts}-{unique}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Saved {symbol} -> s3://{bucket_name}/{key}")
