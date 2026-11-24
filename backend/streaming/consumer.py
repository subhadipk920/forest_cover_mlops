import os
import json
import time
import requests
import psycopg2
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer

# Load local .env if present
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "terrain_survey.raw")
TOPIC_PRED = os.getenv("KAFKA_TOPIC_PRED", "terrain_survey.predictions")
API_URL = os.getenv("API_PREDICT_URL", "http://localhost:8000/predict")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "forest-scoring-group")

def get_db_connection():
    global pg_conn
    if pg_conn and not pg_conn.closed:
        return pg_conn
    if PG_URI and "aivencloud.com" in PG_URI:
        try:
            pg_conn = psycopg2.connect(PG_URI)
            with pg_conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS model_inferences (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMPTZ DEFAULT NOW(),
                        elevation INT,
                        predicted_class INT,
                        tree_species TEXT,
                        confidence FLOAT,
                        latency_ms FLOAT
                    );
                """)
                pg_conn.commit()
            print("Connected to Aiven PostgreSQL for live Grafana dashboard metrics!")
            return pg_conn
        except Exception as e:
            print(f"PostgreSQL connection note: {e}")
            pg_conn = None
            return None
    return None

# Initial connection attempt
get_db_connection()

def connect_consumer():
    """Initializes and returns a Confluent Kafka Consumer."""
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    print(f"Connecting Consumer to {KAFKA_BROKER} on topic '{TOPIC_RAW}'...")
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_RAW])
    print("Connected to Kafka Consumer successfully!")
    return consumer

def connect_producer():
    """Initializes and returns a Confluent Kafka Producer for scored predictions."""
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'forest-scoring-producer'
    }
    return Producer(conf)

def start_scoring_service():
    """
    Subscribes to raw terrain surveys, requests real-time model inference from
    FastAPI (/predict), and publishes enriched predictions to predictions topic.
    """
    print("=" * 70)
    print("STARTING REAL-TIME KAFKA STREAMING CONSUMER & INFERENCE PIPELINE")
    print(f"Inference Endpoint: {API_URL}")
    print("=" * 70)

    consumer = connect_consumer()
    producer = connect_producer()
    records_processed = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                raw_payload = json.loads(msg.value().decode('utf-8'))
                
                # Strip actual target if present for clean scoring request
                scoring_input = {k: v for k, v in raw_payload.items() if k != "Cover_Type"}

                # Call FastAPI model inference engine
                resp = requests.post(API_URL, json=scoring_input, timeout=5.0)

                if resp.status_code == 200:
                    pred_data = resp.json()
                    enriched_event = {
                        "timestamp": time.time(),
                        "elevation": scoring_input.get("Elevation"),
                        "predicted_cover_type": pred_data.get("cover_type_id"),
                        "tree_species": pred_data.get("tree_species"),
                        "confidence": pred_data.get("confidence"),
                        "latency_ms": pred_data.get("latency_ms"),
                        "ground_truth": raw_payload.get("Cover_Type")
                    }

                    # Publish scored event to output Kafka topic
                    producer.produce(
                        topic=TOPIC_PRED,
                        value=json.dumps(enriched_event).encode('utf-8')
                    )

                    # Persist to PostgreSQL for Grafana SQL Dashboard
                    conn = get_db_connection()
                    if conn:
                        try:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    INSERT INTO model_inferences (elevation, predicted_class, tree_species, confidence, latency_ms)
                                    VALUES (%s, %s, %s, %s, %s);
                                """, (
                                    enriched_event['elevation'],
                                    enriched_event['predicted_cover_type'],
                                    enriched_event['tree_species'],
                                    enriched_event['confidence'],
                                    enriched_event['latency_ms']
                                ))
                                conn.commit()
                        except Exception as pg_err:
                            print(f"DB insert retry error: {pg_err}")
                            pg_conn = None
                    records_processed += 1

                    print(
                        f"🌲 [{records_processed}] Scored Survey: Elev={enriched_event['elevation']}m -> "
                        f"Class: {enriched_event['tree_species']} ({enriched_event['predicted_cover_type']}) | "
                        f"Conf: {enriched_event['confidence']:.2f} | Latency: {enriched_event['latency_ms']:.1f}ms"
                    )
                else:
                    print(f"⚠️ FastAPI returned error: {resp.status_code} - {resp.text}")

            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print(f"\nConsumer stopped. Total records scored: {records_processed}")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    start_scoring_service()
