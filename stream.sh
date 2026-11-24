#!/usr/bin/env bash
set -e

# ==============================================================================
# All-in-One Automated Streaming & Real-Time Scoring Runner
# Usage: ./stream.sh [NUM_SAMPLES] [DELAY_SECONDS]
# Example: ./stream.sh 100 0.1
# ==============================================================================

SAMPLES=${1:-50}
DELAY=${2:-0.2}

# Move to script project root
cd "$(dirname "$0")"

# 1. Kill any previously hanging consumers or background stream processes
pkill -f "backend/streaming/consumer.py" 2>/dev/null || true
pkill -f "backend/streaming/producer.py" 2>/dev/null || true

# 2. Check / Ensure docker containers (Kafka, API) are running
if ! docker ps | grep -q "forest_cover_mlops-api" && ! docker ps | grep -q "forest_api"; then
    echo "Starting local services (Kafka KRaft, FastAPI Engine)..."
    docker compose up -d --no-recreate
    echo "Waiting 5 seconds for services to become healthy..."
    sleep 5
fi

# 3. Create Kafka topic if not exists (quiet)
docker exec $(docker ps -q -f name=forest_kafka_broker) /bin/kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic terrain_survey.raw --partitions 1 --replication-factor 1 2>/dev/null || true

echo "======================================================================"
echo "🌲 STREAMING & REAL-TIME PREDICTION"
echo "Target: $SAMPLES predictions | Delay: ${DELAY}s between records"
echo "Dashboard: https://charmingjuniper1515.grafana.net/d/forest-cover-mlops-cloud/forest-cover-mlops-production-dashboard"
echo "======================================================================"

# 4. Start Consumer in background and redirect output cleanly
python3 backend/streaming/consumer.py &
CONSUMER_PID=$!

cleanup() {
    # Let remaining queue flush, then kill consumer
    sleep 2
    kill $CONSUMER_PID 2>/dev/null || true
    echo ""
    echo "======================================================================"
    echo "✅ COMPLETE: Stream finished and written to Aiven PostgreSQL!"
    echo "👉 Check live Grafana: https://charmingjuniper1515.grafana.net/d/forest-cover-mlops-cloud/forest-cover-mlops-production-dashboard"
    echo "======================================================================"
}
trap cleanup EXIT INT TERM

# Give consumer 3s to connect & assign partitions
sleep 3

# 5. Run Producer to push events
python3 backend/streaming/producer.py --batch-size "$SAMPLES" --delay "$DELAY"

# Wait for consumer to process all messages
echo "Flushing inferences to Aiven PostgreSQL database..."
sleep 3
