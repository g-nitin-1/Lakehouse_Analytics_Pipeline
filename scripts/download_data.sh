#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/raw

BASE_URL="https://d37ci6vzurychx.cloudfront.net"
TRIP_FILE="yellow_tripdata_2024-01.parquet"
ZONE_FILE="taxi_zone_lookup.csv"

curl -L --fail "$BASE_URL/trip-data/$TRIP_FILE" -o "data/raw/$TRIP_FILE"
# Optional lookup table (not required by pipeline, useful for exploration)
curl -L --fail "$BASE_URL/misc/$ZONE_FILE" -o "data/raw/$ZONE_FILE"

echo "Downloaded: data/raw/$TRIP_FILE"
echo "Downloaded: data/raw/$ZONE_FILE"
