# Fleet Tracker API Contract Specification

This document defines the exact frozen request/response schemas, paths, HTTP methods, headers, and status codes for all public endpoints in Fleet Tracker. This spec is extracted directly from the Go implementation (`cmd/ingestion-api/main.go` and `cmd/query-api/main.go`) to ensure 100% byte-for-byte behavioral compatibility across the migration.

---

## 1. Telemetry Ingestion API

### `POST /api/v1/telemetry`

Accepts raw telemetry ping from assets and produces to Kafka asynchronously.

- **HTTP Method:** `POST`
- **Path:** `/api/v1/telemetry`
- **Request Headers:**
  - `Content-Type: application/json`

#### Request Body Schema

```json
{
  "asset_id": "string (required)",
  "latitude": 40.7128,
  "longitude": -74.0060
}
```

#### Response Cases

1. **Validation Error (HTTP 400 Bad Request)**
   - Triggered when `asset_id`, `latitude`, or `longitude` are missing or malformed.
   - Response Body:
     ```json
     {
       "error": "Key: 'req.AssetID' Error:Field validation for 'AssetID' failed on the 'required' tag"
     }
     ```

2. **Producer Error (HTTP 500 Internal Server Error)**
   - Triggered if Kafka message publishing fails.
   - Response Body:
     ```json
     {
       "error": "Internal Server Error"
     }
     ```

3. **Success (HTTP 202 Accepted)**
   - Triggered when event is successfully queued to Kafka.
   - Response Body:
     ```json
     {
       "status": "accepted"
     }
     ```

---

## 2. Query API: Latest Location

### `GET /api/v1/assets/:id/location`

Fetches the latest recorded location for a specific asset from the Redis cache.

- **HTTP Method:** `GET`
- **Path:** `/api/v1/assets/:id/location` (Path parameter `id`: asset ID string)

#### Response Cases

1. **Not Found (HTTP 404 Not Found)**
   - Triggered if no location cache key `asset:{id}:latest` exists in Redis.
   - Response Body:
     ```json
     {
       "error": "asset not found or no location reported"
     }
     ```

2. **Internal Error (HTTP 500 Internal Server Error)**
   - Response Body:
     ```json
     {
       "error": "<error message>"
     }
     ```

3. **Success (HTTP 200 OK)**
   - Response Body (RFC3339 timestamp):
     ```json
     {
       "asset_id": "asset-1",
       "latitude": 40.7128,
       "longitude": -74.0060,
       "timestamp": "2026-08-10T14:15:00.000Z"
     }
     ```

---

## 3. Query API: Route History

### `GET /api/v1/assets/:id/route`

Fetches historical location points for an asset within a given time range from historical database storage, ordered by timestamp ascending.

- **HTTP Method:** `GET`
- **Path:** `/api/v1/assets/:id/route`
- **Query Parameters:**
  - `start` (required): ISO-8601 / RFC3339 start timestamp string (e.g. `2026-08-10T00:00:00Z`)
  - `end` (required): ISO-8601 / RFC3339 end timestamp string (e.g. `2026-08-10T23:59:59Z`)

#### Response Cases

1. **Missing Query Parameters (HTTP 400 Bad Request)**
   - Triggered if `start` or `end` query parameter is empty.
   - Response Body:
     ```json
     {
       "error": "start and end timestamps are required"
     }
     ```

2. **Internal Error (HTTP 500 Internal Server Error)**
   - Response Body:
     ```json
     {
       "error": "<error message>"
     }
     ```

3. **Success (HTTP 200 OK)**
   - Response Body (Array of Telemetry records, ordered by `time ASC`):
     ```json
     [
       {
         "asset_id": "asset-1",
         "latitude": 40.7128,
         "longitude": -74.0060,
         "timestamp": "2026-08-10T10:00:00.000Z"
       },
       {
         "asset_id": "asset-1",
         "latitude": 40.7135,
         "longitude": -74.0055,
         "timestamp": "2026-08-10T10:01:00.000Z"
       }
     ]
     ```

---

## 4. System Metrics API

### `GET /api/v1/system/metrics`

Aggregates operational metrics across Database, Cache, Message Queue, and Nginx Gateway.

- **HTTP Method:** `GET`
- **Path:** `/api/v1/system/metrics`

#### Response Cases

1. **Success (HTTP 200 OK)**
   - Response Body:
     ```json
     {
       "status": "healthy",
       "database_timescaledb": {
         "total_telemetry_rows": 125000,
         "registered_assets": 100000,
         "active_db_connections": 4,
         "database_size": "15 MiB",
         "compression_ratio_multiplier": "2.40x"
       },
       "cache_redis": {
         "memory_used": "1.2M",
         "connected_clients": "5",
         "ops_per_second": "120",
         "lifetime_connections": "15",
         "total_active_assets": 100000
       },
       "queue_kafka": {
         "broker": "redpanda:9092",
         "topic": "telemetry",
         "status": "online",
         "total_partitions": 10,
         "partition_0_oldest_offset": 0,
         "partition_0_latest_offset": 12500,
         "partition_0_total_messages_ingested": 12500
       },
       "gateway_nginx": {
         "active_connections": "3",
         "accepted_connections": "150000",
         "handled_connections": "150000",
         "total_requests_routed": "150000",
         "current_state": "Reading: 0 Writing: 1 Waiting: 2"
       }
     }
     ```
