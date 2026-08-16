# Fleet Tracker Verification & Test Report

**Run Timestamp:** `2026-08-16T21:57:45.088Z`  
**Node.js Version:** `v24.12.0`  
**Platform / OS:** `win32 (x64)`  
**Test Framework:** `vitest v1.6.1`

---

## 1. Test Suite Pass / Fail Summary

| Test File | Status | Passed | Failed | Total Tests | Duration |
|:---|:---:|:---:|:---:|:---:|:---:|
| `test/integration/architecture-flow.integration.test.ts` | ✅ PASS | 1 | 0 | 1 | 2.95s |
| `test/integration/kafka-batching.integration.test.ts` | ✅ PASS | 3 | 0 | 3 | 15.54s |
| `test/integration/load-test.integration.test.ts` | ✅ PASS | 1 | 0 | 1 | 12.73s |
| `src/shared/src/domain/__tests__/models.test.ts` | ✅ PASS | 2 | 0 | 2 | 0.01s |
| `src/shared/src/adapters/__tests__/adapters.test.ts` | ✅ PASS | 2 | 0 | 2 | 0.01s |
| `src/shared/src/adapters/__tests__/kafka-consumer-batching.test.ts` | ✅ PASS | 5 | 0 | 5 | 0.05s |

**Overall Status:** ✅ **ALL TESTS PASSED**  
**Total Tests Executed:** 14 (14 passed, 0 failed)

---

## 2. Environment & Service Topology

The verification suite ran against the live Docker Compose topology:

```
NAME                                                   IMAGE                                                COMMAND                  SERVICE              CREATED          STATUS                    PORTS
api-gateway                                            high-throughput-tracker-backend-api-gateway          "/docker-entrypoint.…"   api-gateway          12 minutes ago   Up 12 minutes             0.0.0.0:80->80/tcp, [::]:80->80/tcp
fleet-clickhouse                                       clickhouse/clickhouse-server:24.3-alpine             "/entrypoint.sh"         clickhouse           6 days ago       Up 12 minutes (healthy)   0.0.0.0:8123->8123/tcp, [::]:8123->8123/tcp, 0.0.0.0:9000->9000/tcp, [::]:9000->9000/tcp
fleet-redis                                            redis:7-alpine                                       "docker-entrypoint.s…"   redis                6 days ago       Up 12 minutes             0.0.0.0:6379->6379/tcp, [::]:6379->6379/tcp
high-throughput-tracker-backend-ingestion-api-1-1      high-throughput-tracker-backend-ingestion-api-1      "docker-entrypoint.s…"   ingestion-api-1      12 minutes ago   Up 12 minutes             8080/tcp
high-throughput-tracker-backend-ingestion-api-2-1      high-throughput-tracker-backend-ingestion-api-2      "docker-entrypoint.s…"   ingestion-api-2      12 minutes ago   Up 12 minutes             8080/tcp
high-throughput-tracker-backend-ingestion-api-3-1      high-throughput-tracker-backend-ingestion-api-3      "docker-entrypoint.s…"   ingestion-api-3      12 minutes ago   Up 12 minutes             8080/tcp
high-throughput-tracker-backend-stream-processor-1-1   high-throughput-tracker-backend-stream-processor-1   "docker-entrypoint.s…"   stream-processor-1   12 minutes ago   Up 12 minutes             
high-throughput-tracker-backend-stream-processor-2-1   high-throughput-tracker-backend-stream-processor-2   "docker-entrypoint.s…"   stream-processor-2   12 minutes ago   Up 12 minutes             
high-throughput-tracker-backend-stream-processor-3-1   high-throughput-tracker-backend-stream-processor-3   "docker-entrypoint.s…"   stream-processor-3   12 minutes ago   Up 12 minutes             
query-api                                              high-throughput-tracker-backend-query-api            "docker-entrypoint.s…"   query-api            12 minutes ago   Up 12 minutes             0.0.0.0:8081->8081/tcp, [::]:8081->8081/tcp
redpanda-broker                                        docker.redpanda.com/redpandadata/redpanda:latest     "/entrypoint.sh redp…"   redpanda             6 days ago       Up 12 minutes (healthy)   0.0.0.0:18081-18082->18081-18082/tcp, [::]:18081-18082->18081-18082/tcp, 0.0.0.0:19092->19092/tcp, [::]:19092->19092/tcp, 0.0.0.0:19644->9644/tcp, [::]:19644->9644/tcp
```

- **Nginx API Gateway:** Port `80` (Round-Robin Ingestion Cluster + Query Cluster proxy)
- **Ingestion API:** 3 Replicas (`ingestion-api-1`, `ingestion-api-2`, `ingestion-api-3`)
- **Redpanda / Kafka:** 10 Partitions on topic `telemetry`, external port `19092`
- **Stream Processor:** 3 Replicas (`stream-processor-1`, `stream-processor-2`, `stream-processor-3`) in consumer group `stream-processor-group`
- **ClickHouse:** Port `8123` / `9000` (`fleet.location_history` ReplacingMergeTree table)
- **Redis:** Port `6379` (Caching latest location + Pub/Sub)
- **Query API:** Port `8081` (Location queries, route history, system metrics)

---

## 3. Ingestion Load Test Metrics (10,000 Messages)

| Metric | Measured Value | Target / Specification |
|:---|:---:|:---:|
| **Total Requests Sent** | **10,000** | 10,000 |
| **HTTP Accepted (202)** | **10000** | 10,000 (100%) |
| **Failed Requests** | **0** | 0 (0%) |
| **Throughput** | **872.52 req/s** | > 500 req/s |
| **Average Latency** | **91.44 ms** | < 200 ms |
| **Total Load Duration** | **11.46s** | Bounded |

### Ingestion API Load Balancing Distribution
Traffic distribution across the 3 `ingestion-api` containers behind Nginx:

| Replica Container ID | Routed Requests | Traffic Percentage | Target Window |
|:---|:---:|:---:|:---:|
| `72a5bf3b295d` | 3334 | **33.3%** | 20.0% – 45.0% |
| `45a20454d9e8` | 3333 | **33.3%** | 20.0% – 45.0% |
| `6eb1a2f14d7b` | 3333 | **33.3%** | 20.0% – 45.0% |

---

## 4. Kafka Batching Contract & Stream Processor Metrics

The revised `KafkaConsumer` accumulates telemetry across all assigned partitions in-memory and executes flushes deterministically at 1,000 messages or 1,000ms elapsed time.

| Batching Metric | Value | Verification |
|:---|:---:|:---|
| **Total Flushes Sampled** | **182** | Active stream-processor consumer flushes |
| **Minimum Observed Batch Size** | **1** | Flushed on quiet / lull condition |
| **Maximum Observed Batch Size** | **621** | Strict enforcement <= 1,000 messages |
| **Median Batch Size** | **274** | Efficient multi-message batching |
| **Average Batch Size** | **274.7** | Substantially higher than 1 msg/batch |
| **Max Batch Limit Exceeded (> 1,000)** | **NO (PASS)** | Confirmed batch limit respected |
| **Cross-Partition Batching** | **VERIFIED** | Unit & Real broker tests confirm multi-partition batches |
| **Post-Handler Offset Commit** | **VERIFIED** | Offsets committed only after ClickHouse insert succeeds |

---

## 5. ClickHouse Data Integrity & Persistence

| Item | Value | Status |
|:---|:---:|:---:|
| **Expected Row Count** | `10000` | 10,000 Accepted Requests |
| **ClickHouse Query Count** | `10000` | Query on `fleet.location_history` |
| **Kafka Consumer Lag Drain** | `0` | Lag drained to 0 after ingestion completed |
| **Eventual Merge Tree Integrity** | **100% Match** | All accepted telemetry points persisted |

---

## 6. Architecture Data-Flow Verification

The end-to-end flow test (`test/integration/architecture-flow.integration.test.ts`) verified every topology connection in `ARCHITECTURE_UPDATED.md`:
1. **Telemetry Ingestion**: `POST /api/v1/telemetry` through Nginx returns `202 Accepted`.
2. **Redis Real-Time Cache**: `GET /api/v1/assets/:id/location` retrieves latest coordinates updated by stream-processor.
3. **ClickHouse Historical Storage**: `GET /api/v1/assets/:id/route` returns time-series history query.
4. **Unified System Metrics**: `GET /api/v1/system/metrics` returns `status: "healthy"` with non-empty stats from TimescaleDB/ClickHouse, Redis, Kafka, and Nginx.

---

## 7. Known Issues & Follow-Ups

- **None**: All unit tests, integration tests, load tests, and end-to-end data flow tests passed with 100% success rate.
