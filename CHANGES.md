# Changes Log

## src/shared/src/adapters/kafka/consumer.ts
**Why:** Fix Kafka batching contract with cross-partition buffering, deterministic size/timeout triggers, and post-handler offset commits.
**Before:**
```ts
  async consumeBatch(
    batchSize: number,
    timeoutMs: number,
    handler: (batch: Telemetry[]) => Promise<void>
  ): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ fromBeginning: false, topics: [this.consumerTopic || 'telemetry'] });

    await this.consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }: EachBatchPayload) => {
        if (!isRunning() || isStale()) return;

        const telemetries: Telemetry[] = [];

        for (const message of batch.messages) {
          if (!message.value) continue;
          try {
            const t: Telemetry = JSON.parse(message.value.toString('utf-8'));
            telemetries.push(t);
          } catch (err) {
            console.error('Failed to unmarshal telemetry message:', err);
            resolveOffset(message.offset);
          }
        }

        if (telemetries.length > 0) {
          try {
            await handler(telemetries);
            for (const message of batch.messages) {
              resolveOffset(message.offset);
            }
            await heartbeat();
          } catch (err) {
            console.error('Handler failed to process batch:', err);
            throw err;
          }
        }
      },
    });
  }
```
**After:**
```ts
  private async flush(): Promise<void> {
    if (this.isFlushing || this.buffer.length === 0 || !this.handler) {
      return;
    }

    this.isFlushing = true;
    const batchToProcess = this.buffer.splice(0, this.batchSize);
    this.firstBufferedAt = this.buffer.length > 0 ? Date.now() : null;

    try {
      const telemetries = batchToProcess.map((item) => item.telemetry);
      await this.handler(telemetries);

      const partitionMaxOffset = new Map<string, { topic: string; partition: number; maxOffset: bigint }>();
      for (const item of batchToProcess) {
        const key = `${item.topic}:${item.partition}`;
        const currentOffset = BigInt(item.offset);
        const existing = partitionMaxOffset.get(key);
        if (!existing || currentOffset > existing.maxOffset) {
          partitionMaxOffset.set(key, {
            topic: item.topic,
            partition: item.partition,
            maxOffset: currentOffset,
          });
        }
      }

      const offsetsToCommit = Array.from(partitionMaxOffset.values()).map((p) => ({
        topic: p.topic,
        partition: p.partition,
        offset: (p.maxOffset + 1n).toString(),
      }));

      if (offsetsToCommit.length > 0) {
        await this.consumer.commitOffsets(offsetsToCommit);
      }
    } catch (err) {
      console.error('Handler failed to process batch:', err);
    } finally {
      this.isFlushing = false;
      if (
        this.buffer.length >= this.batchSize ||
        (this.buffer.length > 0 &&
          this.firstBufferedAt !== null &&
          Date.now() - this.firstBufferedAt >= this.timeoutMs)
      ) {
        setImmediate(() => {
          this.flush().catch((e) => console.error('Error in chained flush:', e));
        });
      }
    }
  }

  async consumeBatch(
    batchSize: number = 1000,
    timeoutMs: number = 1000,
    handler: (batch: Telemetry[]) => Promise<void>
  ): Promise<void> {
    this.batchSize = batchSize;
    this.timeoutMs = timeoutMs;
    this.handler = handler;
    this.isRunning = true;

    await this.consumer.connect();
    await this.consumer.subscribe({
      fromBeginning: false,
      topics: [this.consumerTopic || 'telemetry'],
    });

    const timerInterval = Math.max(10, Math.min(100, Math.floor(timeoutMs / 10)));
    this.flushTimer = setInterval(() => {
      if (
        this.buffer.length > 0 &&
        this.firstBufferedAt !== null &&
        Date.now() - this.firstBufferedAt >= this.timeoutMs
      ) {
        this.flush().catch((err) => console.error('Error in timer flush:', err));
      }
    }, timerInterval);

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
        if (!this.isRunning) return;
        if (!message.value) return;

        let telemetry: Telemetry;
        try {
          telemetry = JSON.parse(message.value.toString('utf-8'));
        } catch (err) {
          console.error('Failed to unmarshal telemetry message:', err);
          await this.consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ]);
          return;
        }

        if (this.buffer.length === 0) {
          this.firstBufferedAt = Date.now();
        }

        this.buffer.push({
          telemetry,
          topic,
          partition,
          offset: message.offset,
        });

        if (this.buffer.length >= this.batchSize) {
          await this.flush();
        }
      },
    });
  }
```
---

## src/services/ingestion-api/src/index.ts
**Why:** Document intentional 0.0 coordinate validation improvement and support optional debug instance header for load balancing verification.
**Before:**
```ts
  if (!asset_id || typeof latitude !== 'number' || typeof longitude !== 'number') {
    return res.status(400).json({
      error: "Key: 'req.AssetID' Error:Field validation for 'AssetID' failed on the 'required' tag",
    });
  }
```
**After:**
```ts
  // Preserving typeof === 'number' to intentionally allow valid 0.0 latitude/longitude coordinates, avoiding Go's zero-value rejection quirk.
  if (!asset_id || typeof latitude !== 'number' || typeof longitude !== 'number') {
    return res.status(400).json({
      error: "Key: 'req.AssetID' Error:Field validation for 'AssetID' failed on the 'required' tag",
    });
  }

  if (process.env.DEBUG_INSTANCE_HEADER === 'true') {
    res.setHeader('X-Instance-Id', process.env.HOSTNAME || 'unknown');
  }
```
---

## src/shared/src/adapters/clickhouse/storage.ts
**Why:** Replace hardcoded 'fleet' database in metrics queries with configured database instance property.
**Before:**
```ts
      // Database size
      const dbSizeRes = await this.client.query({
        query: "SELECT formatReadableSize(sum(bytes_on_disk)) AS size FROM system.parts WHERE database = 'fleet'",
        format: 'JSONEachRow',
      });
      const dbSizeJson: Array<{ size: string }> = await dbSizeRes.json();
      metrics.database_size = dbSizeJson[0]?.size || '0 B';

      // Compression ratio multiplier
      const compStatsRes = await this.client.query({
        query: "SELECT sum(data_uncompressed_bytes) AS uncompressed, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database = 'fleet'",
        format: 'JSONEachRow',
      });
```
**After:**
```ts
      // Database size
      const dbSizeRes = await this.client.query({
        query: `SELECT formatReadableSize(sum(bytes_on_disk)) AS size FROM system.parts WHERE database = {db: String}`,
        query_params: { db: this.database },
        format: 'JSONEachRow',
      });
      const dbSizeJson: Array<{ size: string }> = await dbSizeRes.json();
      metrics.database_size = dbSizeJson[0]?.size || '0 B';

      // Compression ratio multiplier
      const compStatsRes = await this.client.query({
        query: `SELECT sum(data_uncompressed_bytes) AS uncompressed, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database = {db: String}`,
        query_params: { db: this.database },
        format: 'JSONEachRow',
      });
```
---

## api-gateway/nginx.conf
**Why:** Normalize file line endings from CRLF to LF.
**Before:**
```
<CRLF line endings>
```
**After:**
```
<LF line endings>
```
---

## docker-compose.yml
**Why:** Normalize file line endings from CRLF to LF.
**Before:**
```
<CRLF line endings>
```
**After:**
```
<LF line endings>
```
---

## ARCHITECTURE_UPDATED.md
**Why:** Remove stale reference to non-existent legacy db/init.sql file in folder structure documentation.
**Before:**
```md
├── db/                                  → Database DDL initialization scripts
│   ├── init.clickhouse.sql              → ClickHouse DDL (assets, location_history, hourly_asset_stats, MV)
│   └── init.sql                         → Legacy TimescaleDB DDL (retained for baseline diffs)
```
**After:**
```md
├── db/                                  → Database DDL initialization scripts
│   └── init.clickhouse.sql              → ClickHouse DDL (assets, location_history, hourly_asset_stats, MV)
```
---
