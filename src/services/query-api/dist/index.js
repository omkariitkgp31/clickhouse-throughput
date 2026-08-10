import express from 'express';
import { Kafka } from 'kafkajs';
import { ClickHouseStorage, RedisCache } from '@fleet-tracker/shared';
const port = parseInt(process.env.PORT || '8081', 10);
const redisAddr = process.env.REDIS_ADDR || 'localhost:6379';
const kafkaBroker = process.env.KAFKA_BROKERS || 'redpanda:9092';
const kafkaTopic = process.env.KAFKA_TOPIC || 'telemetry';
const clickhouseUrl = process.env.CLICKHOUSE_URL || 'http://localhost:8123';
const clickhouseDb = process.env.CLICKHOUSE_DB || 'fleet';
const clickhouseUser = process.env.CLICKHOUSE_USER || 'default';
const clickhousePassword = process.env.CLICKHOUSE_PASSWORD || '';
const storage = new ClickHouseStorage(clickhouseUrl, clickhouseDb, clickhouseUser, clickhousePassword);
const cache = new RedisCache(redisAddr);
const app = express();
app.use(express.json());
app.get('/api/v1/assets/:id/location', async (req, res) => {
    const assetId = req.params.id;
    try {
        const loc = await cache.getLatestLocation(assetId);
        if (!loc) {
            return res.status(404).json({ error: 'asset not found or no location reported' });
        }
        return res.status(200).json(loc);
    }
    catch (err) {
        return res.status(500).json({ error: err.message || String(err) });
    }
});
app.get('/api/v1/assets/:id/route', async (req, res) => {
    const assetId = req.params.id;
    const start = req.query.start;
    const end = req.query.end;
    if (!start || !end) {
        return res.status(400).json({ error: 'start and end timestamps are required' });
    }
    try {
        const history = await storage.getRouteHistory(assetId, start, end);
        return res.status(200).json(history || []);
    }
    catch (err) {
        return res.status(500).json({ error: err.message || String(err) });
    }
});
async function getNginxMetrics() {
    const metrics = {};
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 3000);
        const res = await fetch('http://api-gateway/nginx_status', { signal: controller.signal });
        clearTimeout(timeout);
        if (res.ok) {
            const text = await res.text();
            const lines = text.split('\n');
            if (lines.length >= 4) {
                metrics.active_connections = lines[0].split(':')[1]?.trim() || '0';
                const counts = lines[2].trim().split(/\s+/);
                if (counts.length >= 3) {
                    metrics.accepted_connections = counts[0];
                    metrics.handled_connections = counts[1];
                    metrics.total_requests_routed = counts[2];
                }
                metrics.current_state = lines[3].trim();
            }
        }
        else {
            metrics.status = 'offline or unreachable';
        }
    }
    catch {
        metrics.status = 'offline or unreachable';
    }
    return metrics;
}
async function getKafkaMetrics(brokerStr, topic) {
    const brokers = brokerStr.split(',');
    const metrics = {
        broker: brokers[0],
        topic,
        status: 'offline',
    };
    const kafka = new Kafka({ clientId: 'query-api-metrics', brokers });
    const admin = kafka.admin();
    try {
        await admin.connect();
        metrics.status = 'online';
        const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
        const partitions = metadata.topics[0]?.partitions || [];
        metrics.total_partitions = partitions.length;
        if (partitions.length > 0) {
            const topicOffsets = await admin.fetchTopicOffsets(topic);
            const part0 = topicOffsets.find((p) => p.partition === 0);
            const latestOffset = part0 ? parseInt(part0.high, 10) : 0;
            const oldestOffset = part0 ? parseInt(part0.low, 10) : 0;
            metrics.partition_0_oldest_offset = oldestOffset;
            metrics.partition_0_latest_offset = latestOffset;
            metrics.partition_0_total_messages_ingested = latestOffset - oldestOffset;
        }
        await admin.disconnect();
    }
    catch (err) {
        metrics.status = 'offline';
        metrics.error = err.message || String(err);
    }
    return metrics;
}
app.get('/api/v1/system/metrics', async (req, res) => {
    try {
        const [dbMetrics, cacheMetrics, kafkaMetrics, nginxMetrics] = await Promise.all([
            storage.getDBMetrics(),
            cache.getCacheMetrics(),
            getKafkaMetrics(kafkaBroker, kafkaTopic),
            getNginxMetrics(),
        ]);
        return res.status(200).json({
            status: 'healthy',
            database_timescaledb: dbMetrics,
            cache_redis: cacheMetrics,
            queue_kafka: kafkaMetrics,
            gateway_nginx: nginxMetrics,
        });
    }
    catch (err) {
        return res.status(500).json({ error: err.message || String(err) });
    }
});
const server = app.listen(port, () => {
    console.log(`Query API server listening on port ${port}`);
});
process.on('SIGTERM', async () => {
    console.log('Shutting down Query API...');
    server.close();
    await cache.close();
    await storage.close();
    process.exit(0);
});
//# sourceMappingURL=index.js.map