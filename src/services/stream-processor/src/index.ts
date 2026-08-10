import { KafkaConsumer, ClickHouseStorage, RedisCache, Telemetry } from '@fleet-tracker/shared';

const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:19092').split(',');
const kafkaTopic = process.env.KAFKA_TOPIC || 'telemetry';
const kafkaGroup = process.env.KAFKA_GROUP || 'stream-processor-group';
const redisAddr = process.env.REDIS_ADDR || 'localhost:6379';

const clickhouseUrl = process.env.CLICKHOUSE_URL || 'http://localhost:8123';
const clickhouseDb = process.env.CLICKHOUSE_DB || 'fleet';
const clickhouseUser = process.env.CLICKHOUSE_USER || 'default';
const clickhousePassword = process.env.CLICKHOUSE_PASSWORD || '';

const storage = new ClickHouseStorage(clickhouseUrl, clickhouseDb, clickhouseUser, clickhousePassword);
const cache = new RedisCache(redisAddr);
const consumer = new KafkaConsumer(kafkaBrokers, kafkaTopic, kafkaGroup);

console.log('Stream Processor started. Listening for telemetry...');

async function run() {
  await consumer.start(kafkaTopic, async (batch: Telemetry[]) => {
    console.log(`Processing batch of ${batch.length} telemetry records`);

    // 1. Insert batch into ClickHouse
    try {
      await storage.insertBatch(batch);
    } catch (err) {
      console.error('Error inserting batch to ClickHouse:', err);
      throw err;
    }

    // 2. Update Redis latest location + Pub/Sub channel for each asset
    for (const t of batch) {
      try {
        await cache.setLatestLocation(t);
      } catch (err) {
        console.error(`Error updating redis for asset ${t.asset_id}:`, err);
      }
      try {
        await cache.publishLocation(t);
      } catch (err) {
        console.error(`Error publishing to redis for asset ${t.asset_id}:`, err);
      }
    }
  });
}

run().catch((err) => {
  console.error('Fatal error in stream processor:', err);
  process.exit(1);
});

process.on('SIGTERM', async () => {
  console.log('Shutting down Stream Processor...');
  await consumer.close();
  await cache.close();
  await storage.close();
  process.exit(0);
});
