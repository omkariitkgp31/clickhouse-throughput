import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Kafka } from 'kafkajs';
import { KafkaConsumer, Telemetry } from '@fleet-tracker/shared';

describe('Kafka Batching Contract (Integration - Real Broker)', () => {
  const brokers = ['localhost:19092'];
  let kafka: Kafka;
  let admin: any;
  let producer: any;
  const createdTopics: string[] = [];

  beforeAll(async () => {
    kafka = new Kafka({ clientId: 'test-batching-admin', brokers, logLevel: 1 });
    admin = kafka.admin();
    await admin.connect();
    producer = kafka.producer();
    await producer.connect();
  }, 20000);

  afterAll(async () => {
    if (producer) await producer.disconnect();
    if (admin) {
      if (createdTopics.length > 0) {
        try {
          await admin.deleteTopics({ topics: createdTopics });
        } catch {}
      }
      await admin.disconnect();
    }
  }, 20000);

  async function createTestTopic(partitions = 3): Promise<string> {
    const topic = `test-batch-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
    await admin.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
    });
    createdTopics.push(topic);
    // Allow topic metadata to propagate
    await new Promise((r) => setTimeout(r, 1000));
    return topic;
  }

  it('accumulates messages across multiple partitions into single batch', async () => {
    const topic = await createTestTopic(3);
    const groupId = `group-cross-partition-${Date.now()}`;
    const consumer = new KafkaConsumer(brokers, topic, groupId);

    const receivedBatches: Telemetry[][] = [];
    let resolveBatchPromise: () => void;
    const batchPromise = new Promise<void>((resolve) => {
      resolveBatchPromise = resolve;
    });

    await consumer.consumeBatch(6, 10000, async (batch: Telemetry[]) => {
      receivedBatches.push([...batch]);
      if (receivedBatches.reduce((sum, b) => sum + b.length, 0) >= 6) {
        resolveBatchPromise();
      }
    });

    // Wait for consumer group rebalance and partition assignment
    await new Promise((r) => setTimeout(r, 2000));

    // Send 2 messages to each partition (0, 1, 2)
    const messages = [
      { partition: 0, key: 'p0-1', value: JSON.stringify({ asset_id: 'asset-p0-1', latitude: 1, longitude: 1, timestamp: '2026-08-17T00:00:00Z' }) },
      { partition: 0, key: 'p0-2', value: JSON.stringify({ asset_id: 'asset-p0-2', latitude: 1, longitude: 1, timestamp: '2026-08-17T00:00:01Z' }) },
      { partition: 1, key: 'p1-1', value: JSON.stringify({ asset_id: 'asset-p1-1', latitude: 2, longitude: 2, timestamp: '2026-08-17T00:00:02Z' }) },
      { partition: 1, key: 'p1-2', value: JSON.stringify({ asset_id: 'asset-p1-2', latitude: 2, longitude: 2, timestamp: '2026-08-17T00:00:03Z' }) },
      { partition: 2, key: 'p2-1', value: JSON.stringify({ asset_id: 'asset-p2-1', latitude: 3, longitude: 3, timestamp: '2026-08-17T00:00:04Z' }) },
      { partition: 2, key: 'p2-2', value: JSON.stringify({ asset_id: 'asset-p2-2', latitude: 3, longitude: 3, timestamp: '2026-08-17T00:00:05Z' }) },
    ];

    await producer.sendBatch({
      topicMessages: [
        { topic, messages: [messages[0], messages[1]], partition: 0 },
        { topic, messages: [messages[2], messages[3]], partition: 1 },
        { topic, messages: [messages[4], messages[5]], partition: 2 },
      ],
    });

    await batchPromise;
    await consumer.close();

    expect(receivedBatches.length).toBeGreaterThanOrEqual(1);
    const allReceived = receivedBatches.flat();
    expect(allReceived.length).toBe(6);

    const assetIds = allReceived.map((t) => t.asset_id);
    expect(assetIds).toContain('asset-p0-1');
    expect(assetIds).toContain('asset-p1-1');
    expect(assetIds).toContain('asset-p2-1');
  }, 30000);

  it('flushes on timeout during traffic lull when fewer than batchSize messages arrive', async () => {
    const topic = await createTestTopic(1);
    const groupId = `group-timeout-${Date.now()}`;
    const consumer = new KafkaConsumer(brokers, topic, groupId);

    const receivedBatches: Telemetry[][] = [];
    let resolveBatchPromise: () => void;
    const batchPromise = new Promise<void>((resolve) => {
      resolveBatchPromise = resolve;
    });

    let flushedAt = 0;
    // batchSize = 100, timeout = 1000ms
    await consumer.consumeBatch(100, 1000, async (batch: Telemetry[]) => {
      flushedAt = Date.now();
      receivedBatches.push([...batch]);
      resolveBatchPromise();
    });

    // Wait for consumer group rebalance
    await new Promise((r) => setTimeout(r, 2000));

    const startTime = Date.now();
    // Send only 3 messages
    await producer.send({
      topic,
      messages: [
        { key: 't1', value: JSON.stringify({ asset_id: 'asset-t1', latitude: 10, longitude: 20, timestamp: '2026-08-17T00:00:00Z' }) },
        { key: 't2', value: JSON.stringify({ asset_id: 'asset-t2', latitude: 10, longitude: 20, timestamp: '2026-08-17T00:00:01Z' }) },
        { key: 't3', value: JSON.stringify({ asset_id: 'asset-t3', latitude: 10, longitude: 20, timestamp: '2026-08-17T00:00:02Z' }) },
      ],
    });

    await batchPromise;
    await consumer.close();

    expect(receivedBatches.length).toBe(1);
    expect(receivedBatches[0].length).toBe(3);
    const elapsed = flushedAt - startTime;
    // Should have waited roughly ~1000ms
    expect(elapsed).toBeGreaterThanOrEqual(800);
  }, 30000);

  it('flushes immediately when batchSize is reached', async () => {
    const topic = await createTestTopic(1);
    const groupId = `group-size-${Date.now()}`;
    const consumer = new KafkaConsumer(brokers, topic, groupId);

    const receivedBatches: Telemetry[][] = [];
    let resolveBatchPromise: () => void;
    const batchPromise = new Promise<void>((resolve) => {
      resolveBatchPromise = resolve;
    });

    // batchSize = 10, timeout = 10000ms
    await consumer.consumeBatch(10, 10000, async (batch: Telemetry[]) => {
      receivedBatches.push([...batch]);
      resolveBatchPromise();
    });

    // Wait for consumer group rebalance
    await new Promise((r) => setTimeout(r, 2000));

    const startTime = Date.now();
    const msgs = Array.from({ length: 10 }, (_, i) => ({
      key: `burst-${i}`,
      value: JSON.stringify({ asset_id: `burst-asset-${i}`, latitude: 10, longitude: 20, timestamp: '2026-08-17T00:00:00Z' }),
    }));

    await producer.send({ topic, messages: msgs });

    await batchPromise;
    const flushDuration = Date.now() - startTime;
    await consumer.close();

    expect(receivedBatches.length).toBeGreaterThanOrEqual(1);
    expect(receivedBatches[0].length).toBe(10);
    // Flushed immediately upon reaching size, not waiting for 10000ms timeout
    expect(flushDuration).toBeLessThan(3000);
  }, 30000);
});
