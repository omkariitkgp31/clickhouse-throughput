import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { KafkaConsumer } from '../kafka/consumer.js';
import { Telemetry } from '../../domain/models.js';
import { Kafka, EachMessagePayload } from 'kafkajs';

let mockRunHandler: ((payload: EachMessagePayload) => Promise<void>) | null = null;
let mockConnect: ReturnType<typeof vi.fn>;
let mockSubscribe: ReturnType<typeof vi.fn>;
let mockRun: ReturnType<typeof vi.fn>;
let mockCommitOffsets: ReturnType<typeof vi.fn>;
let mockDisconnect: ReturnType<typeof vi.fn>;

vi.mock('kafkajs', () => {
  return {
    Kafka: vi.fn().mockImplementation(() => ({
      consumer: vi.fn().mockImplementation(() => ({
        connect: mockConnect,
        subscribe: mockSubscribe,
        run: mockRun,
        commitOffsets: mockCommitOffsets,
        disconnect: mockDisconnect,
      })),
    })),
  };
});

function createMessagePayload(
  topic: string,
  partition: number,
  offset: string,
  telemetry: Telemetry
): EachMessagePayload {
  return {
    topic,
    partition,
    message: {
      key: Buffer.from(telemetry.asset_id),
      value: Buffer.from(JSON.stringify(telemetry)),
      timestamp: '0',
      attributes: 0,
      offset,
      headers: {},
    },
    heartbeat: vi.fn(),
    pause: vi.fn(),
  };
}

describe('KafkaConsumer Batching Contract (Unit)', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mockRunHandler = null;
    mockConnect = vi.fn().mockResolvedValue(undefined);
    mockSubscribe = vi.fn().mockResolvedValue(undefined);
    mockCommitOffsets = vi.fn().mockResolvedValue(undefined);
    mockDisconnect = vi.fn().mockResolvedValue(undefined);
    mockRun = vi.fn().mockImplementation(async (options: any) => {
      mockRunHandler = options.eachMessage;
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('1. configures autoCommit: false on consumer.run', async () => {
    const consumer = new KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
    const handler = vi.fn().mockResolvedValue(undefined);

    await consumer.consumeBatch(1000, 1000, handler);

    expect(mockRun).toHaveBeenCalledWith(
      expect.objectContaining({
        autoCommit: false,
        eachMessage: expect.any(Function),
      })
    );
    await consumer.close();
  });

  it('2. accumulates messages across multiple partitions into a single batch', async () => {
    const consumer = new KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
    const receivedBatches: Telemetry[][] = [];
    const handler = vi.fn().mockImplementation(async (batch: Telemetry[]) => {
      receivedBatches.push([...batch]);
    });

    await consumer.consumeBatch(5, 5000, handler);
    expect(mockRunHandler).not.toBeNull();

    // Send 5 messages across 3 partitions (0, 1, 2)
    const messages = [
      { partition: 0, offset: '10', asset_id: 'asset-1' },
      { partition: 1, offset: '20', asset_id: 'asset-2' },
      { partition: 2, offset: '30', asset_id: 'asset-3' },
      { partition: 0, offset: '11', asset_id: 'asset-4' },
      { partition: 1, offset: '21', asset_id: 'asset-5' },
    ];

    for (const msg of messages) {
      await mockRunHandler!(
        createMessagePayload('telemetry', msg.partition, msg.offset, {
          asset_id: msg.asset_id,
          latitude: 10,
          longitude: 20,
          timestamp: '2026-08-17T00:00:00Z',
        })
      );
    }

    // Size limit of 5 is hit -> flushes immediately
    expect(handler).toHaveBeenCalledTimes(1);
    expect(receivedBatches[0]).toHaveLength(5);
    expect(receivedBatches[0].map((t) => t.asset_id)).toEqual([
      'asset-1',
      'asset-2',
      'asset-3',
      'asset-4',
      'asset-5',
    ]);

    // Check offsets committed for each partition: partition 0 -> 12, partition 1 -> 22, partition 2 -> 31
    expect(mockCommitOffsets).toHaveBeenCalledWith([
      { topic: 'telemetry', partition: 0, offset: '12' },
      { topic: 'telemetry', partition: 1, offset: '22' },
      { topic: 'telemetry', partition: 2, offset: '31' },
    ]);

    await consumer.close();
  });

  it('3. flushes on timeout when fewer than batchSize messages arrive (traffic lull)', async () => {
    const consumer = new KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
    const handler = vi.fn().mockResolvedValue(undefined);

    await consumer.consumeBatch(100, 1000, handler);

    // Send only 3 messages
    for (let i = 1; i <= 3; i++) {
      await mockRunHandler!(
        createMessagePayload('telemetry', 0, `${i}`, {
          asset_id: `asset-${i}`,
          latitude: 10,
          longitude: 20,
          timestamp: '2026-08-17T00:00:00Z',
        })
      );
    }

    // Before timeout: no flush yet
    expect(handler).not.toHaveBeenCalled();

    // Advance time past 1000ms timeout
    await vi.advanceTimersByTimeAsync(1100);

    // After timeout: flush triggered
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({ asset_id: 'asset-1' }),
        expect.objectContaining({ asset_id: 'asset-2' }),
        expect.objectContaining({ asset_id: 'asset-3' }),
      ])
    );
    expect(mockCommitOffsets).toHaveBeenCalledWith([{ topic: 'telemetry', partition: 0, offset: '4' }]);

    await consumer.close();
  });

  it('4. flushes immediately when batchSize is reached without waiting for timeout', async () => {
    const consumer = new KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
    const handler = vi.fn().mockResolvedValue(undefined);

    await consumer.consumeBatch(10, 5000, handler);

    // Send 10 messages
    for (let i = 1; i <= 10; i++) {
      await mockRunHandler!(
        createMessagePayload('telemetry', i % 2, `${i}`, {
          asset_id: `asset-${i}`,
          latitude: 10,
          longitude: 20,
          timestamp: '2026-08-17T00:00:00Z',
        })
      );
    }

    // Immediate flush triggered at size 10 (0ms elapsed)
    expect(handler).toHaveBeenCalledTimes(1);
    expect(mockCommitOffsets).toHaveBeenCalledTimes(1);

    await consumer.close();
  });

  it('5. does NOT commit offsets if handler throws an error', async () => {
    const consumer = new KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
    const handler = vi.fn().mockRejectedValue(new Error('ClickHouse write failed'));

    await consumer.consumeBatch(2, 5000, handler);

    for (let i = 1; i <= 2; i++) {
      await mockRunHandler!(
        createMessagePayload('telemetry', 0, `${i}`, {
          asset_id: `asset-${i}`,
          latitude: 10,
          longitude: 20,
          timestamp: '2026-08-17T00:00:00Z',
        })
      );
    }

    expect(handler).toHaveBeenCalledTimes(1);
    // Offset commit must NOT happen on error
    expect(mockCommitOffsets).not.toHaveBeenCalled();

    await consumer.close();
  });
});
