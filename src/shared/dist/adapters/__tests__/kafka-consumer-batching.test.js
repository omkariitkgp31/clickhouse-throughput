"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const consumer_js_1 = require("../kafka/consumer.js");
let mockRunHandler = null;
let mockConnect;
let mockSubscribe;
let mockRun;
let mockCommitOffsets;
let mockDisconnect;
vitest_1.vi.mock('kafkajs', () => {
    return {
        Kafka: vitest_1.vi.fn().mockImplementation(() => ({
            consumer: vitest_1.vi.fn().mockImplementation(() => ({
                connect: mockConnect,
                subscribe: mockSubscribe,
                run: mockRun,
                commitOffsets: mockCommitOffsets,
                disconnect: mockDisconnect,
            })),
        })),
    };
});
function createMessagePayload(topic, partition, offset, telemetry) {
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
        heartbeat: vitest_1.vi.fn(),
        pause: vitest_1.vi.fn(),
    };
}
(0, vitest_1.describe)('KafkaConsumer Batching Contract (Unit)', () => {
    (0, vitest_1.beforeEach)(() => {
        vitest_1.vi.useFakeTimers();
        mockRunHandler = null;
        mockConnect = vitest_1.vi.fn().mockResolvedValue(undefined);
        mockSubscribe = vitest_1.vi.fn().mockResolvedValue(undefined);
        mockCommitOffsets = vitest_1.vi.fn().mockResolvedValue(undefined);
        mockDisconnect = vitest_1.vi.fn().mockResolvedValue(undefined);
        mockRun = vitest_1.vi.fn().mockImplementation(async (options) => {
            mockRunHandler = options.eachMessage;
        });
    });
    (0, vitest_1.afterEach)(() => {
        vitest_1.vi.useRealTimers();
        vitest_1.vi.clearAllMocks();
    });
    (0, vitest_1.it)('1. configures autoCommit: false on consumer.run', async () => {
        const consumer = new consumer_js_1.KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
        const handler = vitest_1.vi.fn().mockResolvedValue(undefined);
        await consumer.consumeBatch(1000, 1000, handler);
        (0, vitest_1.expect)(mockRun).toHaveBeenCalledWith(vitest_1.expect.objectContaining({
            autoCommit: false,
            eachMessage: vitest_1.expect.any(Function),
        }));
        await consumer.close();
    });
    (0, vitest_1.it)('2. accumulates messages across multiple partitions into a single batch', async () => {
        const consumer = new consumer_js_1.KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
        const receivedBatches = [];
        const handler = vitest_1.vi.fn().mockImplementation(async (batch) => {
            receivedBatches.push([...batch]);
        });
        await consumer.consumeBatch(5, 5000, handler);
        (0, vitest_1.expect)(mockRunHandler).not.toBeNull();
        // Send 5 messages across 3 partitions (0, 1, 2)
        const messages = [
            { partition: 0, offset: '10', asset_id: 'asset-1' },
            { partition: 1, offset: '20', asset_id: 'asset-2' },
            { partition: 2, offset: '30', asset_id: 'asset-3' },
            { partition: 0, offset: '11', asset_id: 'asset-4' },
            { partition: 1, offset: '21', asset_id: 'asset-5' },
        ];
        for (const msg of messages) {
            await mockRunHandler(createMessagePayload('telemetry', msg.partition, msg.offset, {
                asset_id: msg.asset_id,
                latitude: 10,
                longitude: 20,
                timestamp: '2026-08-17T00:00:00Z',
            }));
        }
        // Size limit of 5 is hit -> flushes immediately
        (0, vitest_1.expect)(handler).toHaveBeenCalledTimes(1);
        (0, vitest_1.expect)(receivedBatches[0]).toHaveLength(5);
        (0, vitest_1.expect)(receivedBatches[0].map((t) => t.asset_id)).toEqual([
            'asset-1',
            'asset-2',
            'asset-3',
            'asset-4',
            'asset-5',
        ]);
        // Check offsets committed for each partition: partition 0 -> 12, partition 1 -> 22, partition 2 -> 31
        (0, vitest_1.expect)(mockCommitOffsets).toHaveBeenCalledWith([
            { topic: 'telemetry', partition: 0, offset: '12' },
            { topic: 'telemetry', partition: 1, offset: '22' },
            { topic: 'telemetry', partition: 2, offset: '31' },
        ]);
        await consumer.close();
    });
    (0, vitest_1.it)('3. flushes on timeout when fewer than batchSize messages arrive (traffic lull)', async () => {
        const consumer = new consumer_js_1.KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
        const handler = vitest_1.vi.fn().mockResolvedValue(undefined);
        await consumer.consumeBatch(100, 1000, handler);
        // Send only 3 messages
        for (let i = 1; i <= 3; i++) {
            await mockRunHandler(createMessagePayload('telemetry', 0, `${i}`, {
                asset_id: `asset-${i}`,
                latitude: 10,
                longitude: 20,
                timestamp: '2026-08-17T00:00:00Z',
            }));
        }
        // Before timeout: no flush yet
        (0, vitest_1.expect)(handler).not.toHaveBeenCalled();
        // Advance time past 1000ms timeout
        await vitest_1.vi.advanceTimersByTimeAsync(1100);
        // After timeout: flush triggered
        (0, vitest_1.expect)(handler).toHaveBeenCalledTimes(1);
        (0, vitest_1.expect)(handler).toHaveBeenCalledWith(vitest_1.expect.arrayContaining([
            vitest_1.expect.objectContaining({ asset_id: 'asset-1' }),
            vitest_1.expect.objectContaining({ asset_id: 'asset-2' }),
            vitest_1.expect.objectContaining({ asset_id: 'asset-3' }),
        ]));
        (0, vitest_1.expect)(mockCommitOffsets).toHaveBeenCalledWith([{ topic: 'telemetry', partition: 0, offset: '4' }]);
        await consumer.close();
    });
    (0, vitest_1.it)('4. flushes immediately when batchSize is reached without waiting for timeout', async () => {
        const consumer = new consumer_js_1.KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
        const handler = vitest_1.vi.fn().mockResolvedValue(undefined);
        await consumer.consumeBatch(10, 5000, handler);
        // Send 10 messages
        for (let i = 1; i <= 10; i++) {
            await mockRunHandler(createMessagePayload('telemetry', i % 2, `${i}`, {
                asset_id: `asset-${i}`,
                latitude: 10,
                longitude: 20,
                timestamp: '2026-08-17T00:00:00Z',
            }));
        }
        // Immediate flush triggered at size 10 (0ms elapsed)
        (0, vitest_1.expect)(handler).toHaveBeenCalledTimes(1);
        (0, vitest_1.expect)(mockCommitOffsets).toHaveBeenCalledTimes(1);
        await consumer.close();
    });
    (0, vitest_1.it)('5. does NOT commit offsets if handler throws an error', async () => {
        const consumer = new consumer_js_1.KafkaConsumer(['localhost:9092'], 'telemetry', 'test-group');
        const handler = vitest_1.vi.fn().mockRejectedValue(new Error('ClickHouse write failed'));
        await consumer.consumeBatch(2, 5000, handler);
        for (let i = 1; i <= 2; i++) {
            await mockRunHandler(createMessagePayload('telemetry', 0, `${i}`, {
                asset_id: `asset-${i}`,
                latitude: 10,
                longitude: 20,
                timestamp: '2026-08-17T00:00:00Z',
            }));
        }
        (0, vitest_1.expect)(handler).toHaveBeenCalledTimes(1);
        // Offset commit must NOT happen on error
        (0, vitest_1.expect)(mockCommitOffsets).not.toHaveBeenCalled();
        await consumer.close();
    });
});
//# sourceMappingURL=kafka-consumer-batching.test.js.map