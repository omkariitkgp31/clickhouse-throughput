"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaConsumer = void 0;
const kafkajs_1 = require("kafkajs");
class KafkaConsumer {
    kafka;
    consumer;
    constructor(brokers, topic, groupId) {
        this.kafka = new kafkajs_1.Kafka({
            clientId: 'fleet-stream-processor',
            brokers,
        });
        this.consumer = this.kafka.consumer({
            groupId,
            maxBytesPerPartition: 10 * 1024 * 1024, // 10MB
        });
    }
    async consumeBatch(batchSize, timeoutMs, handler) {
        await this.consumer.connect();
        await this.consumer.subscribe({ fromBeginning: false, topics: [this.consumerTopic || 'telemetry'] });
        await this.consumer.run({
            eachBatchAutoResolve: false,
            eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
                if (!isRunning() || isStale())
                    return;
                const telemetries = [];
                for (const message of batch.messages) {
                    if (!message.value)
                        continue;
                    try {
                        const t = JSON.parse(message.value.toString('utf-8'));
                        telemetries.push(t);
                    }
                    catch (err) {
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
                    }
                    catch (err) {
                        console.error('Handler failed to process batch:', err);
                        throw err;
                    }
                }
            },
        });
    }
    consumerTopic;
    async start(topic, handler) {
        this.consumerTopic = topic;
        await this.consumeBatch(1000, 1000, handler);
    }
    async close() {
        await this.consumer.disconnect();
    }
}
exports.KafkaConsumer = KafkaConsumer;
//# sourceMappingURL=consumer.js.map