"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaProducer = void 0;
const kafkajs_1 = require("kafkajs");
class KafkaProducer {
    kafka;
    producer;
    topic;
    isConnected = false;
    constructor(brokers, topic) {
        this.kafka = new kafkajs_1.Kafka({
            clientId: 'fleet-ingestion-producer',
            brokers,
        });
        this.producer = this.kafka.producer();
        this.topic = topic;
    }
    async connect() {
        if (!this.isConnected) {
            await this.producer.connect();
            this.isConnected = true;
        }
    }
    async produce(telemetry) {
        if (!this.isConnected) {
            await this.connect();
        }
        await this.producer.send({
            topic: this.topic,
            messages: [
                {
                    key: telemetry.asset_id,
                    value: JSON.stringify(telemetry),
                },
            ],
        });
    }
    async close() {
        if (this.isConnected) {
            await this.producer.disconnect();
            this.isConnected = false;
        }
    }
}
exports.KafkaProducer = KafkaProducer;
//# sourceMappingURL=producer.js.map