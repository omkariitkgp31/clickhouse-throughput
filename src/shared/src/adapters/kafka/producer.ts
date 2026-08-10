import { Kafka, Producer as KafkaJSProducer } from 'kafkajs';
import { Telemetry } from '../../domain/models.js';
import { TelemetryProducer } from '../../domain/ports.js';

export class KafkaProducer implements TelemetryProducer {
  private kafka: Kafka;
  private producer: KafkaJSProducer;
  private topic: string;
  private isConnected = false;

  constructor(brokers: string[], topic: string) {
    this.kafka = new Kafka({
      clientId: 'fleet-ingestion-producer',
      brokers,
    });
    this.producer = this.kafka.producer();
    this.topic = topic;
  }

  async connect(): Promise<void> {
    if (!this.isConnected) {
      await this.producer.connect();
      this.isConnected = true;
    }
  }

  async produce(telemetry: Telemetry): Promise<void> {
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

  async close(): Promise<void> {
    if (this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
    }
  }
}
