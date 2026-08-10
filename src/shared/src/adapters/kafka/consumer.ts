import { Kafka, Consumer as KafkaJSConsumer, EachBatchPayload } from 'kafkajs';
import { Telemetry } from '../../domain/models.js';

export class KafkaConsumer {
  private kafka: Kafka;
  private consumer: KafkaJSConsumer;

  constructor(brokers: string[], topic: string, groupId: string) {
    this.kafka = new Kafka({
      clientId: 'fleet-stream-processor',
      brokers,
    });
    this.consumer = this.kafka.consumer({
      groupId,
      maxBytesPerPartition: 10 * 1024 * 1024, // 10MB
    });
  }

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

  private consumerTopic?: string;

  async start(topic: string, handler: (batch: Telemetry[]) => Promise<void>): Promise<void> {
    this.consumerTopic = topic;
    await this.consumeBatch(1000, 1000, handler);
  }

  async close(): Promise<void> {
    await this.consumer.disconnect();
  }
}
