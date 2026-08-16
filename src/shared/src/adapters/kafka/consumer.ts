import { Kafka, Consumer as KafkaJSConsumer, EachMessagePayload } from 'kafkajs';
import { Telemetry } from '../../domain/models.js';

interface BufferedMessage {
  telemetry: Telemetry;
  topic: string;
  partition: number;
  offset: string;
}

export class KafkaConsumer {
  private kafka: Kafka;
  private consumer: KafkaJSConsumer;
  private consumerTopic?: string;
  private buffer: BufferedMessage[] = [];
  private firstBufferedAt: number | null = null;
  private isFlushing = false;
  private flushTimer?: NodeJS.Timeout;
  private batchSize = 1000;
  private timeoutMs = 1000;
  private handler?: (batch: Telemetry[]) => Promise<void>;
  private isRunning = false;

  constructor(brokers: string[], topic: string, groupId: string) {
    this.consumerTopic = topic;
    this.kafka = new Kafka({
      clientId: 'fleet-stream-processor',
      brokers,
    });
    this.consumer = this.kafka.consumer({
      groupId,
      maxBytesPerPartition: 10 * 1024 * 1024, // 10MB
    });
  }

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

      // Compute highest offset + 1 per partition for committed offsets
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
      // Do not commit offsets on failure
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

  async start(topic: string, handler: (batch: Telemetry[]) => Promise<void>): Promise<void> {
    this.consumerTopic = topic;
    await this.consumeBatch(1000, 1000, handler);
  }

  async close(): Promise<void> {
    this.isRunning = false;
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = undefined;
    }
    if (this.buffer.length > 0 && this.handler) {
      try {
        await this.flush();
      } catch (err) {
        console.error('Error flushing remaining items during close:', err);
      }
    }
    await this.consumer.disconnect();
  }
}
