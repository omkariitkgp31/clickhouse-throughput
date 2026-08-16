import { Telemetry } from '../../domain/models.js';
export declare class KafkaConsumer {
    private kafka;
    private consumer;
    private consumerTopic?;
    private buffer;
    private firstBufferedAt;
    private isFlushing;
    private flushTimer?;
    private batchSize;
    private timeoutMs;
    private handler?;
    private isRunning;
    constructor(brokers: string[], topic: string, groupId: string);
    private flush;
    consumeBatch(batchSize: number | undefined, timeoutMs: number | undefined, handler: (batch: Telemetry[]) => Promise<void>): Promise<void>;
    start(topic: string, handler: (batch: Telemetry[]) => Promise<void>): Promise<void>;
    close(): Promise<void>;
}
