import { Telemetry } from '../../domain/models.js';
export declare class KafkaConsumer {
    private kafka;
    private consumer;
    constructor(brokers: string[], topic: string, groupId: string);
    consumeBatch(batchSize: number, timeoutMs: number, handler: (batch: Telemetry[]) => Promise<void>): Promise<void>;
    private consumerTopic?;
    start(topic: string, handler: (batch: Telemetry[]) => Promise<void>): Promise<void>;
    close(): Promise<void>;
}
