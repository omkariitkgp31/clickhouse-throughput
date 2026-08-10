import { Telemetry } from '../../domain/models.js';
import { TelemetryProducer } from '../../domain/ports.js';
export declare class KafkaProducer implements TelemetryProducer {
    private kafka;
    private producer;
    private topic;
    private isConnected;
    constructor(brokers: string[], topic: string);
    connect(): Promise<void>;
    produce(telemetry: Telemetry): Promise<void>;
    close(): Promise<void>;
}
