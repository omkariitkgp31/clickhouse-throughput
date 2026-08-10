import { Telemetry } from '../../domain/models.js';
import { TelemetryCache } from '../../domain/ports.js';
export declare class RedisCache implements TelemetryCache {
    private client;
    constructor(addr: string);
    setLatestLocation(telemetry: Telemetry): Promise<void>;
    getLatestLocation(assetId: string): Promise<Telemetry | null>;
    publishLocation(telemetry: Telemetry): Promise<void>;
    getCacheMetrics(): Promise<Record<string, any>>;
    close(): Promise<void>;
}
