import { Telemetry } from '../../domain/models.js';
import { TelemetryStorage } from '../../domain/ports.js';
export declare class ClickHouseStorage implements TelemetryStorage {
    private client;
    private database;
    constructor(url: string, database?: string, username?: string, password?: string);
    insertBatch(telemetries: Telemetry[]): Promise<void>;
    getRouteHistory(assetId: string, start: string, end: string): Promise<Telemetry[]>;
    getDBMetrics(): Promise<Record<string, any>>;
    close(): Promise<void>;
}
