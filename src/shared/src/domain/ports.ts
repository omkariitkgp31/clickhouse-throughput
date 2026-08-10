import { Telemetry } from './models.js';

export interface TelemetryProducer {
  produce(telemetry: Telemetry): Promise<void>;
  close(): Promise<void>;
}

export interface TelemetryCache {
  setLatestLocation(telemetry: Telemetry): Promise<void>;
  getLatestLocation(assetId: string): Promise<Telemetry | null>;
  publishLocation(telemetry: Telemetry): Promise<void>;
  getCacheMetrics(): Promise<Record<string, any>>;
  close(): Promise<void>;
}

export interface TelemetryStorage {
  insertBatch(telemetries: Telemetry[]): Promise<void>;
  getRouteHistory(assetId: string, start: string, end: string): Promise<Telemetry[]>;
  getDBMetrics(): Promise<Record<string, any>>;
  close(): Promise<void>;
}
