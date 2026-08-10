import Redis from 'ioredis';
import { Telemetry } from '../../domain/models.js';
import { TelemetryCache } from '../../domain/ports.js';

export class RedisCache implements TelemetryCache {
  private client: Redis;

  constructor(addr: string) {
    if (addr.includes('://')) {
      this.client = new Redis(addr);
    } else {
      const [host, portStr] = addr.split(':');
      const port = portStr ? parseInt(portStr, 10) : 6379;
      this.client = new Redis({ host: host || 'localhost', port });
    }
  }

  async setLatestLocation(telemetry: Telemetry): Promise<void> {
    const key = `asset:${telemetry.asset_id}:latest`;
    const payload = JSON.stringify(telemetry);
    await this.client.set(key, payload, 'EX', 86400); // 24 hours TTL
  }

  async getLatestLocation(assetId: string): Promise<Telemetry | null> {
    const key = `asset:${assetId}:latest`;
    const val = await this.client.get(key);
    if (!val) return null;
    return JSON.parse(val) as Telemetry;
  }

  async publishLocation(telemetry: Telemetry): Promise<void> {
    const channel = 'telemetry_updates';
    const payload = JSON.stringify(telemetry);
    await this.client.publish(channel, payload);
  }

  async getCacheMetrics(): Promise<Record<string, any>> {
    const metrics: Record<string, any> = {};

    try {
      const info = await this.client.info('memory', 'clients', 'stats');
      const lines = info.split('\r\n');

      for (const line of lines) {
        if (line.startsWith('used_memory_human:')) {
          metrics.memory_used = line.slice('used_memory_human:'.length);
        }
        if (line.startsWith('connected_clients:')) {
          metrics.connected_clients = line.slice('connected_clients:'.length);
        }
        if (line.startsWith('instantaneous_ops_per_sec:')) {
          metrics.ops_per_second = line.slice('instantaneous_ops_per_sec:'.length);
        }
        if (line.startsWith('total_connections_received:')) {
          metrics.lifetime_connections = line.slice('total_connections_received:'.length);
        }
      }

      const dbSize = await this.client.dbsize();
      metrics.total_active_assets = dbSize;
    } catch (err: any) {
      metrics.error = err.message || String(err);
    }

    return metrics;
  }

  async close(): Promise<void> {
    await this.client.quit();
  }
}
