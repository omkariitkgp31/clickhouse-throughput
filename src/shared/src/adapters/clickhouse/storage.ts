import { createClient, ClickHouseClient } from '@clickhouse/client';
import { Telemetry } from '../../domain/models.js';
import { TelemetryStorage } from '../../domain/ports.js';

export class ClickHouseStorage implements TelemetryStorage {
  private client: ClickHouseClient;
  private database: string;

  constructor(url: string, database = 'fleet', username = 'default', password = '') {
    this.database = database;
    this.client = createClient({
      url,
      database,
      username,
      password,
    });
  }

  async insertBatch(telemetries: Telemetry[]): Promise<void> {
    if (telemetries.length === 0) return;

    const rows = telemetries.map((t) => ({
      asset_id: t.asset_id,
      time: t.timestamp.replace('T', ' ').replace('Z', ''),
      latitude: t.latitude,
      longitude: t.longitude,
    }));

    await this.client.insert({
      table: 'location_history',
      values: rows,
      format: 'JSONEachRow',
    });
  }

  async getRouteHistory(assetId: string, start: string, end: string): Promise<Telemetry[]> {
    const query = `
      SELECT time, asset_id, latitude, longitude 
      FROM location_history 
      WHERE asset_id = {assetId: String} AND time >= {start: String} AND time <= {end: String} 
      ORDER BY time ASC
    `;

    const resultSet = await this.client.query({
      query,
      query_params: {
        assetId,
        start,
        end,
      },
      format: 'JSONEachRow',
    });

    const rows: Array<{ time: string; asset_id: string; latitude: number; longitude: number }> =
      await resultSet.json();

    return rows.map((r) => ({
      asset_id: r.asset_id,
      latitude: Number(r.latitude),
      longitude: Number(r.longitude),
      timestamp: r.time,
    }));
  }

  async getDBMetrics(): Promise<Record<string, any>> {
    const metrics: Record<string, any> = {};

    try {
      // Total telemetry rows
      const rowCountRes = await this.client.query({
        query: 'SELECT count() AS count FROM location_history',
        format: 'JSONEachRow',
      });
      const rowCountJson: Array<{ count: string }> = await rowCountRes.json();
      metrics.total_telemetry_rows = Number(rowCountJson[0]?.count || 0);

      // Registered assets
      const assetCountRes = await this.client.query({
        query: 'SELECT count() AS count FROM assets',
        format: 'JSONEachRow',
      });
      const assetCountJson: Array<{ count: string }> = await assetCountRes.json();
      metrics.registered_assets = Number(assetCountJson[0]?.count || 0);

      // Active DB queries / connections
      const activeConnsRes = await this.client.query({
        query: 'SELECT count() AS count FROM system.processes',
        format: 'JSONEachRow',
      });
      const activeConnsJson: Array<{ count: string }> = await activeConnsRes.json();
      metrics.active_db_connections = Number(activeConnsJson[0]?.count || 0);

      // Database size
      const dbSizeRes = await this.client.query({
        query: `SELECT formatReadableSize(sum(bytes_on_disk)) AS size FROM system.parts WHERE database = {db: String}`,
        query_params: { db: this.database },
        format: 'JSONEachRow',
      });
      const dbSizeJson: Array<{ size: string }> = await dbSizeRes.json();
      metrics.database_size = dbSizeJson[0]?.size || '0 B';

      // Compression ratio multiplier
      const compStatsRes = await this.client.query({
        query: `SELECT sum(data_uncompressed_bytes) AS uncompressed, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database = {db: String}`,
        query_params: { db: this.database },
        format: 'JSONEachRow',
      });
      const compStatsJson: Array<{ uncompressed: string; compressed: string }> = await compStatsRes.json();
      const uncompressed = Number(compStatsJson[0]?.uncompressed || 0);
      const compressed = Number(compStatsJson[0]?.compressed || 0);

      if (uncompressed > 0 && compressed > 0) {
        metrics.compression_ratio_multiplier = `${(uncompressed / compressed).toFixed(2)}x`;
      } else {
        metrics.compression_ratio_multiplier = '1.00x (No compressed parts yet)';
      }
    } catch (err: any) {
      metrics.error = err.message || String(err);
    }

    return metrics;
  }

  async close(): Promise<void> {
    await this.client.close();
  }
}
