"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ClickHouseStorage = void 0;
const client_1 = require("@clickhouse/client");
class ClickHouseStorage {
    client;
    constructor(url, database = 'fleet', username = 'default', password = '') {
        this.client = (0, client_1.createClient)({
            url,
            database,
            username,
            password,
        });
    }
    async insertBatch(telemetries) {
        if (telemetries.length === 0)
            return;
        const rows = telemetries.map((t) => ({
            asset_id: t.asset_id,
            time: t.timestamp,
            latitude: t.latitude,
            longitude: t.longitude,
        }));
        await this.client.insert({
            table: 'location_history',
            values: rows,
            format: 'JSONEachRow',
        });
    }
    async getRouteHistory(assetId, start, end) {
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
        const rows = await resultSet.json();
        return rows.map((r) => ({
            asset_id: r.asset_id,
            latitude: Number(r.latitude),
            longitude: Number(r.longitude),
            timestamp: r.time,
        }));
    }
    async getDBMetrics() {
        const metrics = {};
        try {
            // Total telemetry rows
            const rowCountRes = await this.client.query({
                query: 'SELECT count() AS count FROM location_history',
                format: 'JSONEachRow',
            });
            const rowCountJson = await rowCountRes.json();
            metrics.total_telemetry_rows = Number(rowCountJson[0]?.count || 0);
            // Registered assets
            const assetCountRes = await this.client.query({
                query: 'SELECT count() AS count FROM assets',
                format: 'JSONEachRow',
            });
            const assetCountJson = await assetCountRes.json();
            metrics.registered_assets = Number(assetCountJson[0]?.count || 0);
            // Active DB queries / connections
            const activeConnsRes = await this.client.query({
                query: 'SELECT count() AS count FROM system.processes',
                format: 'JSONEachRow',
            });
            const activeConnsJson = await activeConnsRes.json();
            metrics.active_db_connections = Number(activeConnsJson[0]?.count || 0);
            // Database size
            const dbSizeRes = await this.client.query({
                query: "SELECT formatReadableSize(sum(bytes_on_disk)) AS size FROM system.parts WHERE database = 'fleet'",
                format: 'JSONEachRow',
            });
            const dbSizeJson = await dbSizeRes.json();
            metrics.database_size = dbSizeJson[0]?.size || '0 B';
            // Compression ratio multiplier
            const compStatsRes = await this.client.query({
                query: "SELECT sum(data_uncompressed_bytes) AS uncompressed, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database = 'fleet'",
                format: 'JSONEachRow',
            });
            const compStatsJson = await compStatsRes.json();
            const uncompressed = Number(compStatsJson[0]?.uncompressed || 0);
            const compressed = Number(compStatsJson[0]?.compressed || 0);
            if (uncompressed > 0 && compressed > 0) {
                metrics.compression_ratio_multiplier = `${(uncompressed / compressed).toFixed(2)}x`;
            }
            else {
                metrics.compression_ratio_multiplier = '1.00x (No compressed parts yet)';
            }
        }
        catch (err) {
            metrics.error = err.message || String(err);
        }
        return metrics;
    }
    async close() {
        await this.client.close();
    }
}
exports.ClickHouseStorage = ClickHouseStorage;
//# sourceMappingURL=storage.js.map