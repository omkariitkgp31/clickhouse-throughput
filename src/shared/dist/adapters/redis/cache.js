"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisCache = void 0;
const ioredis_1 = __importDefault(require("ioredis"));
class RedisCache {
    client;
    constructor(addr) {
        if (addr.includes('://')) {
            this.client = new ioredis_1.default(addr);
        }
        else {
            const [host, portStr] = addr.split(':');
            const port = portStr ? parseInt(portStr, 10) : 6379;
            this.client = new ioredis_1.default({ host: host || 'localhost', port });
        }
    }
    async setLatestLocation(telemetry) {
        const key = `asset:${telemetry.asset_id}:latest`;
        const payload = JSON.stringify(telemetry);
        await this.client.set(key, payload, 'EX', 86400); // 24 hours TTL
    }
    async getLatestLocation(assetId) {
        const key = `asset:${assetId}:latest`;
        const val = await this.client.get(key);
        if (!val)
            return null;
        return JSON.parse(val);
    }
    async publishLocation(telemetry) {
        const channel = 'telemetry_updates';
        const payload = JSON.stringify(telemetry);
        await this.client.publish(channel, payload);
    }
    async getCacheMetrics() {
        const metrics = {};
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
        }
        catch (err) {
            metrics.error = err.message || String(err);
        }
        return metrics;
    }
    async close() {
        await this.client.quit();
    }
}
exports.RedisCache = RedisCache;
//# sourceMappingURL=cache.js.map