"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
(0, vitest_1.describe)('Adapter Interfaces & Payloads', () => {
    (0, vitest_1.it)('should format Redis cache key correctly', () => {
        const telemetry = {
            asset_id: 'asset-42',
            latitude: 37.7749,
            longitude: -122.4194,
            timestamp: '2026-08-10T12:00:00.000Z',
        };
        const expectedKey = `asset:${telemetry.asset_id}:latest`;
        const serializedPayload = JSON.stringify(telemetry);
        (0, vitest_1.expect)(expectedKey).toBe('asset:asset-42:latest');
        (0, vitest_1.expect)(JSON.parse(serializedPayload)).toEqual(telemetry);
    });
    (0, vitest_1.it)('should format ClickHouse location history row values correctly', () => {
        const telemetries = [
            {
                asset_id: 'asset-100',
                latitude: 51.5074,
                longitude: -0.1278,
                timestamp: '2026-08-10T12:00:00.000Z',
            },
        ];
        const rows = telemetries.map((t) => ({
            asset_id: t.asset_id,
            time: t.timestamp,
            latitude: t.latitude,
            longitude: t.longitude,
        }));
        (0, vitest_1.expect)(rows[0]).toEqual({
            asset_id: 'asset-100',
            time: '2026-08-10T12:00:00.000Z',
            latitude: 51.5074,
            longitude: -0.1278,
        });
    });
});
//# sourceMappingURL=adapters.test.js.map