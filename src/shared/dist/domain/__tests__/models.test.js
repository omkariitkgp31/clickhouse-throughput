"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
(0, vitest_1.describe)('Domain Models', () => {
    (0, vitest_1.it)('should construct a valid Telemetry object', () => {
        const telemetry = {
            asset_id: 'asset-1',
            latitude: 40.7128,
            longitude: -74.006,
            timestamp: new Date().toISOString(),
        };
        (0, vitest_1.expect)(telemetry.asset_id).toBe('asset-1');
        (0, vitest_1.expect)(telemetry.latitude).toBe(40.7128);
        (0, vitest_1.expect)(telemetry.longitude).toBe(-74.006);
        (0, vitest_1.expect)(telemetry.timestamp).toBeDefined();
    });
    (0, vitest_1.it)('should construct a valid Asset object', () => {
        const asset = {
            id: 'asset-1',
            driver_name: 'Alice',
            created_at: new Date().toISOString(),
        };
        (0, vitest_1.expect)(asset.id).toBe('asset-1');
        (0, vitest_1.expect)(asset.driver_name).toBe('Alice');
        (0, vitest_1.expect)(asset.created_at).toBeDefined();
    });
});
//# sourceMappingURL=models.test.js.map