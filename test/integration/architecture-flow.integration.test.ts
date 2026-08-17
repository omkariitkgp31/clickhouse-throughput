import { describe, it, expect } from 'vitest';

describe('Architecture Flow End-to-End Verification', () => {
  const nginxHost = 'http://localhost';
  const assetId = `flow-asset-${Date.now()}`;
  const now = new Date();
  const startTime = new Date(now.getTime() - 60000).toISOString().replace('T', ' ').replace('Z', '');
  const endTime = new Date(now.getTime() + 60000).toISOString().replace('T', ' ').replace('Z', '');

  it('verifies complete telemetry ingestion, cache, storage, and metrics topology', async () => {
    // 1. Ingest telemetry point via Nginx
    const telemetryPayload = {
      asset_id: assetId,
      latitude: 37.7749,
      longitude: -122.4194,
    };

    const postRes = await fetch(`${nginxHost}/api/v1/telemetry`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(telemetryPayload),
    });

    expect(postRes.status).toBe(202);
    const postBody = await postRes.json();
    expect(postBody).toEqual({ status: 'accepted' });

    // 2. Poll Redis cache via GET /api/v1/assets/:id/location
    let redisSuccess = false;
    for (let attempt = 0; attempt < 30; attempt++) {
      await new Promise((res) => setTimeout(res, 500));
      const locRes = await fetch(`${nginxHost}/api/v1/assets/${assetId}/location`);
      if (locRes.status === 200) {
        const locData = await locRes.json();
        if (locData.asset_id === assetId && locData.latitude === 37.7749 && locData.longitude === -122.4194) {
          redisSuccess = true;
          break;
        }
      }
    }
    expect(redisSuccess).toBe(true);

    // 3. Poll ClickHouse storage via GET /api/v1/assets/:id/route
    let chSuccess = false;
    for (let attempt = 0; attempt < 30; attempt++) {
      await new Promise((res) => setTimeout(res, 500));
      const routeRes = await fetch(
        `${nginxHost}/api/v1/assets/${assetId}/route?start=${encodeURIComponent(startTime)}&end=${encodeURIComponent(endTime)}`
      );
      if (routeRes.status === 200) {
        const routeData = await routeRes.json();
        if (Array.isArray(routeData) && routeData.some((p: any) => p.asset_id === assetId)) {
          chSuccess = true;
          break;
        }
      }
    }
    expect(chSuccess).toBe(true);

    // 4. Validate system metrics endpoint
    const metricsRes = await fetch(`${nginxHost}/api/v1/system/metrics`);
    expect(metricsRes.status).toBe(200);
    const metricsData = await metricsRes.json();

    expect(metricsData.status).toBe('healthy');
    expect(metricsData.database_timescaledb).toBeDefined();
    expect(typeof metricsData.database_timescaledb.total_telemetry_rows).toBe('number');
    expect(typeof metricsData.database_timescaledb.database_size).toBe('string');
    expect(typeof metricsData.database_timescaledb.compression_ratio_multiplier).toBe('string');

    expect(metricsData.cache_redis).toBeDefined();
    expect(metricsData.cache_redis.connected_clients).toBeDefined();

    expect(metricsData.queue_kafka).toBeDefined();
    expect(metricsData.queue_kafka.status).toBe('online');
    expect(metricsData.queue_kafka.total_partitions).toBe(10);

    expect(metricsData.gateway_nginx).toBeDefined();
    expect(metricsData.gateway_nginx.active_connections).toBeDefined();
  }, 40000);
});
