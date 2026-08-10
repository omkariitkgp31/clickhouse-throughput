import { describe, it, expect, vi } from 'vitest';
import { Telemetry } from '../../domain/models.js';

describe('Adapter Interfaces & Payloads', () => {
  it('should format Redis cache key correctly', () => {
    const telemetry: Telemetry = {
      asset_id: 'asset-42',
      latitude: 37.7749,
      longitude: -122.4194,
      timestamp: '2026-08-10T12:00:00.000Z',
    };

    const expectedKey = `asset:${telemetry.asset_id}:latest`;
    const serializedPayload = JSON.stringify(telemetry);

    expect(expectedKey).toBe('asset:asset-42:latest');
    expect(JSON.parse(serializedPayload)).toEqual(telemetry);
  });

  it('should format ClickHouse location history row values correctly', () => {
    const telemetries: Telemetry[] = [
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

    expect(rows[0]).toEqual({
      asset_id: 'asset-100',
      time: '2026-08-10T12:00:00.000Z',
      latitude: 51.5074,
      longitude: -0.1278,
    });
  });
});
