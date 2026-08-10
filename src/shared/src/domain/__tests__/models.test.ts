import { describe, it, expect } from 'vitest';
import { Telemetry, Asset } from '../models.js';

describe('Domain Models', () => {
  it('should construct a valid Telemetry object', () => {
    const telemetry: Telemetry = {
      asset_id: 'asset-1',
      latitude: 40.7128,
      longitude: -74.006,
      timestamp: new Date().toISOString(),
    };

    expect(telemetry.asset_id).toBe('asset-1');
    expect(telemetry.latitude).toBe(40.7128);
    expect(telemetry.longitude).toBe(-74.006);
    expect(telemetry.timestamp).toBeDefined();
  });

  it('should construct a valid Asset object', () => {
    const asset: Asset = {
      id: 'asset-1',
      driver_name: 'Alice',
      created_at: new Date().toISOString(),
    };

    expect(asset.id).toBe('asset-1');
    expect(asset.driver_name).toBe('Alice');
    expect(asset.created_at).toBeDefined();
  });
});
