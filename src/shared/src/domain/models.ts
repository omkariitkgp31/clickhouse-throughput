export interface Asset {
  id: string;
  driver_name: string;
  created_at: string;
}

export interface Telemetry {
  asset_id: string;
  latitude: number;
  longitude: number;
  timestamp: string;
}
