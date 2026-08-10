import express, { Request, Response } from 'express';
import { KafkaProducer, Telemetry } from '@fleet-tracker/shared';

const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:19092').split(',');
const kafkaTopic = process.env.KAFKA_TOPIC || 'telemetry';
const port = parseInt(process.env.PORT || '8080', 10);

const producer = new KafkaProducer(kafkaBrokers, kafkaTopic);

const app = express();
app.use(express.json());

app.post('/api/v1/telemetry', async (req: Request, res: Response) => {
  const { asset_id, latitude, longitude } = req.body || {};

  if (!asset_id || typeof latitude !== 'number' || typeof longitude !== 'number') {
    return res.status(400).json({
      error: "Key: 'req.AssetID' Error:Field validation for 'AssetID' failed on the 'required' tag",
    });
  }

  const telemetry: Telemetry = {
    asset_id,
    latitude,
    longitude,
    timestamp: new Date().toISOString(),
  };

  try {
    await producer.produce(telemetry);
    return res.status(202).json({ status: 'accepted' });
  } catch (err) {
    console.error('Failed to produce telemetry to Kafka:', err);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

const server = app.listen(port, () => {
  console.log(`Ingestion API server listening on port ${port}`);
});

process.on('SIGTERM', async () => {
  console.log('Shutting down Ingestion API...');
  server.close();
  await producer.close();
  process.exit(0);
});
