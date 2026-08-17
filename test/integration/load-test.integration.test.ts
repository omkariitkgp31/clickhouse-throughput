import { describe, it, expect } from 'vitest';
import { Kafka } from 'kafkajs';
import { createClient } from '@clickhouse/client';

describe('10,000-Message Ingestion Load Test', () => {
  const TOTAL_MESSAGES = 10000;
  const CONCURRENCY = 80;
  const ASSET_POOL_SIZE = 1000;
  const runId = `run-${Date.now()}`;
  const nginxHost = 'http://localhost';
  const brokers = ['localhost:19092'];
  const topic = 'telemetry';
  const consumerGroup = 'stream-processor-group';

  const chClient = createClient({
    url: 'http://localhost:8123',
    database: 'fleet',
    username: 'default',
    password: '',
  });

  const kafka = new Kafka({ clientId: `loadtest-monitor-${Date.now()}`, brokers });
  const admin = kafka.admin();

  async function getConsumerLag(): Promise<number> {
    try {
      const [topicOffsets, groupOffsets] = await Promise.all([
        admin.fetchTopicOffsets(topic),
        admin.fetchOffsets({ groupId: consumerGroup, topics: [topic] }),
      ]);

      const groupTopic = groupOffsets.find((g: any) => g.topic === topic);
      if (!groupTopic) return 0;

      let totalLag = 0;
      for (const tPart of topicOffsets) {
        const gPart = groupTopic.partitions.find((p: any) => p.partition === tPart.partition);
        const high = BigInt(tPart.high);
        const current = gPart && gPart.offset !== '-1' ? BigInt(gPart.offset) : 0n;
        if (high > current) {
          totalLag += Number(high - current);
        }
      }
      return totalLag;
    } catch {
      return 0;
    }
  }

  async function getNginxRequestsRouted(): Promise<number> {
    try {
      const res = await fetch(`${nginxHost}/nginx_status`);
      if (!res.ok) return 0;
      const text = await res.text();
      const lines = text.split('\n');
      if (lines.length >= 3) {
        const counts = lines[2].trim().split(/\s+/);
        return parseInt(counts[2] || '0', 10);
      }
    } catch {}
    return 0;
  }

  it('runs 10,000 requests through Nginx, verifying load balancing, lag drain, and persistence', async () => {
    await admin.connect();

    const nginxStartReqs = await getNginxRequestsRouted();
    const initialLag = await getConsumerLag();

    const replicaCounts: Record<string, number> = {};
    let acceptedCount = 0;
    let failedCount = 0;
    const latencies: number[] = [];

    // Worker pool execution
    const assetIds = Array.from({ length: ASSET_POOL_SIZE }, (_, i) => `load-test-${runId}-asset-${i}`);
    let currentIndex = 0;
    const startTime = Date.now();

    async function worker() {
      while (true) {
        const idx = currentIndex++;
        if (idx >= TOTAL_MESSAGES) break;

        const assetId = assetIds[idx % ASSET_POOL_SIZE];
        const payload = {
          asset_id: assetId,
          latitude: 37.0 + (idx % 100) * 0.01,
          longitude: -122.0 - (idx % 100) * 0.01,
        };

        let sent = false;
        for (let attempt = 0; attempt < 3; attempt++) {
          const reqStart = Date.now();
          try {
            const res = await fetch(`${nginxHost}/api/v1/telemetry`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(payload),
            });

            const latency = Date.now() - reqStart;
            latencies.push(latency);

            if (res.status === 202) {
              acceptedCount++;
              const instanceId = res.headers.get('x-instance-id') || 'unknown';
              replicaCounts[instanceId] = (replicaCounts[instanceId] || 0) + 1;
              sent = true;
              break;
            }
          } catch {
            await new Promise((r) => setTimeout(r, 50));
          }
        }
        if (!sent) {
          failedCount++;
        }
      }
    }

    const workers = Array.from({ length: CONCURRENCY }, () => worker());
    await Promise.all(workers);

    const totalDurationMs = Date.now() - startTime;
    const throughput = (acceptedCount / (totalDurationMs / 1000)).toFixed(2);
    const avgLatency = (latencies.reduce((a, b) => a + b, 0) / latencies.length).toFixed(2);

    console.log(`--- Load Test Execution Summary ---`);
    console.log(`Total Requests Sent: ${TOTAL_MESSAGES}`);
    console.log(`Accepted (202): ${acceptedCount}, Failed: ${failedCount}`);
    console.log(`Duration: ${(totalDurationMs / 1000).toFixed(2)}s, Throughput: ${throughput} req/s, Avg Latency: ${avgLatency}ms`);
    console.log(`Replica Distribution:`, JSON.stringify(replicaCounts, null, 2));

    // 1. Success Rate Assertion
    expect(acceptedCount).toBe(TOTAL_MESSAGES);
    expect(failedCount).toBe(0);

    // 2. Load Balancer Distribution Assertion
    const replicas = Object.keys(replicaCounts);
    expect(replicas.length).toBe(3); // All 3 ingestion-api replicas must have received traffic

    for (const [replica, count] of Object.entries(replicaCounts)) {
      const percentage = count / acceptedCount;
      console.log(`Replica ${replica}: ${count} requests (${(percentage * 100).toFixed(1)}%)`);
      // Assert reasonably even split across 3 replicas: between 20% and 45%
      expect(percentage).toBeGreaterThanOrEqual(0.20);
      expect(percentage).toBeLessThanOrEqual(0.45);
    }

    // 3. Nginx status delta
    const nginxEndReqs = await getNginxRequestsRouted();
    const nginxDelta = nginxEndReqs - nginxStartReqs;
    expect(nginxDelta).toBeGreaterThanOrEqual(TOTAL_MESSAGES);

    // 4. Kafka Consumer Lag Drain Check
    let finalLag = await getConsumerLag();
    const lagPollStart = Date.now();
    while (finalLag > 0 && Date.now() - lagPollStart < 30000) {
      await new Promise((res) => setTimeout(res, 500));
      finalLag = await getConsumerLag();
    }
    console.log(`Final Consumer Lag after draining: ${finalLag}`);
    expect(finalLag).toBeLessThanOrEqual(5);

    // 5. ClickHouse Persistence Check
    let clickhouseRows = 0;
    const chPollStart = Date.now();
    while (clickhouseRows < acceptedCount && Date.now() - chPollStart < 30000) {
      const countRes = await chClient.query({
        query: `SELECT count() AS count FROM location_history WHERE asset_id LIKE {prefix: String}`,
        query_params: { prefix: `load-test-${runId}-%` },
        format: 'JSONEachRow',
      });
      const countJson: Array<{ count: string }> = await countRes.json();
      clickhouseRows = parseInt(countJson[0]?.count || '0', 10);
      if (clickhouseRows >= acceptedCount) break;
      await new Promise((res) => setTimeout(res, 500));
    }

    console.log(`ClickHouse Verified Rows: ${clickhouseRows} (Expected: ${acceptedCount})`);
    expect(clickhouseRows).toBe(acceptedCount);

    const fs = await import('fs');
    fs.writeFileSync(
      'test/.loadtest-summary.json',
      JSON.stringify(
        {
          totalRequests: TOTAL_MESSAGES,
          accepted: acceptedCount,
          failed: failedCount,
          duration: `${(totalDurationMs / 1000).toFixed(2)}s`,
          throughput: `${throughput} req/s`,
          avgLatency: `${avgLatency} ms`,
          replicas: replicaCounts,
          clickhouseVerified: clickhouseRows,
        },
        null,
        2
      ),
      'utf8'
    );

    await admin.disconnect();
    await chClient.close();
  }, 120000);
});
