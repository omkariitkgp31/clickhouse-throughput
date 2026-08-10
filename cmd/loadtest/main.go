package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	NumAssets    = 100000
	NumWorkers   = 200
	TestDuration = 60 * time.Second
	BatchSize    = 1000
)

var hubs = [][]float64{
	{40.7128, -74.0060}, {34.0522, -118.2437}, {41.8781, -87.6298}, {19.4326, -99.1332}, {43.6532, -79.3832},
	{-23.5505, -46.6333}, {-34.6037, -58.3816}, {4.7110, -74.0721}, {-12.0464, -77.0428},
	{51.5074, -0.1278}, {48.8566, 2.3522}, {52.5200, 13.4050}, {40.4168, -3.7038}, {41.9028, 12.4964}, {55.7558, 37.6173},
	{30.0444, 31.2357}, {6.5244, 3.3792}, {-26.2041, 28.0473}, {-1.2921, 36.8219}, {25.2048, 55.2708}, {24.7136, 46.6753},
	{28.6139, 77.2090}, {19.0760, 72.8777}, {39.9042, 116.4074}, {31.2304, 121.4737}, {35.6762, 139.6503}, {37.5665, 126.9780}, {13.7563, 100.5018}, {-6.2088, 106.8456},
	{-33.8688, 151.2093}, {-37.8136, 144.9631}, {-36.8485, 174.7633},
}

type Telemetry struct {
	AssetID   string  `json:"asset_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func main() {
	apiHost := getEnv("API_HOST", "localhost")
	chURL := getEnv("CLICKHOUSE_URL", "http://localhost:8123")
	apiURL := fmt.Sprintf("http://%s/api/v1/telemetry", apiHost)

	log.Println("==================================================")
	log.Println("🚀 FLEET TRACKER LOAD TESTER")
	log.Println("==================================================")

	log.Printf("Connecting to ClickHouse at %s...", chURL)
	log.Printf("Inserting %d dummy assets into database (this may take a moment)...", NumAssets)
	insertAssets(chURL)
	log.Println("✅ Database ready.")

	log.Printf("Starting bombardment of %s with %d concurrent workers for %v...", apiURL, NumWorkers, TestDuration)
	
	var successCount int64
	var failCount int64
	var totalLatency int64 // in microseconds

	var errMu sync.Mutex
	errStats := make(map[string]int)

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
		},
	}

	start := time.Now()
	var wg sync.WaitGroup
	ctxCancel, cancel := context.WithTimeout(context.Background(), TestDuration)
	defer cancel()

	for i := 0; i < NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctxCancel.Done():
					return
				default:
					assetID := fmt.Sprintf("asset-%d", rand.Intn(NumAssets)+1)

					hub := hubs[rand.Intn(len(hubs))]
					
					payload := Telemetry{
						AssetID:   assetID,
						Latitude:  hub[0] + rand.NormFloat64()*5.0,
						Longitude: hub[1] + rand.NormFloat64()*5.0,
					}
					
					b, _ := json.Marshal(payload)
					
					reqStart := time.Now()
					req, _ := http.NewRequestWithContext(ctxCancel, "POST", apiURL, bytes.NewBuffer(b))
					req.Header.Set("Content-Type", "application/json")
					
					resp, err := client.Do(req)
					if err != nil {
						atomic.AddInt64(&failCount, 1)
						errStr := err.Error()
						if len(errStr) > 50 { errStr = errStr[:50] + "..." }
						errMu.Lock()
						errStats[errStr]++
						errMu.Unlock()
						continue
					}
					
					if resp.StatusCode == http.StatusAccepted {
						atomic.AddInt64(&successCount, 1)
						atomic.AddInt64(&totalLatency, time.Since(reqStart).Microseconds())
					} else {
						atomic.AddInt64(&failCount, 1)
						errStr := fmt.Sprintf("HTTP %d", resp.StatusCode)
						errMu.Lock()
						errStats[errStr]++
						errMu.Unlock()
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Allow some time for TCP sockets to recover from ephemeral port exhaustion
	time.Sleep(3 * time.Second)
	metricsClient := &http.Client{Timeout: 10 * time.Second}
	metricsURL := fmt.Sprintf("http://%s/api/v1/system/metrics", apiHost)
	var sysMetrics string
	
	// Retry loop for fetching metrics
	var resp *http.Response
	for retries := 0; retries < 5; retries++ {
		resp, err = metricsClient.Get(metricsURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(2 * time.Second)
	}

	if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
			pretty, _ := json.MarshalIndent(result, "", "  ")
			sysMetrics = string(pretty)
		}
		resp.Body.Close()
	} else {
		sysMetrics = fmt.Sprintf("Failed to fetch system metrics from %s after retries", metricsURL)
	}

	totalReqs := successCount + failCount
	rps := float64(totalReqs) / elapsed.Seconds()
	avgLatency := 0.0
	if successCount > 0 {
		avgLatency = float64(totalLatency) / float64(successCount) / 1000.0 // ms
	}

	errBreakdown := ""
	for k, v := range errStats {
		errBreakdown += fmt.Sprintf("  - %d x %s\n", v, k)
	}
	if errBreakdown == "" {
		errBreakdown = "  (None)\n"
	}

	report := fmt.Sprintf(`
==================================================
📊 LOAD TEST RESULTS (CV METRICS)
==================================================
Total Time:          %.2f seconds
Total Requests:      %d
Successful Requests: %d
Failed Requests:     %d
Error Breakdown:
%s--------------------------------------------------
🚀 THROUGHPUT:       %.2f Requests / Second
⚡ AVG LATENCY:      %.2f ms
==================================================
🏗️ SYSTEM OBSERVABILITY HEALTH
==================================================
%s
==================================================
`, elapsed.Seconds(), totalReqs, successCount, failCount, errBreakdown, rps, avgLatency, sysMetrics)

	fmt.Println(report)

	f, err := os.OpenFile("loadtest_results.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		timestamp := time.Now().Format(time.RFC3339)
		f.WriteString(fmt.Sprintf("\n--- TEST RUN: %s ---\n", timestamp))
		f.WriteString(report)
		f.Close()
		fmt.Println("✅ Report successfully appended to loadtest_results.log")
	} else {
		fmt.Printf("⚠️ Failed to write log file: %v\n", err)
	}
}

func insertAssets(chURL string) {
	client := &http.Client{Timeout: 30 * time.Second}
	url := fmt.Sprintf("%s/?query=INSERT%%20INTO%%20fleet.assets%%20FORMAT%%20JSONEachRow", chURL)

	for i := 1; i <= NumAssets; i += BatchSize {
		var buf bytes.Buffer
		for j := i; j < i+BatchSize && j <= NumAssets; j++ {
			assetID := fmt.Sprintf("asset-%d", j)
			driverName := fmt.Sprintf("Node %d", j)
			row := map[string]string{"id": assetID, "driver_name": driverName}
			b, _ := json.Marshal(row)
			buf.Write(b)
			buf.WriteString("\n")
		}

		req, err := http.NewRequest("POST", url, &buf)
		if err != nil {
			log.Fatalf("Failed to create HTTP request for assets insert: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Fatalf("Failed to insert assets into ClickHouse: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
