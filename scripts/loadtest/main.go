// Command loadtest fires concurrent publish requests at a running broker and
// reports real measured throughput and latency percentiles. This replaces
// the PRD's asserted "50K events/sec" claim with an honest number for
// whatever machine this actually runs on.
//
// Note (Windows clients only): at concurrency above ~20-30 against a
// localhost/Docker-forwarded port, Windows can exhaust ephemeral outbound
// ports ("Only one usage of each socket address...") purely as a client-side
// TCP limitation unrelated to the broker's own capacity - the broker logs
// show every request that actually reached it succeeding. Keep -concurrency
// modest on Windows, or run the load generator from inside a Linux
// container/WSL against the broker's container network instead.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type publishBody struct {
	Key   string `json:"key"`
	Value struct {
		N int64 `json:"n"`
	} `json:"value"`
}

func main() {
	baseURL := flag.String("url", "http://localhost:8060", "broker base URL")
	topic := flag.String("topic", "loadtest", "topic name (created if missing)")
	partitions := flag.Int("partitions", 8, "partitions for the load-test topic")
	concurrency := flag.Int("concurrency", 16, "concurrent producer goroutines (keep modest on Windows, see package doc)")
	total := flag.Int("total", 50000, "total messages to publish")
	flag.Parse()

	// The default http.Transport caps idle connections per host at 2, which
	// starves a highly concurrent producer of reusable connections and shows
	// up as client-side dial/timeout failures that have nothing to do with
	// the broker's own capacity. Raise the caps to match concurrency.
	transport := &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		MaxConnsPerHost:     0,
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: transport}

	createTopic(client, *baseURL, *topic, *partitions)

	perGoroutine := *total / *concurrency
	actualTotal := perGoroutine * *concurrency // total may not divide evenly by concurrency

	var (
		sent      int64
		failed    int64
		latMu     sync.Mutex
		latencies = make([]time.Duration, 0, actualTotal)
		wg        sync.WaitGroup
	)

	start := time.Now()

	for g := 0; g < *concurrency; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				idx := g*perGoroutine + i
				body := publishBody{Key: fmt.Sprintf("key-%d", idx%1000)}
				body.Value.N = int64(idx)
				payload, _ := json.Marshal(body)

				reqStart := time.Now()
				resp, err := client.Post(fmt.Sprintf("%s/api/v1/events/%s", *baseURL, *topic), "application/json", bytes.NewReader(payload))
				elapsed := time.Since(reqStart)

				if err != nil || resp.StatusCode >= 300 {
					n := atomic.AddInt64(&failed, 1)
					if n <= 5 {
						if err != nil {
							fmt.Printf("sample failure: %v\n", err)
						} else {
							b, _ := io.ReadAll(resp.Body)
							fmt.Printf("sample failure: status=%d body=%s\n", resp.StatusCode, string(b))
						}
					}
					if resp != nil {
						resp.Body.Close()
					}
					continue
				}
				resp.Body.Close()
				latMu.Lock()
				latencies = append(latencies, elapsed)
				latMu.Unlock()
				atomic.AddInt64(&sent, 1)
			}
		}(g)
	}

	wg.Wait()
	totalElapsed := time.Since(start)

	if len(latencies) == 0 {
		fmt.Println("no successful requests; nothing to report")
		return
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]

	fmt.Println("--- load test results (real measured, not asserted) ---")
	fmt.Printf("messages sent:      %d\n", sent)
	fmt.Printf("messages failed:    %d\n", failed)
	fmt.Printf("wall time:          %s\n", totalElapsed)
	fmt.Printf("throughput:         %.0f events/sec\n", float64(sent)/totalElapsed.Seconds())
	fmt.Printf("publish latency p50: %s\n", p50)
	fmt.Printf("publish latency p99: %s\n", p99)
}

func createTopic(client *http.Client, baseURL, topic string, partitions int) {
	body, _ := json.Marshal(map[string]interface{}{
		"name":               topic,
		"partitions":         partitions,
		"replication_factor": 1,
		"retention_ms":       3600000,
	})
	resp, err := client.Post(baseURL+"/api/v1/topics/", "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Printf("warning: create topic request failed: %v (continuing, topic may already exist)\n", err)
		return
	}
	resp.Body.Close()
}
