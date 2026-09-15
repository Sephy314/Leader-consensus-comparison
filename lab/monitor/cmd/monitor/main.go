// Command monitor samples per-container resource usage and writes it to a
// CSV. It is a thin standalone wrapper around the monitor package; the
// experiment runner uses the same package in-process.
package main

import (
	"encoding/csv"
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"conslab/monitor"
)

var (
	runID     = flag.String("run-id", "run", "run identifier")
	protocol  = flag.String("protocol", "raft", "protocol")
	role      = flag.String("role", "replica", "role: replica|client|master")
	replicaID = flag.Int("replica-id", -1, "replica id (-1 for non-replica)")
	container = flag.String("container", "", "container name")
	interval  = flag.Duration("interval", 200*time.Millisecond, "sampling interval")
	duration  = flag.Duration("duration", 60*time.Second, "total sampling duration")
	out       = flag.String("out", "resources.csv", "output CSV path")
)

func main() {
	flag.Parse()
	if *container == "" {
		log.Fatal("-container is required")
	}
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{
		"run_id", "protocol", "role", "replica_id", "container", "ts_ns",
		"cpu_usage_usec", "cpu_user_usec", "cpu_system_usec", "net_rx_bytes", "net_tx_bytes", "rss_bytes",
	})
	deadline := time.Now().Add(*duration)
	for time.Now().Before(deadline) {
		s, err := monitor.SampleContainer(*runID, *protocol, *role, *replicaID, *container)
		if err != nil {
			log.Printf("sample %s: %v", *container, err)
		} else {
			w.Write([]string{
				s.RunID, s.Protocol, s.Role, strconv.Itoa(s.ReplicaID), s.Container,
				strconv.FormatInt(s.TS.UnixNano(), 10),
				strconv.FormatInt(s.CPUUsageUS, 10), strconv.FormatInt(s.CPUUserUS, 10),
				strconv.FormatInt(s.CPUSystemUS, 10), strconv.FormatInt(s.NetRXBytes, 10),
				strconv.FormatInt(s.NetTXBytes, 10), strconv.FormatInt(s.RSSBytes, 10),
			})
			w.Flush()
		}
		time.Sleep(*interval)
	}
	log.Printf("monitor done")
}
