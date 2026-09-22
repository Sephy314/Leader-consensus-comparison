package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"conslab/internal/labcfg"
)

// runSmoke executes the staged smoke tests. Each stage must pass before the
// next is attempted. The full matrix is never started by this command.
func runSmoke(s SmokeSpec) error {
	// Normalize: the smoke JSON may omit fields, so apply defaults and make
	// the non-failure stages explicitly failure-free.
	base := labcfg.Default()
	base.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
	applyRunDefaults(&s.Base, base)
	applyRunDefaults(&s.Basic, base)
	applyRunDefaults(&s.Small, base)

	// Smoke runs are functional gates, not measured experiments: they write
	// to results/smoke-raw so they never pollute the measured dataset.
	env := smokeEnv()

	fmt.Println("################ SMOKE STAGE 1/6: build ################")
	if err := cmdBuild(); err != nil {
		return fmt.Errorf("stage 1 (build) failed: %w", err)
	}
	fmt.Println("stage 1 PASS")

	fmt.Println("\n################ SMOKE STAGE 2/6: minimal clusters ################")
	clusterCfg := s.Basic
	clusterCfg.DurationS = 3
	clusterCfg.WarmupS = 2
	clusterCfg.Repetitions = 1
	clusterCfg.Concurrency = 1
	clusterCfg.WritePct = 50
	clusterCfg.ReadPct = 50
	for _, tgt := range s.smokeTargets() {
		cfg := clusterCfg
		cfg.Protocol = tgt.proto
		cfg.Implementation = tgt.impl
		res := executeRun(cfg, 1, env, nil)
		if res.Status != "success" {
			return fmt.Errorf("stage 2 (%s/%s minimal cluster) failed: %s", cfg.Protocol, cfg.Impl(), res.Reason)
		}
		fmt.Printf("  %s/%s minimal cluster PASS (%s)\n", cfg.Protocol, cfg.Impl(), res.RunID)
	}
	fmt.Println("stage 2 PASS")

	fmt.Println("\n################ SMOKE STAGE 3/6: basic PUT/GET requests ################")
	for _, tgt := range s.smokeTargets() {
		for _, w := range []int{0, 100} {
			cfg := s.Basic
			cfg.Protocol = tgt.proto
			cfg.Implementation = tgt.impl
			cfg.WritePct = w
			cfg.ReadPct = 100 - w
			cfg.DurationS = 3
			cfg.WarmupS = 1
			cfg.Repetitions = 1
			cfg.Concurrency = 2
			res := executeRun(cfg, 1, env, nil)
			if res.Status != "success" {
				return fmt.Errorf("stage 3 (%s/%s %d%% writes) failed: %s", cfg.Protocol, cfg.Impl(), w, res.Reason)
			}
			stat, err := readRequestStats(filepath.Join(res.Dir, "requests.csv"))
			if err != nil {
				return fmt.Errorf("stage 3 (%s/%s): %w", cfg.Protocol, cfg.Impl(), err)
			}
			if stat.total == 0 {
				return fmt.Errorf("stage 3 (%s/%s %d%% writes): no requests recorded", cfg.Protocol, cfg.Impl(), w)
			}
			if stat.ok != stat.total {
				return fmt.Errorf("stage 3 (%s/%s %d%% writes): %d/%d succeeded", cfg.Protocol, cfg.Impl(), w, stat.ok, stat.total)
			}
			fmt.Printf("  %s/%s %3d%% writes: %d/%d ok PASS\n", cfg.Protocol, cfg.Impl(), w, stat.ok, stat.total)
		}
	}
	fmt.Println("stage 3 PASS")

	fmt.Println("\n################ SMOKE STAGE 4/6: small mixed workload ################")
	for _, tgt := range s.smokeTargets() {
		cfg := s.Small
		cfg.Protocol = tgt.proto
		cfg.Implementation = tgt.impl
		cfg.Repetitions = 1
		res := executeRun(cfg, 1, env, nil)
		if res.Status != "success" {
			return fmt.Errorf("stage 4 (%s/%s small workload) failed: %s", cfg.Protocol, cfg.Impl(), res.Reason)
		}
		stat, err := readRequestStats(filepath.Join(res.Dir, "requests.csv"))
		if err != nil {
			return fmt.Errorf("stage 4 (%s/%s): %w", cfg.Protocol, cfg.Impl(), err)
		}
		fmt.Printf("  %s/%s small workload: %d requests, %.1f%% ok, p95=%.2fms PASS\n",
			cfg.Protocol, cfg.Impl(), stat.total, 100*float64(stat.ok)/float64(stat.total), stat.p95ms())
	}
	fmt.Println("stage 4 PASS")

	fmt.Println("\n################ SMOKE STAGE 5/6: failure injection ################")
	for _, c := range s.Failures {
		cfg := s.Base
		cfg.Protocol = c.Protocol
		cfg.Replicas = c.Replicas
		cfg.WritePct = c.WritePct
		cfg.ReadPct = 100 - c.WritePct
		cfg.Concurrency = c.Concurrency
		cfg.Repetitions = 1
		cfg.Failure = labcfg.Failure{Mode: c.Mode, AtS: c.AtS, RestartAfterS: c.RestartAfterS}
		res := executeRun(cfg, 1, env, nil)
		if res.Status != "success" {
			return fmt.Errorf("stage 5 (%s %s failure) failed: %s", c.Protocol, c.Mode, res.Reason)
		}
		evs, err := readEvents(filepath.Join(res.Dir, "events.csv"))
		if err != nil {
			return fmt.Errorf("stage 5 (%s %s): %w", c.Protocol, c.Mode, err)
		}
		if !evs["failure_confirmed"] {
			return fmt.Errorf("stage 5 (%s %s): no failure_confirmed event recorded", c.Protocol, c.Mode)
		}
		fmt.Printf("  %s %s failure smoke PASS (%s)\n", c.Protocol, c.Mode, res.RunID)
	}
	fmt.Println("stage 5 PASS")

	fmt.Println("\n################ SMOKE STAGE 6/6: metrics ################")
	if err := checkMetricsInventory(env.root); err != nil {
		return fmt.Errorf("stage 6 (metrics) failed: %w", err)
	}
	fmt.Println("stage 6 PASS")

	fmt.Println("\n################ SMOKE COMPLETE ################")
	fmt.Println("all smoke stages passed; the full matrix (runner matrix) was NOT started")
	return nil
}

// readRequestStats reads a requests.csv and returns basic counters.
type requestStats struct {
	total  int
	ok     int
	lats   []int64
	byProt map[string]int
}

func readRequestStats(path string) (requestStats, error) {
	st := requestStats{byProt: map[string]int{}}
	f, err := os.Open(path)
	if err != nil {
		return st, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return st, err
	}
	if len(rows) < 2 {
		return st, nil
	}
	hdr := map[string]int{}
	for i, h := range rows[0] {
		hdr[h] = i
	}
	for _, row := range rows[1:] {
		st.total++
		if row[hdr["ok"]] == "1" {
			st.ok++
		}
		if lat, err := strconv.ParseInt(row[hdr["latency_ns"]], 10, 64); err == nil {
			st.lats = append(st.lats, lat)
		}
		st.byProt[row[hdr["protocol"]]]++
	}
	return st, nil
}

func (s requestStats) p95ms() float64 {
	if len(s.lats) == 0 {
		return 0
	}
	sorted := append([]int64(nil), s.lats...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(0.95 * float64(len(sorted)-1))
	return float64(sorted[idx]) / 1e6
}

// readEvents returns the set of event names recorded in events.csv.
func readEvents(path string) (map[string]bool, error) {
	out := map[string]bool{}
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return out, err
	}
	if len(rows) < 2 {
		return out, nil
	}
	hdr := map[string]int{}
	for i, h := range rows[0] {
		hdr[h] = i
	}
	for _, row := range rows[1:] {
		out[row[hdr["event"]]] = true
	}
	return out, nil
}

// checkMetricsInventory verifies the most recent runs produced every expected
// metrics file with the expected columns. root is the results root the smoke
// session wrote to (smoke runs never write to the measured dataset).
func checkMetricsInventory(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	checked := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		metaPath := filepath.Join(dir, "metadata.json")
		fi, err := os.Stat(metaPath)
		if err != nil {
			continue
		}
		// only inspect runs from this smoke session
		if fi.ModTime().Before(time.Now().Add(-20 * time.Minute)) {
			continue
		}
		checked++
		for _, want := range []string{"metadata.json", "requests.csv", "resources.csv", "events.csv", "compose.yaml"} {
			if _, err := os.Stat(filepath.Join(dir, want)); err != nil {
				return fmt.Errorf("%s: missing %s", e.Name(), want)
			}
		}
		// resource samples must cover every replica
		roles, err := readResourceRoles(filepath.Join(dir, "resources.csv"))
		if err != nil {
			return fmt.Errorf("%s: %w", e.Name(), err)
		}
		if len(roles) == 0 {
			return fmt.Errorf("%s: resources.csv has no samples", e.Name())
		}
	}
	if checked == 0 {
		return fmt.Errorf("no recent runs found to validate")
	}
	fmt.Printf("  validated metrics for %d recent runs\n", checked)
	return nil
}

func readResourceRoles(path string) (map[string]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	if len(rows) < 2 {
		return out, nil
	}
	hdr := map[string]int{}
	for i, h := range rows[0] {
		hdr[h] = i
	}
	for _, row := range rows[1:] {
		out[row[hdr["role"]]+"-"+row[hdr["replica_id"]]]++
	}
	return out, nil
}
