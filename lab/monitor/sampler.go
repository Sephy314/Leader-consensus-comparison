// Package monitor samples per-container resource usage from the host.
//
// Each replica runs in its own container (its own network namespace), so
// per-replica network RX/TX is only measurable from the host. For a container
// we read:
//
//   - cumulative CPU time from its cgroup v2 cpu.stat (usage/user/system usec)
//   - cumulative network bytes from /proc/<pid>/net/dev (the container's
//     network namespace)
//   - resident memory from /proc/<pid>/status (VmRSS)
//
// Values are stored raw (cumulative); rates are derived in the processing
// pipeline. No replica data is aggregated before storage.
package monitor

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Sample is one raw resource sample for one container.
type Sample struct {
	RunID       string    `json:"run_id"`
	Protocol    string    `json:"protocol"`
	Role        string    `json:"role"`       // replica | client | master
	ReplicaID   int       `json:"replica_id"` // -1 for non-replica roles
	Container   string    `json:"container"`
	TS          time.Time `json:"ts"`
	CPUUsageUS  int64     `json:"cpu_usage_usec"` // cumulative
	CPUUserUS   int64     `json:"cpu_user_usec"`
	CPUSystemUS int64     `json:"cpu_system_usec"`
	NetRXBytes  int64     `json:"net_rx_bytes"` // cumulative
	NetTXBytes  int64     `json:"net_tx_bytes"`
	RSSBytes    int64     `json:"rss_bytes"`
}

// Target identifies a container to sample.
type Target struct {
	Role      string
	ReplicaID int
	Container string
}

// SampleAll samples every target, resolving all container PIDs with a single
// docker inspect call per round.
func SampleAll(runID, protocol string, targets []Target) []Sample {
	now := time.Now()
	pids := resolvePids(targets)
	samples := make([]Sample, 0, len(targets))
	for _, t := range targets {
		s := Sample{
			RunID: runID, Protocol: protocol, Role: t.Role,
			ReplicaID: t.ReplicaID, Container: t.Container, TS: now,
		}
		pid, ok := pids[t.Container]
		if !ok || pid <= 0 {
			continue
		}
		cgPath, err := cgroupPath(pid)
		if err != nil {
			continue
		}
		if err := readCPUStat(cgPath, &s); err != nil {
			continue
		}
		if err := readNetDev(pid, &s); err != nil {
			continue
		}
		_ = readRSS(pid, &s)
		samples = append(samples, s)
	}
	return samples
}

// SampleContainer samples one container by name (used by the standalone
// monitor binary).
func SampleContainer(runID, protocol, role string, replicaID int, container string) (Sample, error) {
	s := Sample{
		RunID: runID, Protocol: protocol, Role: role, ReplicaID: replicaID,
		Container: container, TS: time.Now(),
	}
	pid, err := containerPid(container)
	if err != nil {
		return s, err
	}
	cgPath, err := cgroupPath(pid)
	if err != nil {
		return s, err
	}
	if err := readCPUStat(cgPath, &s); err != nil {
		return s, err
	}
	if err := readNetDev(pid, &s); err != nil {
		return s, err
	}
	if err := readRSS(pid, &s); err != nil {
		return s, err
	}
	return s, nil
}

// resolvePids maps container name -> PID using one docker inspect call.
func resolvePids(targets []Target) map[string]int {
	out := map[string]int{}
	if len(targets) == 0 {
		return out
	}
	args := []string{"inspect", "-f", "{{.Name}} {{.State.Pid}}"}
	for _, t := range targets {
		args = append(args, t.Container)
	}
	cmd := exec.Command("docker", args...)
	data, err := cmd.Output()
	if err != nil {
		return out
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		name := strings.TrimPrefix(fields[0], "/")
		pid, err := strconv.Atoi(fields[1])
		if err != nil {
			continue
		}
		out[name] = pid
	}
	return out
}

func containerPid(name string) (int, error) {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Pid}}", name).Output()
	if err != nil {
		return 0, fmt.Errorf("docker inspect %s: %w", name, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0, fmt.Errorf("parsing pid for %s: %w", name, err)
	}
	return pid, nil
}

// cgroupPath returns the cgroup v2 path of a process (e.g.
// /system.slice/docker-<id>.scope).
func cgroupPath(pid int) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", err
	}
	line := strings.TrimSpace(string(data))
	parts := strings.SplitN(line, "::", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("unexpected cgroup line %q", line)
	}
	return parts[1], nil
}

func readCPUStat(cgPath string, s *Sample) error {
	data, err := os.ReadFile("/sys/fs/cgroup" + cgPath + "/cpu.stat")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "usage_usec":
			s.CPUUsageUS = v
		case "user_usec":
			s.CPUUserUS = v
		case "system_usec":
			s.CPUSystemUS = v
		}
	}
	return nil
}

// readNetDev reads cumulative RX/TX bytes for the container's network
// namespace. Only non-loopback interfaces are summed.
func readNetDev(pid int, s *Sample) error {
	f, err := os.Open(fmt.Sprintf("/proc/%d/net/dev", pid))
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		iface := strings.TrimSpace(parts[0])
		if iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 9 {
			continue
		}
		rx, err1 := strconv.ParseInt(fields[0], 10, 64)
		tx, err2 := strconv.ParseInt(fields[8], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		s.NetRXBytes += rx
		s.NetTXBytes += tx
	}
	return sc.Err()
}

func readRSS(pid int, s *Sample) error {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					s.RSSBytes = v * 1024 // kB -> bytes
				}
			}
			return nil
		}
	}
	return nil
}
