package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"conslab/internal/labcfg"
)

// OS-level network emulation for the network-delay experiment.
//
// Why this is not in the adapters: an adapter-level sleep makes the injected
// latency a property of one implementation's send path, so two implementations
// receive different conditions and the measurement describes the injection
// point rather than the protocol. Here the delay is installed by the runner in
// the kernel traffic-control layer, inside each replica's own network
// namespace, so every implementation receives exactly the same network
// condition and no consensus code is touched.
//
// The legacy in-adapter mechanism (labcfg.CommCostMS) is left untouched so the
// archived commcost dataset keeps its exact meaning; the two mechanisms are
// never combined (labcfg.Validate rejects a run that sets both) and their
// results are never pooled.
//
// Traffic scope: with filter "peers" the emulation is applied only to packets
// addressed to another replica, so client-observed request latency is not
// polluted by an emulation that the consensus path alone should receive. Every
// replica delays its own egress, so an inter-replica round trip accumulates the
// configured delay once per hop, matching the one-way semantics the legacy
// commcost points were recorded under.
//
// Teardown needs no code: `docker compose down` removes the containers, and the
// qdisc lives only inside the removed network namespace.

// netemCmd records one emulation command and its observed result.
type netemCmd struct {
	Step      string `json:"step"`
	Container string `json:"container,omitempty"`
	Command   string `json:"command"`
	Output    string `json:"output,omitempty"`
	Error     string `json:"error,omitempty"`
}

// netemRecord is written to the run directory as network.json. It makes the
// network condition of a run evidence rather than an assumption: the exact
// commands, their output, and the kernel's own view of the installed qdisc are
// stored with the measurement.
type netemRecord struct {
	Applied      bool       `json:"applied"`
	Method       string     `json:"method"`
	DelayMS      int        `json:"delay_ms"`
	JitterPct    int        `json:"jitter_pct"`
	Interface    string     `json:"interface"`
	Filter       string     `json:"filter"`
	PeerIPs      []string   `json:"peer_ips,omitempty"`
	Commands     []netemCmd `json:"commands"`
	Verification []string   `json:"verification,omitempty"`
	Error        string     `json:"error,omitempty"`
}

// dockerExec runs a command inside a container and returns its combined output.
func dockerExec(container string, args ...string) (string, error) {
	full := append([]string{"exec", container}, args...)
	cmd := exec.Command("docker", full...)
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if err != nil {
		return text, fmt.Errorf("docker %s: %w: %s", strings.Join(full, " "), err, text)
	}
	return text, nil
}

// containerIPv4 returns the container's IPv4 address on its compose network.
func containerIPv4(name string) (string, error) {
	cmd := exec.Command("docker", "inspect", "-f",
		"{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("inspecting %s: %w", name, err)
	}
	ip := strings.TrimSpace(string(out))
	if ip == "" {
		return "", fmt.Errorf("%s has no IPv4 address yet", name)
	}
	return ip, nil
}

// applyNetworkDelay installs the configured emulation on every replica and
// verifies that the kernel accepted it. It returns nil when the run has no
// emulation and is not part of the network-delay family.
//
// A failure to install or verify the emulation is returned as an error so the
// run fails loudly: a run whose delay silently did not apply would otherwise be
// recorded as a delay measurement.
func applyNetworkDelay(dir string, cfg labcfg.Run, runID string, events *eventLog) (*netemRecord, error) {
	if !cfg.NetworkEmulated() && cfg.Experiment != "networkdelay" {
		return nil, nil
	}
	rec := &netemRecord{
		Method:    cfg.EmulationMethod(),
		DelayMS:   cfg.NetworkDelayMS,
		Interface: cfg.EmulationIface(),
		Filter:    cfg.EmulationFilter(),
	}
	write := func() {
		data, _ := json.MarshalIndent(rec, "", "  ")
		os.WriteFile(filepath.Join(dir, "network.json"), data, 0o644)
	}
	if !cfg.NetworkEmulated() {
		// Explicit baseline point of the family: state that nothing was
		// applied rather than leaving the field absent.
		write()
		return rec, nil
	}
	if rec.Method != labcfg.NetEmuTCNetem {
		return rec, fmt.Errorf("unsupported network emulation method %q", rec.Method)
	}

	iface := rec.Interface
	names := make([]string, cfg.Replicas)
	for i := range names {
		names[i] = containerName(runID, fmt.Sprintf("replica%d", i))
	}
	ips := make([]string, len(names))
	for i, n := range names {
		ip, err := containerIPv4(n)
		if err != nil {
			rec.Error = err.Error()
			write()
			return rec, err
		}
		ips[i] = ip
	}
	rec.PeerIPs = ips

	// run executes one emulation command. fatal distinguishes commands that must
	// succeed (installation and verification) from the best-effort reset of a
	// qdisc that may not exist. A best-effort step still records its command and
	// output, but a non-zero exit is not treated as an error: on a fresh
	// namespace there is no root qdisc yet, and `tc qdisc del` reports exactly
	// that. Anything else would clutter the evidence with a failure that is not
	// one.
	run := func(step, container string, fatal bool, args ...string) (string, error) {
		out, err := dockerExec(container, args...)
		entry := netemCmd{Step: step, Container: container, Command: strings.Join(args, " "), Output: out}
		if err != nil {
			if !fatal {
				entry.Step = step + " (best-effort)"
				rec.Commands = append(rec.Commands, entry)
				return out, nil
			}
			entry.Error = err.Error()
			rec.Commands = append(rec.Commands, entry)
			rec.Error = err.Error()
			return out, err
		}
		rec.Commands = append(rec.Commands, entry)
		return out, nil
	}

	for i, name := range names {
		if _, err := run("reset", name, false, "tc", "qdisc", "del", "dev", iface, "root"); err != nil {
			return rec, err
		}
		if rec.Filter == labcfg.NetFilterAll {
			if _, err := run("install-root", name, true,
				"tc", "qdisc", "add", "dev", iface, "root", "netem",
				"delay", strconv.Itoa(rec.DelayMS)+"ms"); err != nil {
				return rec, err
			}
		} else {
			// prio gives the delay its own band; only packets classified into
			// that band are delayed, so the default band (and therefore client
			// traffic) is unaffected.
			if _, err := run("install-prio", name, true,
				"tc", "qdisc", "add", "dev", iface, "root", "handle", "1:", "prio"); err != nil {
				return rec, err
			}
			if _, err := run("install-netem", name, true,
				"tc", "qdisc", "add", "dev", iface, "parent", "1:3", "handle", "30:",
				"netem", "delay", strconv.Itoa(rec.DelayMS)+"ms"); err != nil {
				return rec, err
			}
			for _, peer := range ips {
				if peer == ips[i] {
					continue // a replica is not its own peer
				}
				if _, err := run("install-filter", name, true,
					"tc", "filter", "add", "dev", iface, "parent", "1:",
					"protocol", "ip", "prio", "1", "u32",
					"match", "ip", "dst", peer+"/32", "flowid", "1:3"); err != nil {
					return rec, err
				}
			}
		}
		// Read the kernel's own view back. A qdisc that failed to attach would
		// otherwise be reported as a measured delay condition.
		out, err := dockerExec(name, "tc", "-s", "qdisc", "show", "dev", iface)
		rec.Verification = append(rec.Verification, fmt.Sprintf("%s: %s", name, out))
		if err != nil {
			rec.Error = err.Error()
			return rec, err
		}
		want := "delay " + strconv.Itoa(rec.DelayMS) + "ms"
		if !strings.Contains(out, "netem") || !strings.Contains(out, want) {
			err := fmt.Errorf("%s: expected a netem qdisc with %q, kernel reports: %s", name, want, out)
			rec.Error = err.Error()
			return rec, err
		}
	}

	rec.Applied = true
	write()
	if events != nil {
		events.log(runID, "network_emulation", fmt.Sprintf(
			"applied %s %dms on %s (filter=%s, replicas=%d)", rec.Method, rec.DelayMS, iface, rec.Filter, len(names)))
	}
	return rec, nil
}
