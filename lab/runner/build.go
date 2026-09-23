package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"conslab/internal/labcfg"
)

// repoRoot returns the lab directory (this repository's lab/ folder).
func labRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	// Walk up until we find go.mod with "module conslab".
	dir := wd
	for {
		if data, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil {
			if strings.Contains(string(data), "module conslab") {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return wd
}

// resultsBase names the results subtree the runner writes to, relative to
// results/. Empty means the primary dataset (results/). The
// implementation-sensitivity experiment sets this so its raw runs, run index,
// and execution manifest are stored under results/sensitivity/ and the primary
// dataset is never read or written.
var resultsBase string

func resultsDir() string {
	if resultsBase != "" {
		return filepath.Join(labRoot(), "results", resultsBase)
	}
	return filepath.Join(labRoot(), "results")
}

// imageTag is the lab image used by every container in every run.
const imageTag = "conslab:lab"

// cmdBuild builds all Go binaries and the lab Docker image.
func cmdBuild() error {
	root := labRoot()
	fmt.Println("building Go binaries ...")
	if err := goBuild(root,
		"bin/raftadapter", "./adapters/raft",
		"bin/etcdraftadapter", "./adapters/etcdraft",
		"bin/raftmaster", "./master/raft",
		"bin/client", "./client",
		"bin/monitor", "./monitor/cmd/monitor",
		"bin/runner", "./runner",
	); err != nil {
		return err
	}
	fmt.Println("building upstream EPaxos binaries (GOPATH mode) ...")
	if err := buildEPaxos(root); err != nil {
		return err
	}
	fmt.Println("building nvb/epaxos adapter (GOPATH mode) ...")
	if err := buildNVBEPaxos(root); err != nil {
		return err
	}
	fmt.Println("building docker image ...")
	if err := run(root, "docker", "build", "-t", imageTag, "-f", "docker/Dockerfile.lab", "."); err != nil {
		return err
	}
	fmt.Println("build complete")
	return nil
}

func goBuild(root string, pairs ...string) error {
	for i := 0; i < len(pairs); i += 2 {
		out, pkg := pairs[i], pairs[i+1]
		if err := run(root, "go", "build", "-o", out, pkg); err != nil {
			return err
		}
	}
	return nil
}

// buildEPaxos builds the upstream EPaxos master and server. The upstream tree
// is a GOPATH-style project, so it is built in a temporary GOPATH that
// symlinks the vendored src/ directory. The upstream code is not modified.
func buildEPaxos(root string) error {
	tmp, err := os.MkdirTemp("", "epaxos-gopath")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	src := filepath.Join(root, "upstream", "epaxos", "src")
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	tmpSrc := filepath.Join(tmp, "src")
	if err := os.MkdirAll(tmpSrc, 0o755); err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.Symlink(filepath.Join(src, e.Name()), filepath.Join(tmpSrc, e.Name())); err != nil {
			return err
		}
	}
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		return err
	}
	env := append(os.Environ(), "GO111MODULE=off", "GOPATH="+tmp, "GOFLAGS=", "GOBIN=")
	for _, pkg := range []string{"master", "server"} {
		out := filepath.Join(bin, "epaxos-"+pkg)
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Dir = tmpSrc
		cmd.Env = env
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("building epaxos %s: %w", pkg, err)
		}
	}
	return nil
}

// buildNVBEPaxos builds the adapter around github.com/nvanbenschoten/epaxos.
//
// That library predates Go modules and ships its dependencies through dep
// (Gopkg.lock), so it is built in GOPATH mode against its own vendored tree,
// exactly like the original efficient/epaxos server. The lab's wire protocol
// and state packages are exposed to the GOPATH build through symlinks, so the
// adapter imports exactly the same protocol code as every other adapter.
func buildNVBEPaxos(root string) error {
	src := filepath.Join(root, "upstream", "nvb-epaxos")
	if _, err := os.Stat(filepath.Join(src, "vendor")); err != nil {
		return fmt.Errorf("nvb/epaxos vendored dependencies missing at %s/vendor: "+
			"restore them with `git clone https://github.com/nvb/epaxos && "+
			"git -C epaxos checkout 425bd36502ecc810ef7f08943896f957673102bc && "+
			"cp -r epaxos/vendor %s/vendor` (see upstream/nvb-epaxos/UPSTREAM.md)", src, src)
	}

	tmp, err := os.MkdirTemp("", "nvb-gopath")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	tmpSrc := filepath.Join(tmp, "src")

	links := []struct{ link, target string }{
		{"github.com/nvanbenschoten/epaxos", src},
		{"conslab/internal/proto", filepath.Join(root, "internal", "proto")},
		{"conslab/internal/state", filepath.Join(root, "internal", "state")},
		{"conslab/adapters/nvbepaxos", filepath.Join(root, "adapters", "nvbepaxos")},
		// internal/proto imports the state package by its module-relative
		// name, which in GOPATH mode resolves to $GOPATH/src/state.
		{"state", filepath.Join(root, "internal", "state")},
	}
	for _, l := range links {
		dst := filepath.Join(tmpSrc, l.link)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.Symlink(l.target, dst); err != nil {
			return err
		}
	}

	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		return err
	}
	out := filepath.Join(bin, "nvbepaxos-server")
	env := append(os.Environ(), "GO111MODULE=off", "GOPATH="+tmp, "GOFLAGS=", "GOBIN=")
	cmd := exec.Command("go", "build", "-o", out, "conslab/adapters/nvbepaxos")
	cmd.Dir = tmpSrc
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("building nvb/epaxos adapter: %w", err)
	}
	return nil
}

func run(dir, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

// runQuiet runs a command with its output captured rather than streamed.
// Compose emits hundreds of lines of create/start/remove chatter per run,
// which would swamp the runner log across a 273-run matrix.
func runQuiet(dir, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, out)
	}
	return nil
}

// hostInfo records the host environment for provenance.
func hostInfo() map[string]any {
	return map[string]any{
		"os":   runtime.GOOS,
		"arch": runtime.GOARCH,
		"cpus": runtime.NumCPU(),
	}
}

// versionInfo records exact versions of every important component.
func versionInfo() map[string]any {
	return map[string]any{
		"go":             cmdVersion("go", "version"),
		"docker":         cmdVersion("docker", "version", "--format", "{{.Server.Version}}"),
		"docker_compose": cmdVersion("docker", "compose", "version", "--short"),
		"raft":           "github.com/hashicorp/raft v1.7.3",
		"raft_boltdb":    "github.com/hashicorp/raft-boltdb/v2 v2.3.0",
		"epaxos":         "github.com/efficient/epaxos 791b115669fca472d3136f6a2eda46c00b3f8251",
		"raft_etcd":      "go.etcd.io/raft/v3 v3.7.0",
		"epaxos_nvb":     "github.com/nvanbenschoten/epaxos (github.com/nvb/epaxos) 425bd36502ecc810ef7f08943896f957673102bc",
		"lab_image":      cmdVersion("docker", "inspect", "-f", "{{index .RepoDigests 0}}", imageTag),
		"repo_commit":    cmdVersion("git", "rev-parse", "HEAD"),
		"repo_dirty":     cmdVersion("git", "status", "--porcelain"),
	}
}

func cmdVersion(name string, args ...string) string {
	cmd := exec.Command(name, args...)
	cmd.Dir = labRoot()
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// runIDFor builds a stable run identifier from a configuration. The
// identifier encodes every independent variable needed to distinguish two
// runs of the same experiment, so different configurations never collide.
func runIDFor(cfg labcfg.Run, rep int) string {
	experiment := experimentName(cfg)
	// Primary implementations keep their historical run IDs, so the recorded
	// dataset is unchanged. An independent implementation adds its name to the
	// protocol token, so experiment-sensitivity runs can never collide with the
	// primary runs of the same configuration.
	proto := cfg.Protocol
	if !cfg.IsPrimary() {
		proto = cfg.Protocol + "-" + cfg.Impl()
	}
	switch experiment {
	case "failure":
		if cfg.Failure.Mode == labcfg.FailureElection {
			return fmt.Sprintf("%s-%s-%s-r%d-e%d-%d", experiment, cfg.Failure.Mode, proto, cfg.Replicas, cfg.Failure.FailedElections, rep)
		}
		return fmt.Sprintf("%s-%s-%s-r%d-%d", experiment, cfg.Failure.Mode, proto, cfg.Replicas, rep)
	case "election":
		// The election experiment's independent variable (target number of
		// failed elections) is part of the run ID, so the target is never
		// conflated with the repetition.
		return fmt.Sprintf("%s-%s-r%d-e%d-%d", experiment, proto, cfg.Replicas, cfg.Failure.FailedElections, rep)
	case "conflict":
		return fmt.Sprintf("%s-%s-r%d-x%d-w%d-c%d-%d", experiment, proto, cfg.Replicas, cfg.ConflictPct, cfg.WritePct, cfg.Concurrency, rep)
	case "concurrency":
		return fmt.Sprintf("%s-%s-r%d-w%d-c%d-%d", experiment, proto, cfg.Replicas, cfg.WritePct, cfg.Concurrency, rep)
	case "pernode":
		return fmt.Sprintf("%s-%s-r%d-w%d-c%d-%d", experiment, proto, cfg.Replicas, cfg.WritePct, cfg.Concurrency, rep)
	case "commcost":
		// The communication cost and jitter are part of the run ID so the
		// cost levels never collide.
		return fmt.Sprintf("%s-%s-r%d-w%d-c%d-x%d-j%d-%d", experiment, proto, cfg.Replicas, cfg.WritePct, cfg.Concurrency, cfg.CommCostMS, cfg.CommJitterPct, rep)
	default:
		return fmt.Sprintf("%s-%s-r%d-w%d-c%d-%d", experiment, proto, cfg.Replicas, cfg.WritePct, cfg.Concurrency, rep)
	}
}

// experimentName returns the experiment family of a configuration. An
// explicit Experiment field wins; otherwise it is derived.
func experimentName(cfg labcfg.Run) string {
	if cfg.Experiment != "" {
		return cfg.Experiment
	}
	if cfg.Failure.Mode != labcfg.FailureNone {
		return "failure"
	}
	if cfg.ConflictPct > 0 {
		return "conflict"
	}
	if cfg.Replicas != 3 {
		return "scaling"
	}
	return "workload"
}
