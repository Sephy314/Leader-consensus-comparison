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

func resultsDir() string { return filepath.Join(labRoot(), "results") }

// imageTag is the lab image used by every container in every run.
const imageTag = "conslab:lab"

// cmdBuild builds all Go binaries and the lab Docker image.
func cmdBuild() error {
	root := labRoot()
	fmt.Println("building Go binaries ...")
	if err := goBuild(root,
		"bin/raftadapter", "./adapters/raft",
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
	switch experiment {
	case "failure":
		if cfg.Failure.Mode == labcfg.FailureElection {
			return fmt.Sprintf("%s-%s-%s-r%d-e%d-%d", experiment, cfg.Failure.Mode, cfg.Protocol, cfg.Replicas, cfg.Failure.FailedElections, rep)
		}
		return fmt.Sprintf("%s-%s-%s-r%d-%d", experiment, cfg.Failure.Mode, cfg.Protocol, cfg.Replicas, rep)
	case "election":
		// The election experiment's independent variable (target number of
		// failed elections) is part of the run ID, so the target is never
		// conflated with the repetition.
		return fmt.Sprintf("%s-%s-r%d-e%d-%d", experiment, cfg.Protocol, cfg.Replicas, cfg.Failure.FailedElections, rep)
	case "conflict":
		return fmt.Sprintf("%s-%s-r%d-x%d-w%d-c%d-%d", experiment, cfg.Protocol, cfg.Replicas, cfg.ConflictPct, cfg.WritePct, cfg.Concurrency, rep)
	case "concurrency":
		return fmt.Sprintf("%s-%s-r%d-w%d-c%d-%d", experiment, cfg.Protocol, cfg.Replicas, cfg.WritePct, cfg.Concurrency, rep)
	case "pernode":
		return fmt.Sprintf("%s-%s-r%d-w%d-c%d-%d", experiment, cfg.Protocol, cfg.Replicas, cfg.WritePct, cfg.Concurrency, rep)
	default:
		return fmt.Sprintf("%s-%s-r%d-w%d-c%d-%d", experiment, cfg.Protocol, cfg.Replicas, cfg.WritePct, cfg.Concurrency, rep)
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
