// This go.mod exists only to exclude this package from the parent module's
// ./... wildcard. The adapter is NOT built as a module: its dependency
// (github.com/nvanbenschoten/epaxos) predates Go modules and carries its own
// vendored dependency tree, including a vendored github.com/cockroachdb
// package that is not published as a resolvable module. The runner therefore
// builds this package in GOPATH mode (GO111MODULE=off) against the vendored
// source, exactly as it builds the original efficient/epaxos server.
//
// See lab/upstream/nvb-epaxos/UPSTREAM.md.
module conslab/nvbepaxos

go 1.26
