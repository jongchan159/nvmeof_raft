//go:build raft_tcp
// +build raft_tcp

package main

import "net/rpc"

// dialLeader opens a TCP connection to addr using the standard net/rpc HTTP transport.
func dialLeader(addr string) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", addr)
}
