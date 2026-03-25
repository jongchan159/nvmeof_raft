//go:build raft_tcp
// +build raft_tcp

package main

import "net/rpc"

func dialLeader(addr string) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", addr)
}
