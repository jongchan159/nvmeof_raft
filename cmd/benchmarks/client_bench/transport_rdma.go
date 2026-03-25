//go:build raft
// +build raft

package main

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"net/rpc"
	"time"

	"nvmeof_raft/rdmacm"
)

// dialLeader opens an RDMA connection to addr and performs the net/rpc HTTP
// handshake, returning a ready *rpc.Client.
func dialLeader(addr string) (*rpc.Client, error) {
	conn, err := rdmacm.Dial(addr)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n"); err != nil {
		conn.Close()
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err != nil {
		conn.Close()
		return nil, err
	}
	if resp.Status != "200 Connected to Go RPC" {
		conn.Close()
		return nil, errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.SetDeadline(time.Time{})
	return rpc.NewClient(conn), nil
}
