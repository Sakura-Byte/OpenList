package smb

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestIdleConnectionIsClosedBeforeReplacement(t *testing.T) {
	var connections []*countingConn
	d := &SMB{
		newConnection: func(context.Context) (*smbConnection, error) {
			client, server := net.Pipe()
			t.Cleanup(func() { _ = server.Close() })
			conn := &countingConn{Conn: client}
			connections = append(connections, conn)
			return &smbConnection{transport: conn}, nil
		},
	}

	if err := d.Init(context.Background()); err != nil {
		t.Fatalf("init SMB driver: %v", err)
	}
	d.lastConnTime.Store(time.Now().Add(-6 * time.Minute).Unix())

	_, release, err := d.acquireFS(context.Background())
	if err != nil {
		t.Fatalf("acquire SMB connection: %v", err)
	}
	release()

	if got := len(connections); got != 2 {
		t.Fatalf("expected replacement connection, got %d connections", got)
	}
	if got := connections[0].closeCount.Load(); got != 1 {
		t.Fatalf("expected old connection to be closed once, got %d", got)
	}
	if got := connections[1].closeCount.Load(); got != 0 {
		t.Fatalf("expected replacement connection to remain open, got %d closes", got)
	}

	if err := d.Drop(context.Background()); err != nil {
		t.Fatalf("drop SMB driver: %v", err)
	}
	if got := connections[1].closeCount.Load(); got != 1 {
		t.Fatalf("expected replacement connection to close on drop, got %d", got)
	}
}

func TestActiveLeasesPreventConnectionClose(t *testing.T) {
	var connections []*countingConn
	d := &SMB{
		newConnection: func(context.Context) (*smbConnection, error) {
			client, server := net.Pipe()
			t.Cleanup(func() { _ = server.Close() })
			conn := &countingConn{Conn: client}
			connections = append(connections, conn)
			return &smbConnection{transport: conn}, nil
		},
	}

	if err := d.Init(context.Background()); err != nil {
		t.Fatalf("init SMB driver: %v", err)
	}
	_, releaseFirst, err := d.acquireFS(context.Background())
	if err != nil {
		t.Fatalf("acquire first SMB lease: %v", err)
	}

	d.lastConnTime.Store(time.Now().Add(-6 * time.Minute).Unix())
	_, releaseSecond, err := d.acquireFS(context.Background())
	if err != nil {
		t.Fatalf("acquire second SMB lease: %v", err)
	}
	if got := len(connections); got != 1 {
		t.Fatalf("expected active lease to prevent reconnect, got %d connections", got)
	}

	if err := d.Drop(context.Background()); err != nil {
		t.Fatalf("drop SMB driver: %v", err)
	}
	if got := connections[0].closeCount.Load(); got != 0 {
		t.Fatalf("expected active connection to remain open, got %d closes", got)
	}

	releaseFirst()
	if got := connections[0].closeCount.Load(); got != 0 {
		t.Fatalf("expected final lease to keep connection open, got %d closes", got)
	}
	releaseSecond()
	if got := connections[0].closeCount.Load(); got != 1 {
		t.Fatalf("expected final lease release to close connection, got %d", got)
	}
}

type countingConn struct {
	net.Conn
	closeCount atomic.Int32
}

func (c *countingConn) Close() error {
	c.closeCount.Add(1)
	return c.Conn.Close()
}
