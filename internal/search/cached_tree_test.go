package search

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCachedTreeUpdateDoesNotBlockBehindIndexBuild(t *testing.T) {
	cachedTreeIndexMu.Lock()
	defer cachedTreeIndexMu.Unlock()
	done := make(chan struct{})
	go func() {
		ok, err := updateCachedTree(context.Background(), nil, "/old", "/new")
		require.False(t, ok)
		require.NoError(t, err)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cached update blocked behind index build")
	}
}
