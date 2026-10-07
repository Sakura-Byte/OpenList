package meilisearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/meilisearch/meilisearch-go"
	"github.com/stretchr/testify/require"
)

func TestCachedTreeReplacementWaitsAndDeletesRootWithoutLookup(t *testing.T) {
	for _, status := range []string{"succeeded", "failed"} {
		t.Run(status, func(t *testing.T) {
			var mu sync.Mutex
			var requests []string
			id := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				requests = append(requests, r.Method+" "+r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				if strings.HasPrefix(r.URL.Path, "/tasks/") {
					json.NewEncoder(w).Encode(map[string]any{"uid": id, "status": status})
					return
				}
				id++
				w.WriteHeader(http.StatusAccepted)
				json.NewEncoder(w).Encode(map[string]any{"taskUid": id, "indexUid": "test", "status": "enqueued", "type": "documentAdditionOrUpdate"})
			}))
			defer server.Close()
			m := &Meilisearch{Client: meilisearch.New(server.URL), IndexUid: "test"}
			err := m.ReplaceCachedTree(context.Background(), "/ASMR/RJ1 Old", []model.SearchNode{{Parent: "/ASMR", Name: "RJ1 New", IsDir: true}})
			if status == "failed" {
				require.ErrorContains(t, err, "failed")
				return
			}
			require.NoError(t, err)
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, requests, 6)
			require.Equal(t, "DELETE /indexes/test/documents/"+hashPath("/ASMR/RJ1 Old"), requests[2])
			require.Equal(t, "POST /indexes/test/documents", requests[4])
			for _, request := range requests {
				require.False(t, strings.HasPrefix(request, "GET /indexes/"))
			}
		})
	}
}
