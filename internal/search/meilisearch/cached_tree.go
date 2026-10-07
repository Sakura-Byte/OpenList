package meilisearch

import (
	"context"
	"fmt"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/meilisearch/meilisearch-go"
)

// Unlike browsing snapshots, durable tree replacements must be acknowledged
// only after their asynchronous tasks succeed, and cannot depend on a document
// lookup while an earlier addition is still pending in Meilisearch.
func (m *Meilisearch) ReplaceCachedTree(ctx context.Context, oldPath string, nodes []model.SearchNode) error {
	wait := func(id int64) error {
		task, err := m.Client.WaitForTaskWithContext(ctx, id, 50*time.Millisecond)
		if err != nil {
			return err
		}
		if task.Status != meilisearch.TaskStatusSucceeded {
			return fmt.Errorf("cached tree index task %d: %s", id, task.Status)
		}
		return nil
	}
	if oldPath != "" {
		children, err := m.Client.Index(m.IndexUid).DeleteDocumentsByFilterWithContext(ctx, fmt.Sprintf("parent_path_hashes = '%s'", hashPath(oldPath)))
		if err != nil {
			return err
		}
		if err := wait(children.TaskUID); err != nil {
			return err
		}
		root, err := m.Client.Index(m.IndexUid).DeleteDocumentWithContext(ctx, hashPath(oldPath))
		if err != nil {
			return err
		}
		if err := wait(root.TaskUID); err != nil {
			return err
		}
	}
	ids, err := m.batchIndexWithTaskUID(ctx, nodes)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := wait(id); err != nil {
			return err
		}
	}
	return nil
}
