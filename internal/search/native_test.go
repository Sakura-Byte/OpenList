package search

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/stretchr/testify/require"
)

type localSearchStub struct {
	driver.Driver
	model.Storage
	nodes []model.SearchNode
}

func (s *localSearchStub) GetStorage() *model.Storage { return &s.Storage }
func (s *localSearchStub) SetStorage(v model.Storage) { s.Storage = v }
func (s *localSearchStub) SearchCached(ctx context.Context, req model.SearchReq, emit func(model.SearchNode) error) error {
	for _, n := range s.nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if driver.CachedSearchMatch(req, n) {
			if err := emit(n); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestNativeSearchWithoutIndexFiltersBeforePagination(t *testing.T) {
	old := instance
	instance = nil
	t.Cleanup(func() { instance = old })
	d := &localSearchStub{Storage: model.Storage{MountPath: "/Cache", Status: op.WORK}, nodes: []model.SearchNode{
		{Parent: "/", Name: "secret", IsDir: true}, {Parent: "/", Name: "first", IsDir: true}, {Parent: "/", Name: "second", IsDir: true},
	}}
	req := model.SearchReq{Parent: "/", PageReq: model.PageReq{Page: 2, PerPage: 1}}
	nodes, total, err := searchWithStorages(context.Background(), req, func(n model.SearchNode) bool { return n.Name != "secret" }, []driver.Driver{d})
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
	require.Equal(t, []model.SearchNode{{Parent: "/Cache", Name: "second", IsDir: true}}, nodes)
	req.Parent = "/Other"
	_, _, err = searchWithStorages(context.Background(), req, nil, []driver.Driver{d})
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req.Parent = "/"
	_, _, err = searchWithStorages(ctx, req, nil, []driver.Driver{d})
	require.ErrorIs(t, err, context.Canceled)
}

func TestNativeSearchFederatesAndMasksNestedMounts(t *testing.T) {
	old := instance
	t.Cleanup(func() { instance = old })
	instance = &filteredSearchStub{nodes: []model.SearchNode{
		{Parent: "/Other", Name: "indexed"}, {Parent: "/Cache", Name: "stale"},
	}}
	d := &localSearchStub{Storage: model.Storage{MountPath: "/Cache", Status: op.WORK}, nodes: []model.SearchNode{{Parent: "/", Name: "new"}, {Parent: "/nested", Name: "masked"}}}
	nested := &localSearchStub{Storage: model.Storage{MountPath: "/Cache/nested", Status: op.WORK}, nodes: []model.SearchNode{{Parent: "/", Name: "actual"}}}
	req := model.SearchReq{Parent: "/", PageReq: model.PageReq{Page: 1, PerPage: 10}}
	nodes, total, err := searchWithStorages(context.Background(), req, nil, []driver.Driver{nested, d})
	require.NoError(t, err)
	require.EqualValues(t, 3, total)
	require.Equal(t, []model.SearchNode{{Parent: "/Other", Name: "indexed"}, {Parent: "/Cache", Name: "new"}, {Parent: "/Cache/nested", Name: "actual"}}, nodes)
	prior := conf.SlicesMap[conf.IgnorePaths]
	conf.SlicesMap[conf.IgnorePaths] = []string{"/Cache/nested"}
	t.Cleanup(func() { conf.SlicesMap[conf.IgnorePaths] = prior })
	nodes, total, err = searchWithStorages(context.Background(), req, nil, []driver.Driver{d, nested})
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
	require.Len(t, nodes, 2)
}
