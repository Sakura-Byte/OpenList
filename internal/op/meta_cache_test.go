package op_test

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/stretchr/testify/require"
)

func TestCaseInsensitiveMetaCacheAfterGridChanges(t *testing.T) {
	setupStorageTestEnv(t)
	meta := &model.Meta{Path: "/MetaCacheOriginal", Password: "before", PSub: true}
	require.NoError(t, op.CreateMeta(meta))
	got, err := op.GetNearestMeta("/metacacheoriginal/child")
	require.NoError(t, err)
	require.Equal(t, "before", got.Password)

	require.NoError(t, op.UpdateMetasGrid([]model.MetaGridUpdate{{ID: meta.ID, Path: "/MetaCacheRenamed", Password: "after", PSub: true}}))
	_, err = op.GetNearestMeta("/metacacheoriginal/child")
	require.ErrorIs(t, err, errs.MetaNotFound)
	got, err = op.GetNearestMeta("/metacacherenamed/child")
	require.NoError(t, err)
	require.Equal(t, "after", got.Password)

	got.Password = "updated"
	require.NoError(t, op.UpdateMeta(got))
	got, err = op.GetNearestMeta("/metacacherenamed/child")
	require.NoError(t, err)
	require.Equal(t, "updated", got.Password)
	require.NoError(t, op.DeleteMetaById(meta.ID))
	_, err = op.GetNearestMeta("/metacacherenamed/child")
	require.ErrorIs(t, err, errs.MetaNotFound)
}
