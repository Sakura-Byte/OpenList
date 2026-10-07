package db

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestDeleteSearchSubtreeIncludesRootAndTreatsWildcardsLiterally(t *testing.T) {
	previousConfig := conf.Conf
	conf.Conf = conf.DefaultConfig("data")
	t.Cleanup(func() { conf.Conf = previousConfig })
	database, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	previous := db
	db = database
	t.Cleanup(func() { db = previous; conn, _ := database.DB(); conn.Close() })
	require.NoError(t, database.AutoMigrate(&model.SearchNode{}))
	nodes := []model.SearchNode{{Parent: "/ASMR", Name: "RJ1 A_%", IsDir: true}, {Parent: "/ASMR/RJ1 A_%", Name: "track.mp3"}, {Parent: "/ASMR/RJ1 A_%/disc", Name: "track2.mp3"}, {Parent: "/ASMR", Name: "RJ1 A_XX", IsDir: true}, {Parent: "/ASMR/RJ1 A_XX/disc", Name: "keep.mp3"}}
	require.NoError(t, BatchCreateSearchNodes(&nodes))
	require.NoError(t, DeleteSearchNodesByParent("/ASMR/RJ1 A_%"))
	var left []model.SearchNode
	require.NoError(t, database.Find(&left).Error)
	require.Len(t, left, 2)
	require.Equal(t, "RJ1 A_XX", left[0].Name)
	require.Equal(t, "keep.mp3", left[1].Name)
	require.NoError(t, DeleteSearchNodesByParent("/ASMR/RJ1 A_%"))
	left = nil
	require.NoError(t, database.Find(&left).Error)
	require.Len(t, left, 2)
}
