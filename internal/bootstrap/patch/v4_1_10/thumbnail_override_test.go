package v4_1_10

import (
	"fmt"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestEnableThumbnailExpirationOverride(t *testing.T) {
	prevConf := conf.Conf
	prevVersion := conf.Version
	conf.Conf = conf.DefaultConfig("data")
	conf.Version = "v4.1.10"
	t.Cleanup(func() {
		conf.Conf = prevConf
		conf.Version = prevVersion
	})

	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	db.Init(database)

	storage := model.Storage{
		Driver:                      "Local",
		MountPath:                   "/local",
		Addition:                    `{"root_folder_path":"."}`,
		ThumbnailExpirationOverride: false,
	}
	if err := db.CreateStorage(&storage); err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	EnableThumbnailExpirationOverride()

	updated, err := db.GetStorageById(storage.ID)
	if err != nil {
		t.Fatalf("failed to reload storage: %v", err)
	}
	if !updated.ThumbnailExpirationOverride {
		t.Fatal("expected thumbnail_expiration_override to be enabled by patch")
	}
}
