package db

import (
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

func GetMetaByPath(path string) (*model.Meta, error) {
	meta := model.Meta{Path: path}
	if err := db.Where(meta).First(&meta).Error; err != nil {
		return nil, errors.Wrapf(err, "failed select meta")
	}
	return &meta, nil
}

func GetMetaById(id uint) (*model.Meta, error) {
	var u model.Meta
	if err := db.First(&u, id).Error; err != nil {
		return nil, errors.Wrapf(err, "failed get old meta")
	}
	return &u, nil
}

func CreateMeta(u *model.Meta) error {
	return errors.WithStack(db.Create(u).Error)
}

func UpdateMeta(u *model.Meta) error {
	return errors.WithStack(db.Save(u).Error)
}

// UpdateMetasGrid updates existing records atomically and returns both old and
// new paths for cache invalidation after the transaction commits.
func UpdateMetasGrid(metas []model.MetaGridUpdate) ([]string, error) {
	paths := make([]string, 0, len(metas)*2)
	err := db.Transaction(func(tx *gorm.DB) error {
		for _, meta := range metas {
			var old model.Meta
			if err := tx.First(&old, meta.ID).Error; err != nil {
				return errors.Wrapf(err, "failed get meta %d", meta.ID)
			}
			paths = append(paths, old.Path, meta.Path)
			values := map[string]interface{}{
				"path": meta.Path, "password": meta.Password, "p_sub": meta.PSub,
				"write": meta.Write, "w_sub": meta.WSub, "hide": meta.Hide,
				"h_sub": meta.HSub, "readme": meta.Readme, "r_sub": meta.RSub,
				"header": meta.Header, "header_sub": meta.HeaderSub,
			}
			if err := tx.Model(&old).Updates(values).Error; err != nil {
				return errors.Wrapf(err, "failed update meta %d", meta.ID)
			}
		}
		return nil
	})
	return paths, errors.WithStack(err)
}

func GetMetas(pageIndex, pageSize int) (metas []model.Meta, count int64, err error) {
	metaDB := db.Model(&model.Meta{})
	if err = metaDB.Count(&count).Error; err != nil {
		return nil, 0, errors.Wrapf(err, "failed get metas count")
	}
	if err = metaDB.Order(columnName("id")).Offset((pageIndex - 1) * pageSize).Limit(pageSize).Find(&metas).Error; err != nil {
		return nil, 0, errors.Wrapf(err, "failed get find metas")
	}
	return metas, count, nil
}

func DeleteMetaById(id uint) error {
	return errors.WithStack(db.Delete(&model.Meta{}, id).Error)
}
