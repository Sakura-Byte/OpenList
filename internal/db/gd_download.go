package db

import (
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// IncGDDownloadCount adds one download to the hour bucket of an IP.
func IncGDDownloadCount(ipKey string, hourBucket int64) error {
	row := model.GDDownloadCount{IPKey: ipKey, HourBucket: hourBucket, Count: 1}
	return errors.WithStack(db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "ip_key"}, {Name: "hour_bucket"}},
		DoUpdates: clause.Assignments(map[string]any{"count": gorm.Expr("count + 1")}),
	}).Create(&row).Error)
}

// ListGDDownloadCounts returns the buckets of an IP from sinceHour on, oldest first.
func ListGDDownloadCounts(ipKey string, sinceHour int64) ([]model.GDDownloadCount, error) {
	var rows []model.GDDownloadCount
	err := db.Where("ip_key = ? AND hour_bucket >= ?", ipKey, sinceHour).Order("hour_bucket asc").Find(&rows).Error
	return rows, errors.WithStack(err)
}

// DeleteGDDownloadCountsBefore removes buckets older than beforeHour.
func DeleteGDDownloadCountsBefore(beforeHour int64) error {
	return errors.WithStack(db.Where("hour_bucket < ?", beforeHour).Delete(&model.GDDownloadCount{}).Error)
}

// IncGDDownloadStat adds one download to an author's daily row.
func IncGDDownloadStat(area, author, day string, bytes int64) error {
	row := model.GDDownloadStat{Area: area, Author: author, Day: day, Count: 1, Bytes: bytes}
	return errors.WithStack(db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "area"}, {Name: "author"}, {Name: "day"}},
		DoUpdates: clause.Assignments(map[string]any{
			"count": gorm.Expr("count + 1"),
			"bytes": gorm.Expr("bytes + ?", bytes),
		}),
	}).Create(&row).Error)
}

// GDDownloadAuthor is one row of the most-downloaded authors report.
type GDDownloadAuthor struct {
	Author string `json:"author"`
	Count  int64  `json:"count"`
	Bytes  int64  `json:"bytes"`
}

// TopGDDownloadAuthors sums an area's daily rows from sinceDay on, most downloads first.
func TopGDDownloadAuthors(area, sinceDay string, limit int) ([]GDDownloadAuthor, error) {
	var rows []GDDownloadAuthor
	err := db.Model(&model.GDDownloadStat{}).
		Select("author, SUM(count) AS count, SUM(bytes) AS bytes").
		Where("area = ? AND day >= ?", area, sinceDay).
		Group("author").
		Order("count desc").
		Limit(limit).
		Scan(&rows).Error
	return rows, errors.WithStack(err)
}
