package db

import (
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

func CreateUpdateSiteOutbox(item *model.UpdateSiteOutbox) error {
	return errors.WithStack(db.Create(item).Error)
}

func ClaimUpdateSiteOutbox(now time.Time) (*model.UpdateSiteOutbox, error) {
	var claimed *model.UpdateSiteOutbox
	err := db.Transaction(func(tx *gorm.DB) error {
		var item model.UpdateSiteOutbox
		err := tx.
			Where("status in ? AND next_attempt_at <= ?", []string{"queued", "retry"}, now).
			Order("next_attempt_at asc").
			Order("created_at asc").
			First(&item).Error
		if err != nil {
			return err
		}
		if err := tx.Model(&model.UpdateSiteOutbox{}).
			Where("id = ? AND status in ?", item.ID, []string{"queued", "retry"}).
			Updates(map[string]any{
				"status":       "sending",
				"updated_at":   now,
				"attempt_count": item.AttemptCount + 1,
			}).Error; err != nil {
			return err
		}
		item.Status = "sending"
		item.AttemptCount++
		claimed = &item
		return nil
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return claimed, errors.WithStack(err)
}

func MarkUpdateSiteOutboxSent(id string, sentAt time.Time) error {
	return errors.WithStack(db.Model(&model.UpdateSiteOutbox{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":         "sent",
			"sent_at":        sentAt,
			"last_error":     "",
			"next_attempt_at": sentAt,
			"updated_at":     sentAt,
		}).Error)
}

func MarkUpdateSiteOutboxRetry(id string, nextAttemptAt time.Time, errMessage string) error {
	return errors.WithStack(db.Model(&model.UpdateSiteOutbox{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":         "retry",
			"next_attempt_at": nextAttemptAt,
			"last_error":     errMessage,
			"updated_at":     time.Now(),
		}).Error)
}
