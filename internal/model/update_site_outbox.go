package model

import "time"

type UpdateSiteOutbox struct {
	ID          string     `json:"id" gorm:"primaryKey"`
	EventID     string     `json:"event_id" gorm:"uniqueIndex;size:64;not null"`
	EventType   string     `json:"event_type" gorm:"size:64;index;not null"`
	Payload     string     `json:"payload" gorm:"type:text;not null"`
	Status      string     `json:"status" gorm:"size:24;index;not null"`
	AttemptCount int       `json:"attempt_count" gorm:"not null"`
	NextAttemptAt time.Time `json:"next_attempt_at" gorm:"index;not null"`
	LastError   string     `json:"last_error" gorm:"type:text"`
	SentAt      *time.Time `json:"sent_at"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}
