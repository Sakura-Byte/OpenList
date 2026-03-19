package updatesite

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var (
	queueOnce   sync.Once
	httpClient  *resty.Client
)

func Init() {
	queueOnce.Do(func() {
		httpClient = resty.New().
			SetRetryCount(2).
			SetRetryWaitTime(500 * time.Millisecond).
			SetTimeout(10 * time.Second)
		go worker()
	})
}

func worker() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		item, err := db.ClaimUpdateSiteOutbox(time.Now())
		if err != nil {
			log.Errorf("updatesite: claim outbox failed: %v", err)
			continue
		}
		if item == nil {
			continue
		}
		send(item)
	}
}

func enqueue(eventType string, eventID string, cfg DeliveryConfig, payload any) {
	if !cfg.Enabled {
		return
	}
	Init()
	body, err := utils.Json.Marshal(payload)
	if err != nil {
		log.Errorf("updatesite: marshal %s failed: %v", eventType, err)
		return
	}
	if err := db.CreateUpdateSiteOutbox(&model.UpdateSiteOutbox{
		ID:           uuid.NewString(),
		EventID:      eventID,
		EventType:    eventType,
		Payload:      string(body),
		Status:       "queued",
		AttemptCount: 0,
		NextAttemptAt: time.Now(),
	}); err != nil {
		log.Errorf("updatesite: persist %s failed: %v", eventType, err)
	}
}

func send(item *model.UpdateSiteOutbox) {
	cfg := currentConfig()
	if !cfg.Enabled {
		_ = db.MarkUpdateSiteOutboxRetry(item.ID, time.Now().Add(30*time.Second), "update-site delivery disabled")
		return
	}
	timestamp := time.Now().UTC().Format(time.RFC3339)
	body := []byte(item.Payload)
	mac := hmac.New(sha256.New, []byte(cfg.Secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("\n"))
	mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	resp, err := httpClient.R().
		SetContext(context.Background()).
		SetHeader("Content-Type", "application/json").
		SetHeader("X-OpenList-Event", item.EventType).
		SetHeader("X-OpenList-Event-Id", item.EventID).
		SetHeader("X-OpenList-Instance", cfg.InstanceName).
		SetHeader("X-OpenList-Timestamp", timestamp).
		SetHeader("X-OpenList-Signature", signature).
		SetBody(body).
		Post(cfg.WebhookURL)
	if err != nil {
		log.Errorf("updatesite: send %s failed: %v", item.EventType, err)
		_ = db.MarkUpdateSiteOutboxRetry(item.ID, nextRetryAt(item.AttemptCount), err.Error())
		return
	}
	if resp.StatusCode() >= http.StatusBadRequest {
		msg := fmt.Sprintf("status %d: %s", resp.StatusCode(), strings.TrimSpace(resp.String()))
		log.Errorf("updatesite: send %s failed with %s", item.EventType, msg)
		_ = db.MarkUpdateSiteOutboxRetry(item.ID, nextRetryAt(item.AttemptCount), msg)
		return
	}
	_ = db.MarkUpdateSiteOutboxSent(item.ID, time.Now())
}

func NotifyCatalogChanged(rawDirPath string, source string, reason string, recursive bool, requestID string) {
	cfg := currentConfig()
	if !cfg.Enabled {
		return
	}
	publicPath, ok := resolvePublicPath(rawDirPath)
	if !ok {
		return
	}
	if requestID == "" {
		requestID = uuid.NewString()
	}
	eventID := uuid.NewString()
	payload := catalogChangedEvent{
		SchemaVersion: 1,
		EventID:       eventID,
		EventType:     EventCatalogChanged,
		Instance: eventInstance{
			Name:    cfg.InstanceName,
			BaseURL: cfg.BaseURL,
		},
		OccurredAt:    time.Now().UTC(),
		Source:        source,
		Reason:        reason,
		PublicDirPath: publicPath,
		VisibleRoot:   visibleRootForPath(publicPath),
		Recursive:     recursive,
		RequestID:     requestID,
	}
	enqueue(EventCatalogChanged, eventID, cfg, payload)
}

func NotifyContentAccess(rawPath string, obj model.Obj, accessKind string, actor string, requestID string) {
	cfg := currentConfig()
	if !cfg.Enabled || obj == nil || obj.IsDir() {
		return
	}
	publicPath, ok := resolvePublicPath(rawPath)
	if !ok {
		return
	}
	if requestID == "" {
		requestID = uuid.NewString()
	}
	thumb, _ := model.GetThumb(obj)
	eventID := uuid.NewString()
	payload := contentAccessEvent{
		SchemaVersion: 1,
		EventID:       eventID,
		EventType:     EventContentAccess,
		Instance: eventInstance{
			Name:    cfg.InstanceName,
			BaseURL: cfg.BaseURL,
		},
		OccurredAt: time.Now().UTC(),
		AccessKind: accessKind,
		Score:      1,
		Item: contentAccessItem{
			VisibleRoot: visibleRootForPath(publicPath),
			BucketPath:  defaultBucketPath(publicPath, false),
			VisiblePath: publicPath,
			Name:        obj.GetName(),
			IsDir:       false,
			Size:        obj.GetSize(),
			ModifiedAt:  modifiedAt(obj),
			ThumbURL:    thumb,
			MimeHint:    utils.GetMimeType(obj.GetName()),
		},
		Dedupe: dedupeWindow{
			Key:           dedupeKey(actor, publicPath, accessKind, timeWindowBucket(1800)),
			WindowSeconds: 1800,
		},
		RequestID: requestID,
	}
	enqueue(EventContentAccess, eventID, cfg, payload)
}

func ActorKey(username string, ip string) string {
	username = strings.TrimSpace(username)
	switch {
	case username != "" && username != "guest":
		return fmt.Sprintf("user:%s", username)
	case ip != "":
		return fmt.Sprintf("ip:%s", ip)
	default:
		return "anon"
	}
}

func timeWindowBucket(windowSeconds int) string {
	if windowSeconds <= 0 {
		windowSeconds = 1800
	}
	return fmt.Sprintf("%d", time.Now().UTC().Unix()/int64(windowSeconds))
}

func nextRetryAt(attempt int) time.Time {
	if attempt < 1 {
		attempt = 1
	}
	backoff := time.Duration(attempt*attempt) * 5 * time.Second
	if backoff > 15*time.Minute {
		backoff = 15 * time.Minute
	}
	return time.Now().Add(backoff)
}
