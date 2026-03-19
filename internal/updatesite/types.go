package updatesite

import "time"

const (
	EventCatalogChanged = "catalog.changed"
	EventContentAccess  = "content.access"

	SourceRefresh    = "refresh"
	SourceManualScan = "manual_scan"
	SourceIndex      = "index"
	SourceWrite      = "write"

	ReasonRefresh     = "refresh"
	ReasonScan        = "scan"
	ReasonIndexUpdate = "index_update"
	ReasonUpload      = "upload"
	ReasonCopy        = "copy"
	ReasonDecompress  = "decompress"
	ReasonMove        = "move"
	ReasonRename      = "rename"
	ReasonRemove      = "remove"

	AccessKindDetail  = "detail"
	AccessKindConsume = "consume"
)

type eventInstance struct {
	Name    string `json:"name"`
	BaseURL string `json:"base_url"`
}

type catalogChangedEvent struct {
	SchemaVersion int           `json:"schema_version"`
	EventID       string        `json:"event_id"`
	EventType     string        `json:"event_type"`
	Instance      eventInstance `json:"instance"`
	OccurredAt    time.Time     `json:"occurred_at"`
	Source        string        `json:"source"`
	Reason        string        `json:"reason"`
	PublicDirPath string        `json:"public_dir_path"`
	VisibleRoot   string        `json:"visible_root"`
	Recursive     bool          `json:"recursive"`
	RequestID     string        `json:"request_id"`
}

type dedupeWindow struct {
	Key           string `json:"key"`
	WindowSeconds int    `json:"window_seconds"`
}

type contentAccessItem struct {
	VisibleRoot string     `json:"visible_root"`
	BucketPath  string     `json:"bucket_path"`
	VisiblePath string     `json:"visible_path"`
	Name        string     `json:"name"`
	IsDir       bool       `json:"is_dir"`
	Size        int64      `json:"size"`
	ModifiedAt  *time.Time `json:"modified_at,omitempty"`
	ThumbURL    string     `json:"thumb_url,omitempty"`
	MimeHint    string     `json:"mime_hint,omitempty"`
}

type contentAccessEvent struct {
	SchemaVersion int               `json:"schema_version"`
	EventID       string            `json:"event_id"`
	EventType     string            `json:"event_type"`
	Instance      eventInstance     `json:"instance"`
	OccurredAt    time.Time         `json:"occurred_at"`
	AccessKind    string            `json:"access_kind"`
	Score         int               `json:"score"`
	Item          contentAccessItem `json:"item"`
	Dedupe        dedupeWindow      `json:"dedupe"`
	RequestID     string            `json:"request_id"`
}
