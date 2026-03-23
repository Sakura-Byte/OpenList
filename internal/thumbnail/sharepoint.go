package thumbnail

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrExpiryNotFound = errors.New("thumbnail expiry not found")

func ParseSharePointThumbnailExpiry(raw string) (*time.Time, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, err
	}
	candidates := make([]string, 0, 2)
	if token := strings.TrimSpace(parsed.Query().Get("tempauth")); token != "" {
		candidates = append(candidates, token)
	}
	if docID := strings.TrimSpace(parsed.Query().Get("docid")); docID != "" {
		decodedDocID, err := url.QueryUnescape(docID)
		if err == nil {
			docURL, parseErr := url.Parse(decodedDocID)
			if parseErr == nil {
				if token := strings.TrimSpace(docURL.Query().Get("tempauth")); token != "" {
					candidates = append(candidates, token)
				}
			}
		}
	}
	for _, token := range candidates {
		expiry, err := parseTempAuthExpiry(token)
		if err == nil && expiry != nil {
			return expiry, nil
		}
	}
	return nil, ErrExpiryNotFound
}

func parseTempAuthExpiry(token string) (*time.Time, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	switch {
	case len(parts) >= 3 && parts[0] == "v1":
		return decodeTempAuthExpiry(parts[1])
	case len(parts) >= 2:
		return decodeTempAuthExpiry(parts[1])
	default:
		return nil, ErrExpiryNotFound
	}
}

func decodeTempAuthExpiry(payload string) (*time.Time, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		decoded, err = base64.URLEncoding.DecodeString(payload)
		if err != nil {
			return nil, err
		}
	}
	var body struct {
		Exp json.RawMessage `json:"exp"`
	}
	if err := json.Unmarshal(decoded, &body); err != nil {
		return nil, err
	}
	if len(body.Exp) == 0 {
		return nil, ErrExpiryNotFound
	}
	var unixSeconds int64
	if body.Exp[0] == '"' {
		var raw string
		if err := json.Unmarshal(body.Exp, &raw); err != nil {
			return nil, err
		}
		unixSeconds, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		if err := json.Unmarshal(body.Exp, &unixSeconds); err != nil {
			return nil, err
		}
	}
	expiry := time.Unix(unixSeconds, 0).UTC()
	return &expiry, nil
}
