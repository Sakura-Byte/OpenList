package thumbnail

import (
	"encoding/base64"
	"net/url"
	"testing"
	"time"
)

func TestParseSharePointThumbnailExpiry(t *testing.T) {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"exp":"1774191600"}`))
	tempauth := "v1." + payload + ".signature"
	docID := (&url.URL{
		Scheme:   "https",
		Host:     "example.sharepoint.com",
		Path:     "/_api/v2.0/drives/demo/items/123",
		RawQuery: url.Values{"tempauth": []string{tempauth}}.Encode(),
	}).String()
	raw := (&url.URL{
		Scheme: "https",
		Host:   "japaneast1-mediap.svc.ms",
		Path:   "/transform/thumbnail",
		RawQuery: url.Values{
			"docid": []string{docID},
			"width": []string{"176"},
		}.Encode(),
	}).String()

	expiry, err := ParseSharePointThumbnailExpiry(raw)
	if err != nil {
		t.Fatalf("ParseSharePointThumbnailExpiry returned error: %v", err)
	}

	expected := time.Unix(1774191600, 0).UTC()
	if expiry == nil || !expiry.Equal(expected) {
		t.Fatalf("expected expiry %s, got %v", expected, expiry)
	}
}

func TestParseSharePointThumbnailExpiryFromDirectTempAuth(t *testing.T) {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"exp":1774018800}`))
	raw := (&url.URL{
		Scheme:   "https",
		Host:     "japaneast1-mediap.svc.ms",
		Path:     "/transform/thumbnail",
		RawQuery: url.Values{"tempauth": []string{"v1." + payload + ".signature"}}.Encode(),
	}).String()

	expiry, err := ParseSharePointThumbnailExpiry(raw)
	if err != nil {
		t.Fatalf("ParseSharePointThumbnailExpiry returned error: %v", err)
	}

	expected := time.Unix(1774018800, 0).UTC()
	if expiry == nil || !expiry.Equal(expected) {
		t.Fatalf("expected expiry %s, got %v", expected, expiry)
	}
}

func TestParseSharePointThumbnailExpiryMissing(t *testing.T) {
	_, err := ParseSharePointThumbnailExpiry("https://example.com/thumb")
	if err == nil {
		t.Fatal("expected missing expiry error")
	}
}
