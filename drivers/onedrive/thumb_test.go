package onedrive

import (
	"encoding/base64"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func sampleSharePointThumbnailURL(t *testing.T, exp int64) string {
	t.Helper()
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"exp":"` + strconv.FormatInt(exp, 10) + `"}`))
	tempauth := "v1." + payload + ".signature"
	docID := (&url.URL{
		Scheme:   "https",
		Host:     "example.sharepoint.com",
		Path:     "/_api/v2.0/drives/demo/items/123",
		RawQuery: url.Values{"tempauth": []string{tempauth}}.Encode(),
	}).String()
	return (&url.URL{
		Scheme: "https",
		Host:   "japaneast1-mediap.svc.ms",
		Path:   "/transform/thumbnail",
		RawQuery: url.Values{
			"docid": []string{docID},
			"width": []string{"176"},
		}.Encode(),
	}).String()
}

func TestFileToObjStoresThumbnailExpiry(t *testing.T) {
	raw := sampleSharePointThumbnailURL(t, 1774191600)
	obj := fileToObj(File{
		Id:   "1",
		Name: "demo.mp4",
		File: &struct {
			MimeType string "json:\"mimeType\""
		}{MimeType: "video/mp4"},
		Thumbnails: []struct {
			Medium struct {
				Url string `json:"url"`
			} `json:"medium"`
		}{
			{Medium: struct {
				Url string `json:"url"`
			}{Url: raw}},
		},
	}, "parent")

	expiry, ok := model.GetThumbExpiration(obj)
	if !ok || expiry == nil {
		t.Fatal("expected thumbnail expiry on OneDrive object")
	}
	if want := time.Unix(1774191600, 0).UTC(); !expiry.Equal(want) {
		t.Fatalf("expected expiry %s, got %v", want, expiry)
	}
}

func TestUpdateSiteThumbnailValueHidesNearExpiredThumb(t *testing.T) {
	expiry := time.Now().Add(30 * time.Second)
	obj := &model.ObjThumb{
		Object: model.Object{Name: "demo.mp4"},
		Thumbnail: model.Thumbnail{
			Thumbnail: "thumb://demo",
			ExpiresAt: &expiry,
		},
	}

	if got := updateSiteThumbnailValue(obj, true, time.Now()); got != "" {
		t.Fatalf("expected near-expired thumbnail to be hidden, got %q", got)
	}
	if got := updateSiteThumbnailValue(obj, false, time.Now()); got == "" {
		t.Fatal("expected thumbnail to remain visible when override is disabled")
	}
}
