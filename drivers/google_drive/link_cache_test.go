package google_drive

import (
	"testing"
	"time"
)

func TestLinkCacheTTL(t *testing.T) {
	d := &GoogleDrive{}
	if got := d.linkCacheTTL(); got != 0 {
		t.Fatalf("token never issued: ttl = %v, want 0", got)
	}

	d.markTokenIssued()
	if got := d.linkCacheTTL(); got != maxLinkCacheTTL {
		t.Fatalf("fresh token: ttl = %v, want %v", got, maxLinkCacheTTL)
	}

	d.tokenIssuedAt.Store(time.Now().Add(-40 * time.Minute).Unix())
	got := d.linkCacheTTL()
	if got <= 0 || got > accessTokenLifetime-linkCacheMargin-40*time.Minute+time.Second {
		t.Fatalf("aging token: ttl = %v, want about 15m", got)
	}

	d.tokenIssuedAt.Store(time.Now().Add(-56 * time.Minute).Unix())
	if got := d.linkCacheTTL(); got > 0 {
		t.Fatalf("token near expiry: ttl = %v, want <= 0", got)
	}
}
