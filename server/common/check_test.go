package common

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestCanAccessHideRelativePath(t *testing.T) {
	user := &model.User{}
	meta := &model.Meta{
		Path: "/a",
		Hide: "c.+",
		HSub: true,
	}
	if CanAccess(user, meta, "/a/b/cdef/gghh", "") {
		t.Fatalf("expected hide pattern to block access to nested path")
	}
	if !CanAccess(user, meta, "/a/b/other", "") {
		t.Fatalf("expected unmatched path to stay accessible")
	}
}
