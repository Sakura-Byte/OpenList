package alias

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func TestResolveBackingPathUsesFirstRootHoldingTheFile(t *testing.T) {
	onlySecond := newFakeFile("/only-second.bin", 1, "", "", nil)
	onBoth := newFakeFile("/both.bin", 1, "", "", nil)
	fixture := setupAliasFixture(t,
		&fakeThumbData{get: map[string]model.Obj{"/both.bin": onBoth}, list: map[string][]model.Obj{"/": {onBoth}}},
		&fakeThumbData{get: map[string]model.Obj{"/both.bin": onBoth, "/only-second.bin": onlySecond}, list: map[string][]model.Obj{"/": {onBoth, onlySecond}}},
		false, false)
	storage, _, err := op.PeekStorageAndActualPath(fixture.aliasMount)
	if err != nil {
		t.Fatal(err)
	}
	resolver, ok := storage.(driver.BackingPathResolver)
	if !ok {
		t.Fatalf("%T must resolve backing paths", storage)
	}
	first, err := resolver.ResolveBackingPath(context.Background(), "/both.bin")
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolver.ResolveBackingPath(context.Background(), "/only-second.bin")
	if err != nil {
		t.Fatal(err)
	}
	if first == second || second[len(second)-len("-b/only-second.bin"):] != "-b/only-second.bin" || first[len(first)-len("-a/both.bin"):] != "-a/both.bin" {
		t.Fatalf("resolved to %q and %q", first, second)
	}
	if _, err := resolver.ResolveBackingPath(context.Background(), "/missing.bin"); err == nil {
		t.Fatal("missing file must not resolve")
	}
}

func TestLinkCloneKeepsStorage(t *testing.T) {
	if got := (&model.Link{Storage: "/mount.balance1"}).Clone().Storage; got != "/mount.balance1" {
		t.Fatalf("Clone lost the storage, got %q", got)
	}
}
