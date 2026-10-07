# Metadata grid batch updates

The metadata grid saves all rows with one administrator-only request:

```http
POST /api/admin/meta/update_grid
Content-Type: application/json
Authorization: <admin token>
```

```json
{
  "metas": [
    {
      "id": 1,
      "path": "/documents",
      "password": "",
      "p_sub": false,
      "write": false,
      "w_sub": false,
      "hide": "",
      "h_sub": false,
      "header": "Welcome",
      "header_sub": true,
      "readme": "Updated description",
      "r_sub": true
    }
  ]
}
```

Each item replaces all grid-editable fields on an existing record. Empty strings
and `false` clear existing values. The API preserves `read_users`,
`read_users_sub`, `write_users`, and `write_users_sub`; edit those through the
single-record metadata editor.

The nonempty `metas` array must contain nonzero IDs and nonempty paths. IDs and
normalized paths must be unique within the request, and all hide expressions
must be valid. Paths are normalized using the same rules as single-record saves.
The records are updated in one database transaction. Missing records or path
conflicts roll back the entire request; swaps between occupied paths are rejected.

Responses use the existing HTTP 200 envelope with business code `200` on success,
`400` for invalid input, `403` for unauthorized access, and `500` for database
errors. After a successful commit, caches for old and new paths are invalidated.
Reads started before the save cannot refill those caches with stale values.

The frontend remembers the grid/list preference in local storage under
`metas-layout`. Draft edits are saved only when the user clicks Save All.

## Verification

`go test -race ./server -run '^TestMetadata|^TestHiddenPathsLookMissing$' -count=1`
covers single-save visibility, overlapping reads, batch validation,
administrator access, rollback, clearing values, access-list preservation, and
path-cache invalidation. Tests use the real router, authentication, SQLite, and
local storage fixtures.

Browser checks cover typing without losing focus, one request for multiple rows,
grid preference after reload, single-record saving, and retention of drafts when
a batch request fails.

Verified on 2026-10-06:

- `go test -race ./server/... ./internal/op ./internal/db` passed.
- Backend `go build` and frontend `pnpm build` passed. The local locked
  `mpegts.js` dependency was built from source to restore its missing bundle.
- Real-backend browser checks saved two rows with one batch request and saw the
  new header/readme on the next folder request. Both grid and list preferences
  survived reload, and a rejected batch kept the draft and left the DB unchanged.
- `pnpm lint` remains blocked by four existing errors in the archive preview,
  local settings toolbar, and user editor. The same errors were reproduced on a
  clean `HEAD` snapshot using the same installed dependencies.
- `go test ./...` remains blocked by existing nonconstant format-string vet
  errors in drivers/offline downloads and aria2 tests requiring `localhost:6800`.
