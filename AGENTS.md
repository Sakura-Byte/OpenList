# Repository Guidelines

## Project Structure & Modules
- Root Go module: `github.com/OpenListTeam/OpenList/v4` (see `go.mod`).
- CLI and entrypoints: `cmd/` (e.g., `cmd/server.go`, `cmd/start.go`).
- HTTP/FTP/S3/WebDAV servers and middleware: `server/`.
- Core business logic and subsystems: `internal/` (e.g., `internal/conf`, `internal/op`, `internal/db`).
- Storage providers: `drivers/` (one folder per driver; start from `drivers/template`).
- Shared libraries: `pkg/`.
- Embedded/static assets: `public/`, `server/static/`.
- Main entry: `main.go`.

## Build, Run, and Test
- Build: `go build -o bin/openlist .` — compile backend.
- Run (dev): `go run main.go` — start API/server locally.
- Test (all): `go test ./...` — run unit tests.
- Test with race/coverage: `go test -race -cover ./...`.
- Format: `go fmt ./...` (or `gofmt -s -w .`).
- Docker (optional): `docker build -t openlist .` or `docker-compose up`.

## Coding Style & Naming
- Use standard Go formatting (tabs via `gofmt`). No custom style.
- Packages: lowercase, short (e.g., `internal/op`). Files: snake_case.
- Exported identifiers: `CamelCase`. Unexported: `camelCase`.
- Errors: wrap with context (`fmt.Errorf("...: %w", err)`) and prefer sentinel/typed errors where applicable.

## Testing Guidelines
- Frameworks: standard `testing`, with `testify` available.
- Test files: `*_test.go`; test names `TestXxx`/`BenchmarkXxx`.
- Aim to cover new logic and regressions; prefer table-driven tests.
- Run locally: `go test ./...` before opening a PR.

## Commit & Pull Requests
- Commits: follow Conventional Commits (e.g., `feat: add s3 listing cache`, `fix: handle 404 on webdav propfind`).
- Keep commits focused; include rationale in the body if behavior changes.
- PRs to `main` must include:
  - Clear description, linked issues (e.g., `Closes #123`).
  - Tests for functional changes; updated docs where relevant.
  - Evidence it runs locally (command or screenshot/log excerpt).

## Security & Configuration
- Configuration uses env and JSON structs under `internal/conf` (e.g., `DB_*`, `S3_*`, `MEILISEARCH_*`). Avoid committing secrets.
- Prefer reading config via env/flags over hardcoding. Do not log secrets.

## Agent-Specific Notes
- Place new features under the appropriate module (`drivers/`, `internal/*`, `server/*`).
- Avoid broad refactors; keep patches minimal and scoped.
- When adding a driver, copy `drivers/template` and follow in-file comments.

## Tool Priority

- Filename search: `fd`.
- Text/content search: `rg` (ripgrep).
- AST/structural search: `sg` (ast-grep) — preferred for code-aware queries (imports, call expressions, JSX/TSX nodes).

### AST-grep Usage (Windows)

- Announce intent and show the exact command before running complex patterns.
- Common queries:
  - Find imports from `node:path` (TypeScript/TSX):
    - `ast-grep -p "import $$ from 'node:path'" src --lang ts,tsx,mts,cts`
  - Find CommonJS requires of `node:path`:
    - `ast-grep -p "require('node:path')" src --lang js,cjs,mjs,ts,tsx`
  - Suggest rewrite (do not auto-apply in code unless approved):
    - Search: `ast-grep -p "import $$ from 'node:path'" src --lang ts,tsx`
    - Proposed replacement: `import $$ from 'pathe'`

### Search Hygiene (fd/rg/sg)

- Exclude bulky folders to keep searches fast and relevant: `.git`, `node_modules`, `coverage`, `out`, `dist`.
- Prefer running searches against a scoped path (e.g., `src`) to implicitly avoid vendor and VCS directories.
- Examples:
  - `rg -n "pattern" -g "!{.git,node_modules,coverage,out,dist}" src`
  - `fd --hidden --exclude .git --exclude node_modules --exclude coverage --exclude out --exclude dist --type f ".tsx?$" src`
- ast-grep typically respects `.gitignore`; target `src` to avoid scanning vendor folders:
  - `ast-grep -p "import $$ from '@shared/$$'" src --lang ts,tsx,mts,cts`
  - If needed, add ignore patterns to your ignore files rather than disabling ignores.