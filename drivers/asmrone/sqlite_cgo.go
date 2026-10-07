//go:build sqlite_cgo_compat || (linux && (mips || mips64 || mips64le || mipsle || loong64)) || (windows && 386)

package asmrone

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openSQLite(filename string) gorm.Dialector { return sqlite.Open(filename) }
