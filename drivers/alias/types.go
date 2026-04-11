package alias

import (
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
)

const (
	DisabledWP             = "disabled"
	FirstRWP               = "first"
	DeterministicWP        = "deterministic"
	DeterministicOrAllWP   = "deterministic_or_all"
	AllRWP                 = "all"
	AllStrictWP            = "all_strict"
	RandomBalancedRP       = "random"
	BalancedByQuotaP       = "quota"
	BalancedByQuotaStrictP = "quota_strict"
)

var (
	ValidReadConflictPolicy  = []string{FirstRWP, RandomBalancedRP, AllRWP}
	ValidWriteConflictPolicy = []string{DisabledWP, FirstRWP, DeterministicWP, DeterministicOrAllWP, AllRWP,
		AllStrictWP}
	ValidPutConflictPolicy = []string{DisabledWP, FirstRWP, DeterministicWP, DeterministicOrAllWP, AllRWP,
		AllStrictWP, RandomBalancedRP, BalancedByQuotaP, BalancedByQuotaStrictP}
)

var (
	ErrPathConflict     = errors.New("path conflict")
	ErrSamePathLeak     = errors.New("leak some of same-name dirs")
	ErrNoEnoughSpace    = errors.New("none of same-name dirs has enough space")
	ErrNotEnoughSrcObjs = errors.New("cannot move fewer objs to more paths, please try copying")
)

type BalancedObjs []model.Obj

func (b BalancedObjs) GetSize() int64 {
	if len(b) == 0 || b[0] == nil {
		return 0
	}
	if !b[0].IsDir() {
		return b[0].GetSize()
	}
	var total int64
	for _, obj := range b {
		if obj == nil || !obj.IsDir() {
			continue
		}
		total += obj.GetSize()
	}
	return total
}

func (b BalancedObjs) ModTime() time.Time {
	return b[0].ModTime()
}

func (b BalancedObjs) CreateTime() time.Time {
	return b[0].CreateTime()
}

func (b BalancedObjs) IsDir() bool {
	return b[0].IsDir()
}

func (b BalancedObjs) GetHash() utils.HashInfo {
	return b[0].GetHash()
}

func (b BalancedObjs) GetName() string {
	return b[0].GetName()
}

func (b BalancedObjs) GetPath() string {
	return b[0].GetPath()
}

func (b BalancedObjs) GetID() string {
	return b[0].GetID()
}

func (b BalancedObjs) Unwrap() model.Obj {
	return b[0]
}

var _ model.Obj = (BalancedObjs)(nil)

type tempObj struct{ model.Object }
