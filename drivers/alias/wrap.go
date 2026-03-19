package alias

import "github.com/OpenListTeam/OpenList/v4/internal/model"

type rawPathProvider interface {
	RawPath() string
}

type aliasViewObj struct {
	model.Obj
	path    string
	rawPath string
	mask    model.ObjMask
}

func (o *aliasViewObj) GetPath() string {
	return o.path
}

func (o *aliasViewObj) SetPath(path string) {
	o.path = path
}

func (o *aliasViewObj) GetObjMask() model.ObjMask {
	return o.mask
}

func (o *aliasViewObj) RawPath() string {
	if o.rawPath != "" {
		return o.rawPath
	}
	return o.Obj.GetPath()
}

func (o *aliasViewObj) Unwrap() model.Obj {
	return o.Obj
}

type thumbOverlay struct {
	model.Obj
	thumb string
}

func (o *thumbOverlay) Thumb() string {
	return o.thumb
}

func (o *thumbOverlay) Unwrap() model.Obj {
	return o.Obj
}

type providerOverlay struct {
	model.Obj
	provider string
}

func (o *providerOverlay) GetProvider() string {
	return o.provider
}

func (o *providerOverlay) Unwrap() model.Obj {
	return o.Obj
}

func wrapAliasViewObj(obj model.Obj, rawPath, aliasPath string, mask model.ObjMask) model.Obj {
	return &aliasViewObj{
		Obj:     obj,
		path:    aliasPath,
		rawPath: rawPath,
		mask:    mask,
	}
}

func hasThumb(obj model.Obj) bool {
	thumb, ok := model.GetThumb(obj)
	return ok && thumb != ""
}

func borrowThumb(obj, candidate model.Obj) model.Obj {
	if obj == nil || obj.IsDir() || hasThumb(obj) {
		return obj
	}
	thumb, ok := model.GetThumb(candidate)
	if !ok || thumb == "" {
		return obj
	}
	if !isConsistent(obj, candidate) {
		return obj
	}
	return &thumbOverlay{Obj: obj, thumb: thumb}
}

func getRawPath(obj model.Obj) string {
	for {
		switch o := obj.(type) {
		case rawPathProvider:
			return o.RawPath()
		case model.ObjUnwrap:
			obj = o.Unwrap()
		default:
			return obj.GetPath()
		}
	}
}
