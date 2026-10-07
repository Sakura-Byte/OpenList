package model

// TagInfo describes a tag directory. Count refers to locally cached works.
type TagInfo struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type ObjWithTag interface{ GetTagInfo() *TagInfo }

func GetTagInfo(obj Obj) *TagInfo {
	for obj != nil {
		if t, ok := obj.(ObjWithTag); ok {
			return t.GetTagInfo()
		}
		w, ok := obj.(ObjUnwrap)
		if !ok {
			break
		}
		obj = w.Unwrap()
	}
	return nil
}
