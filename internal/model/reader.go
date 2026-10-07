package model

// ReaderInfo advertises a readable work or chapter without exposing a fake file.
type ReaderInfo struct {
	Kind      string `json:"kind"`
	Mode      string `json:"mode"`
	AlbumID   string `json:"album_id"`
	ChapterID string `json:"chapter_id,omitempty"`
}

type ObjWithReader interface{ GetReaderInfo() *ReaderInfo }

func GetReaderInfo(obj Obj) *ReaderInfo {
	for obj != nil {
		if r, ok := obj.(ObjWithReader); ok {
			return r.GetReaderInfo()
		}
		w, ok := obj.(ObjUnwrap)
		if !ok {
			break
		}
		obj = w.Unwrap()
	}
	return nil
}
