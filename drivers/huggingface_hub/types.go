package huggingface_hub

type HFRepoTreeEntry struct {
	Type    string         `json:"type"`
	OID     string         `json:"oid"`
	Size    int64          `json:"size"`
	Path    string         `json:"path"`
	LFS     *HFRepoLFSInfo `json:"lfs,omitempty"`
	XetHash string         `json:"xetHash,omitempty"`
}

type HFRepoLFSInfo struct {
	OID         string `json:"oid"`
	Size        int64  `json:"size"`
	PointerSize int64  `json:"pointerSize"`
}

type HFErrorResp struct {
	Error string `json:"error"`
}
