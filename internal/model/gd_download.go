package model

// GDDownloadCount counts Google Drive downloads of one IP in one hour.
type GDDownloadCount struct {
	IPKey      string `json:"ip_key" gorm:"primaryKey;size:64"`
	HourBucket int64  `json:"hour_bucket" gorm:"primaryKey;autoIncrement:false"`
	Count      int    `json:"count"`
}

// GDDownloadStat counts Google Drive downloads of one author in one UTC day.
// Bytes is an estimate: file size times the number of downloads.
type GDDownloadStat struct {
	Area   string `json:"area" gorm:"primaryKey;size:64"`
	Author string `json:"author" gorm:"primaryKey;size:255"`
	Day    string `json:"day" gorm:"primaryKey;size:10"`
	Count  int64  `json:"count"`
	Bytes  int64  `json:"bytes"`
}
