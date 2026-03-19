package op

import "github.com/OpenListTeam/OpenList/v4/internal/updatesite"

func init() {
	updatesite.SetStorageProvider(GetAllStorages)
}
