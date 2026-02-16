package huggingface_hub

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	driver.RootPath
	APIEndpoint                  string `json:"api_endpoint" type:"string" default:"https://huggingface.co" required:"true" help:"Hugging Face API endpoint for metadata/listing, e.g. https://huggingface.co"`
	DownloadEndpoint             string `json:"download_endpoint" type:"string" help:"Optional download mirror endpoint for resolve links, e.g. https://hf-mirror.com"`
	RepoType                     string `json:"repo_type" type:"select" options:"model,dataset,space" default:"model" required:"true"`
	RepoID                       string `json:"repo_id" type:"string" required:"true" help:"Repository id, e.g. openai-community/gpt2"`
	Revision                     string `json:"revision" type:"string" default:"main" required:"true"`
	Token                        string `json:"token" type:"string" help:"Optional Hugging Face token for private/gated repos"`
	ForwardTokenToDownloadMirror bool   `json:"forward_token_to_download_mirror" type:"bool" default:"false" help:"Forward token to custom download mirror (security risk)"`
}

var config = driver.Config{
	Name:        "HuggingFace Hub",
	LocalSort:   true,
	OnlyProxy:   false,
	NoUpload:    true,
	DefaultRoot: "/",
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &HuggingFaceHub{}
	})
}
