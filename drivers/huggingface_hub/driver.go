package huggingface_hub

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
)

type HuggingFaceHub struct {
	model.Storage
	Addition

	client *resty.Client

	apiEndpointURL         *url.URL
	downloadEndpointURL    *url.URL
	normalizedAPIBase      string
	normalizedDownloadBase string
	repoType               string
	revision               string
	repoID                 string
	token                  string
}

func (d *HuggingFaceHub) Config() driver.Config {
	return config
}

func (d *HuggingFaceHub) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *HuggingFaceHub) Init(ctx context.Context) error {
	d.RootFolderPath = utils.FixAndCleanPath(d.RootFolderPath)

	var err error
	d.normalizedAPIBase, d.apiEndpointURL, err = normalizeEndpoint(d.APIEndpoint, defaultAPIEndpoint)
	if err != nil {
		return err
	}
	d.normalizedDownloadBase, d.downloadEndpointURL, err = normalizeOptionalEndpoint(d.DownloadEndpoint)
	if err != nil {
		return err
	}

	d.repoType = strings.ToLower(strings.TrimSpace(d.RepoType))
	if d.repoType == "" {
		d.repoType = "model"
	}
	if !isValidRepoType(d.repoType) {
		return fmt.Errorf("invalid repo_type: %q", d.RepoType)
	}

	d.repoID = strings.TrimSpace(d.RepoID)
	if !isValidRepoID(d.repoID) {
		return fmt.Errorf("invalid repo_id: %q, expected owner/repo", d.RepoID)
	}

	d.revision = strings.TrimSpace(d.Revision)
	if d.revision == "" {
		d.revision = "main"
	}

	d.token = strings.TrimSpace(d.Token)

	d.client = base.NewRestyClient().
		SetHeader("Accept", "application/json")
	if d.token != "" {
		d.client.SetHeader("Authorization", "Bearer "+d.token)
	}

	if err := d.probeRoot(ctx); err != nil {
		switch {
		case errors.Is(err, errs.ObjectNotFound):
			return fmt.Errorf("root folder path %q not found in repo %q: %w", d.RootFolderPath, d.repoID, err)
		case errors.Is(err, errHFRevisionNotFound):
			return fmt.Errorf("invalid revision %q for repo %q: %w", d.revision, d.repoID, err)
		case errors.Is(err, errHFAccessDenied):
			return fmt.Errorf("cannot access repo %q, check token/permissions: %w", d.repoID, err)
		default:
			return err
		}
	}
	return nil
}

func (d *HuggingFaceHub) Drop(ctx context.Context) error {
	return nil
}

func (d *HuggingFaceHub) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	entries, err := d.fetchTreeEntries(ctx, storagePathToRepoPath(dir.GetPath()))
	if err != nil {
		return nil, err
	}

	result := make([]model.Obj, 0, len(entries))
	for _, entry := range entries {
		name := entryName(entry.Path)
		if name == "" {
			continue
		}

		obj := &model.Object{
			ID:       entry.OID,
			Name:     name,
			Size:     entry.Size,
			Modified: time.Unix(0, 0),
			IsFolder: entry.Type == "directory",
		}
		if entry.LFS != nil && len(entry.LFS.OID) == utils.SHA256.Width {
			obj.HashInfo = utils.NewHashInfo(utils.SHA256, entry.LFS.OID)
		}
		result = append(result, obj)
	}
	return result, nil
}

func (d *HuggingFaceHub) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	repoPath := storagePathToRepoPath(file.GetPath())
	if repoPath == "" {
		return nil, errs.NotFile
	}

	link := &model.Link{
		URL: d.buildResolveURL(repoPath),
	}
	if d.shouldForwardTokenForDownload(args) {
		link.Header = http.Header{
			"Authorization": []string{"Bearer " + d.token},
		}
	}
	return link, nil
}

func (d *HuggingFaceHub) probeRoot(ctx context.Context) error {
	_, err := d.fetchTreeEntries(ctx, storagePathToRepoPath(d.RootFolderPath))
	return err
}

func (d *HuggingFaceHub) fetchTreeEntries(ctx context.Context, repoPath string) ([]HFRepoTreeEntry, error) {
	nextPageURL := d.buildTreeURL(repoPath)
	entries := make([]HFRepoTreeEntry, 0)

	for nextPageURL != "" {
		pageEntries, pageNextURL, err := d.fetchTreePage(ctx, nextPageURL)
		if err != nil {
			return nil, err
		}
		entries = append(entries, pageEntries...)
		nextPageURL = pageNextURL
	}
	return entries, nil
}

func (d *HuggingFaceHub) fetchTreePage(ctx context.Context, pageURL string) ([]HFRepoTreeEntry, string, error) {
	res, err := d.client.R().
		SetContext(ctx).
		Get(pageURL)
	if err != nil {
		return nil, "", err
	}
	if res.StatusCode() != http.StatusOK {
		return nil, "", parseHFError(res)
	}

	var entries []HFRepoTreeEntry
	if err := utils.Json.Unmarshal(res.Body(), &entries); err != nil {
		return nil, "", err
	}
	nextPageURL := parseNextPageURL(res.Header().Get("Link"), pageURL)
	return entries, nextPageURL, nil
}

func (d *HuggingFaceHub) buildTreeURL(repoPath string) string {
	treePath := fmt.Sprintf("api/%s/%s/tree/%s", repoTypeToPlural[d.repoType], d.repoID, url.PathEscape(d.revision))
	if repoPath != "" {
		treePath += "/" + url.PathEscape(repoPath)
	}
	return fmt.Sprintf("%s/%s?recursive=false&expand=false", d.normalizedAPIBase, treePath)
}

func (d *HuggingFaceHub) buildResolveURL(repoPath string) string {
	base := d.normalizedAPIBase
	if d.normalizedDownloadBase != "" {
		base = d.normalizedDownloadBase
	}
	repoPathInURL := repoTypeToResolvePrefix[d.repoType] + d.repoID
	return fmt.Sprintf("%s/%s/resolve/%s/%s",
		base, repoPathInURL, url.PathEscape(d.revision), url.PathEscape(repoPath))
}

func (d *HuggingFaceHub) shouldForwardTokenForDownload(args model.LinkArgs) bool {
	if d.token == "" {
		return false
	}
	// Keep 302 direct-link downloads anonymous unless this is the admin get-link endpoint.
	if args.Redirect && args.Type != model.LinkTypeAdminGetLink {
		return false
	}
	if d.normalizedDownloadBase == "" {
		return true
	}
	if endpointHostEqual(d.apiEndpointURL, d.downloadEndpointURL) {
		return true
	}
	return d.ForwardTokenToDownloadMirror
}

var _ driver.Driver = (*HuggingFaceHub)(nil)
