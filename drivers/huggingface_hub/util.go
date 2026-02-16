package huggingface_hub

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	stdpath "path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
)

const defaultAPIEndpoint = "https://huggingface.co"

var (
	errHFRevisionNotFound = errors.New("huggingface revision not found")
	errHFAccessDenied     = errors.New("huggingface access denied")
)

var repoTypeToPlural = map[string]string{
	"model":   "models",
	"dataset": "datasets",
	"space":   "spaces",
}

var repoTypeToResolvePrefix = map[string]string{
	"model":   "",
	"dataset": "datasets/",
	"space":   "spaces/",
}

func normalizeEndpoint(rawValue, defaultValue string) (string, *url.URL, error) {
	endpoint := strings.TrimSpace(rawValue)
	if endpoint == "" {
		endpoint = defaultValue
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", nil, fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", nil, fmt.Errorf("invalid endpoint %q: scheme and host are required", endpoint)
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	normalized := strings.TrimSuffix(parsed.String(), "/")
	parsed, err = url.Parse(normalized)
	if err != nil {
		return "", nil, fmt.Errorf("invalid endpoint %q: %w", normalized, err)
	}
	return normalized, parsed, nil
}

func normalizeOptionalEndpoint(rawValue string) (string, *url.URL, error) {
	if strings.TrimSpace(rawValue) == "" {
		return "", nil, nil
	}
	return normalizeEndpoint(rawValue, "")
}

func isValidRepoType(repoType string) bool {
	_, ok := repoTypeToPlural[repoType]
	return ok
}

func isValidRepoID(repoID string) bool {
	parts := strings.Split(repoID, "/")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" || strings.Contains(part, " ") {
			return false
		}
	}
	return true
}

func storagePathToRepoPath(storagePath string) string {
	cleaned := utils.FixAndCleanPath(storagePath)
	if cleaned == "/" {
		return ""
	}
	return strings.TrimPrefix(cleaned, "/")
}

func entryName(entryPath string) string {
	name := stdpath.Base(entryPath)
	switch name {
	case "", ".", "/":
		return ""
	default:
		return name
	}
}

func parseNextPageURL(linkHeader string, currentURL string) string {
	if linkHeader == "" {
		return ""
	}
	parts := strings.Split(linkHeader, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if !(strings.Contains(part, `rel="next"`) || strings.Contains(part, "rel=next")) {
			continue
		}
		start := strings.Index(part, "<")
		end := strings.Index(part, ">")
		if start < 0 || end <= start+1 {
			return ""
		}
		nextURL := part[start+1 : end]
		nextParsed, err := url.Parse(nextURL)
		if err != nil {
			return ""
		}
		currentParsed, err := url.Parse(currentURL)
		if err != nil {
			return nextURL
		}
		return currentParsed.ResolveReference(nextParsed).String()
	}
	return ""
}

func parseHFError(res *resty.Response) error {
	errorCode := res.Header().Get("X-Error-Code")
	errorMessage := strings.TrimSpace(res.Header().Get("X-Error-Message"))

	if errorMessage == "" && len(res.Body()) > 0 {
		var payload HFErrorResp
		if err := utils.Json.Unmarshal(res.Body(), &payload); err == nil {
			errorMessage = strings.TrimSpace(payload.Error)
		}
	}
	if errorMessage == "" {
		errorMessage = strings.TrimSpace(res.Status())
	}

	if errorCode == "EntryNotFound" {
		return errs.ObjectNotFound
	}
	if errorCode == "RevisionNotFound" {
		return fmt.Errorf("%w: %s", errHFRevisionNotFound, errorMessage)
	}
	if res.StatusCode() == http.StatusUnauthorized || res.StatusCode() == http.StatusForbidden {
		return fmt.Errorf("%w: %s", errHFAccessDenied, errorMessage)
	}
	return fmt.Errorf("%s: %s", res.Status(), errorMessage)
}

func endpointHostEqual(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(a.Hostname(), b.Hostname()) && normalizedPort(a) == normalizedPort(b)
}

func normalizedPort(u *url.URL) string {
	if u == nil {
		return ""
	}
	if p := u.Port(); p != "" {
		return p
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	default:
		return ""
	}
}
