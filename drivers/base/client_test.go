package base

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/caarlos0/env/v9"
	"github.com/go-resty/resty/v2"
)

func TestClientsUseConfiguredUserAgents(t *testing.T) {
	oldConf := conf.Conf
	oldUserAgent, oldUserAgentNT := UserAgent, UserAgentNT
	oldRestyClient, oldNoRedirectClient, oldHttpClient := RestyClient, NoRedirectClient, HttpClient
	t.Cleanup(func() {
		conf.Conf = oldConf
		UserAgent, UserAgentNT = oldUserAgent, oldUserAgentNT
		RestyClient, NoRedirectClient, HttpClient = oldRestyClient, oldNoRedirectClient, oldHttpClient
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.UserAgent()))
	}))
	t.Cleanup(server.Close)

	for _, tc := range []struct {
		name            string
		configJSON      string
		environment     map[string]string
		prefix          string
		wantUserAgent   string
		wantUserAgentNT string
	}{
		{
			name:            "legacy config",
			configJSON:      `{}`,
			wantUserAgent:   conf.DefaultUserAgent,
			wantUserAgentNT: conf.DefaultUserAgentNT,
		},
		{
			name:            "JSON config",
			configJSON:      `{"user_agent":"ConfiguredClient/1.0","user_agent_nt":"ConfiguredWindowsClient/1.0"}`,
			wantUserAgent:   "ConfiguredClient/1.0",
			wantUserAgentNT: "ConfiguredWindowsClient/1.0",
		},
		{
			name:            "empty config restores built-in defaults",
			configJSON:      `{"user_agent":"","user_agent_nt":""}`,
			wantUserAgent:   conf.DefaultUserAgent,
			wantUserAgentNT: conf.DefaultUserAgentNT,
		},
		{
			name:            "general UA only",
			configJSON:      `{"user_agent":"ConfiguredClient/1.0"}`,
			wantUserAgent:   "ConfiguredClient/1.0",
			wantUserAgentNT: conf.DefaultUserAgentNT,
		},
		{
			name:            "Windows UA only",
			configJSON:      `{"user_agent_nt":"ConfiguredWindowsClient/1.0"}`,
			wantUserAgent:   conf.DefaultUserAgent,
			wantUserAgentNT: "ConfiguredWindowsClient/1.0",
		},
		{
			name:       "environment overrides JSON",
			configJSON: `{"user_agent":"ConfiguredClient/1.0","user_agent_nt":"ConfiguredWindowsClient/1.0"}`,
			environment: map[string]string{
				"OPENLIST_USER_AGENT":    "EnvironmentClient/1.0",
				"OPENLIST_USER_AGENT_NT": "EnvironmentWindowsClient/1.0",
			},
			prefix:          "OPENLIST_",
			wantUserAgent:   "EnvironmentClient/1.0",
			wantUserAgentNT: "EnvironmentWindowsClient/1.0",
		},
		{
			name:       "environment without prefix",
			configJSON: `{}`,
			environment: map[string]string{
				"USER_AGENT":    "EnvironmentClient/1.0",
				"USER_AGENT_NT": "EnvironmentWindowsClient/1.0",
			},
			wantUserAgent:   "EnvironmentClient/1.0",
			wantUserAgentNT: "EnvironmentWindowsClient/1.0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf.Conf = conf.DefaultConfig(t.TempDir())
			if err := utils.Json.Unmarshal([]byte(tc.configJSON), conf.Conf); err != nil {
				t.Fatal(err)
			}
			if tc.environment != nil {
				if err := env.ParseWithOptions(conf.Conf, env.Options{
					Prefix:      tc.prefix,
					Environment: tc.environment,
				}); err != nil {
					t.Fatal(err)
				}
			}
			InitClient()
			t.Cleanup(HttpClient.CloseIdleConnections)

			if UserAgent != tc.wantUserAgent || UserAgentNT != tc.wantUserAgentNT {
				t.Fatalf("shared user agents = (%q, %q), want (%q, %q)",
					UserAgent, UserAgentNT, tc.wantUserAgent, tc.wantUserAgentNT)
			}

			for name, client := range map[string]*resty.Client{
				"shared":       RestyClient,
				"no redirect":  NoRedirectClient,
				"new instance": NewRestyClient(),
			} {
				t.Run(name, func(t *testing.T) {
					t.Cleanup(client.GetClient().CloseIdleConnections)
					for _, request := range []struct {
						name     string
						override string
						want     string
					}{
						{name: "default", want: tc.wantUserAgent},
						{name: "driver override", override: "DriverClient/1.0", want: "DriverClient/1.0"},
						{name: "Windows default", override: UserAgentNT, want: tc.wantUserAgentNT},
					} {
						t.Run(request.name, func(t *testing.T) {
							req := client.R()
							if request.override != "" {
								req.SetHeader("User-Agent", request.override)
							}
							res, err := req.Get(server.URL)
							if err != nil {
								t.Fatal(err)
							}
							if got := res.String(); got != request.want {
								t.Fatalf("upstream User-Agent = %q, want %q", got, request.want)
							}
						})
					}
				})
			}
		})
	}
}
