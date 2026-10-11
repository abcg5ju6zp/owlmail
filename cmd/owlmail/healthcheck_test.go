package main

import (
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/soulteary/owlmail/internal/config"
)

func TestContainerServerArgs(t *testing.T) {
	args, err := containerServerArgs([]byte("/app/owlmail\x00-web\x009999\x00-config\x00/app/config.yaml\x00"))
	if err != nil || len(args) != 4 || args[1] != "9999" {
		t.Fatalf("args=%v err=%v", args, err)
	}
	for _, command := range []string{"/app/owlmail\x00doctor\x00", "/app/owlmail\x00mcp-stdio\x00", "/bin/sh\x00", ""} {
		if _, err := containerServerArgs([]byte(command)); err == nil {
			t.Fatalf("accepted non-server command %q", command)
		}
	}
}

func TestProbeHealth(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(strconv.FormatBool(secure), func(t *testing.T) {
			status := http.StatusOK
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/mail/healthz" {
					t.Errorf("path=%s", r.URL.Path)
				}
				w.WriteHeader(status)
			}))
			if secure {
				server.StartTLS()
			} else {
				server.Start()
			}
			defer server.Close()
			host, port, _ := net.SplitHostPort(server.Listener.Addr().String())
			cfg := config.DefaultConfig()
			cfg.WebHost = host
			cfg.WebPort, _ = strconv.Atoi(port)
			cfg.BasePathname = "/mail"
			cfg.HTTPSEnabled = secure
			if secure {
				pair := server.TLS.Certificates[0]
				cfg.HTTPSCertFile = filepath.Join(t.TempDir(), "cert.pem")
				if err := os.WriteFile(cfg.HTTPSCertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pair.Certificate[0]}), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := probeHealth(cfg); err != nil {
				t.Fatal(err)
			}
			status = http.StatusServiceUnavailable
			if err := probeHealth(cfg); err == nil {
				t.Fatal("accepted unhealthy response")
			}
			status = http.StatusFound
			if err := probeHealth(cfg); err == nil {
				t.Fatal("accepted redirect")
			}
		})
	}
}

func TestHealthcheckConfigUsesStartupPrecedence(t *testing.T) {
	t.Setenv("OWLMAIL_WEB_PORT", "9001")
	cfg, err := commandConfig("healthcheck", []string{"-web", "9002", "-base-pathname", "/mail"})
	if err != nil || cfg.WebPort != 9002 || cfg.BasePathname != "/mail" {
		t.Fatalf("config=%+v err=%v", cfg, err)
	}
}

func TestHealthcheckConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("web: 9003\nbase-pathname: /configured\nhttps: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := commandConfig("healthcheck", []string{"-config", path})
	if err != nil || cfg.WebPort != 9003 || !cfg.HTTPSEnabled || cfg.BasePathname != "/configured" {
		t.Fatalf("config=%+v err=%v", cfg, err)
	}
}
