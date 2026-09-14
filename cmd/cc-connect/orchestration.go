package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/chenhg5/cc-connect/core"
)

// runOrchestration implements `cc-connect orchestration watch|status|cancel`.
// The watch subcommand registers a daemon-side watcher and exits immediately
// — it never waits for the orca run itself.
func runOrchestration(args []string) {
	if len(args) == 0 {
		printOrchestrationUsage()
		os.Exit(1)
	}

	var payload any
	var method string
	var path string
	dataDir := resolveCCDataDir()

	req := core.OrchestrationWatchRequest{}
	switch args[0] {
	case "watch":
		method, path = "POST", "/orchestration/watch"
		if err := parseOrchestrationWatchArgs(args[1:], &req, &dataDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			printOrchestrationUsage()
			os.Exit(1)
		}
		if req.RunID == "" || req.SessionKey == "" {
			fmt.Fprintf(os.Stderr, "Error: --run and --session are required\n")
			printOrchestrationUsage()
			os.Exit(1)
		}
		payload = req
	case "status":
		method, path = "GET", "/orchestration/status"
		flags := parseOrchestrationSimpleArgs(args[1:], &dataDir)
		if run := flags["run"]; run != "" {
			path += "?run_id=" + urlQueryEscape(run)
		}
		payload = nil
	case "cancel":
		method, path = "POST", "/orchestration/cancel"
		flags := parseOrchestrationSimpleArgs(args[1:], &dataDir)
		if flags["run"] == "" {
			fmt.Fprintf(os.Stderr, "Error: --run is required\n")
			os.Exit(1)
		}
		payload = map[string]string{"run_id": flags["run"]}
	default:
		fmt.Fprintf(os.Stderr, "Unknown orchestration command: %s\n", args[0])
		printOrchestrationUsage()
		os.Exit(1)
	}

	sockPath := resolveSocketPath(dataDir)
	if _, err := os.Stat(sockPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: cc-connect is not running (socket not found: %s)\n", sockPath)
		os.Exit(1)
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sockPath)
			},
		},
	}

	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: encode request: %v\n", err)
			os.Exit(1)
		}
		body = bytes.NewReader(encoded)
	}
	httpReq, err := http.NewRequest(method, "http://unix"+path, body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if payload != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(respBody)))
		os.Exit(1)
	}

	fmt.Println(strings.TrimSpace(string(respBody)))
}

func printOrchestrationUsage() {
	fmt.Fprint(os.Stderr, `Usage: cc-connect orchestration <watch|status|cancel> [flags]

watch:
  cc-connect orchestration watch --run <run_id> --session <session_key> [--task <task_id>]... [--project <name>]
  Registers a daemon-side watcher for the orca run and returns immediately.
  The watcher delivers worker_done/escalation back into the session as a new
  turn without holding the session lock.

status:
  cc-connect orchestration status [--run <run_id>]

cancel:
  cc-connect orchestration cancel --run <run_id>

Shared flags: --data-dir <dir> (default ~/.cc-connect), --project <name>
(session_key auto-detection requires a single active session when omitted
from the watch request handled server-side; --session is the cc-connect
session key, e.g. discord:channel:user)
`)
}

func parseOrchestrationWatchArgs(args []string, req *core.OrchestrationWatchRequest, dataDir *string) error {
	for i := 0; i < len(args); i++ {
		need := func() (string, error) {
			i++
			if i >= len(args) {
				return "", fmt.Errorf("%s requires a value", args[i-1])
			}
			return args[i], nil
		}
		switch args[i] {
		case "--project", "-p":
			v, err := need()
			if err != nil {
				return err
			}
			req.Project = v
		case "--session", "-s":
			v, err := need()
			if err != nil {
				return err
			}
			req.SessionKey = v
		case "--run":
			v, err := need()
			if err != nil {
				return err
			}
			req.RunID = v
		case "--task":
			v, err := need()
			if err != nil {
				return err
			}
			req.TaskIDs = append(req.TaskIDs, v)
		case "--data-dir":
			v, err := need()
			if err != nil {
				return err
			}
			*dataDir = v
		default:
			return fmt.Errorf("unknown flag %q", args[i])
		}
	}
	return nil
}

func parseOrchestrationSimpleArgs(args []string, dataDir *string) map[string]string {
	flags := map[string]string{}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--run":
			if i+1 < len(args) {
				i++
				flags["run"] = args[i]
			}
		case "--project", "-p":
			if i+1 < len(args) {
				i++
				flags["project"] = args[i]
			}
		case "--data-dir":
			if i+1 < len(args) {
				i++
				*dataDir = args[i]
			}
		}
	}
	return flags
}

// resolveCCDataDir mirrors resolveSocketPath's fallback chain.
func resolveCCDataDir() string {
	if envDataDir := strings.TrimSpace(os.Getenv("CC_DATA_DIR")); envDataDir != "" {
		return envDataDir
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".cc-connect")
	}
	return ".cc-connect"
}

func urlQueryEscape(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "&", "%26"), "?", "%3F")
}
