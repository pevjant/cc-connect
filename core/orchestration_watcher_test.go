package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func heartbeatJSON(id string) string {
	return `{"ok":true,"result":{"deliveries":[{"id":"` + id + `","type":"heartbeat","run_id":"run_test"}]}}`
}

func workerDoneJSON(id, taskID string) string {
	return `{"ok":true,"result":{"deliveries":[{"id":"` + id + `","type":"worker_done","run_id":"run_test","task_id":"` + taskID + `","summary":"all green"}]}}`
}

func TestOrchestrationWatchPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m := NewOrchestrationManager(dir)
	w, deduplicated, err := m.RegisterWatch("proj", "discord:chan:user", "run_r1", []string{"task_a"})
	if err != nil || deduplicated {
		t.Fatalf("register: dedup=%v err=%v", deduplicated, err)
	}
	_ = w

	restored := NewOrchestrationManager(dir)
	if err := restored.Resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	got := restored.GetWatch("run_r1")
	if got == nil {
		t.Fatal("watch not found after resume")
	}
	if got.SessionKey != "discord:chan:user" || got.Project != "proj" {
		t.Fatalf("watch fields lost: %+v", got)
	}
	if len(got.TaskIDs) != 1 || got.TaskIDs[0] != "task_a" {
		t.Fatalf("task ids lost: %v", got.TaskIDs)
	}

	// Idempotent re-registration unions task ids and reports dedup.
	updated, dedup, err := restored.RegisterWatch("proj", "discord:chan:user", "run_r1", []string{"task_b"})
	if err != nil || !dedup {
		t.Fatalf("re-register: dedup=%v err=%v", dedup, err)
	}
	if len(updated.TaskIDs) != 2 {
		t.Fatalf("task ids not unioned: %v", updated.TaskIDs)
	}
}

func TestOrchestrationWatchCorruptStoreNotOverwritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orchestration-watches.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := NewOrchestrationManager(dir)
	if err := m.Resume(); err == nil {
		t.Fatal("expected corruption error")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "{not json" {
		t.Fatal("corrupt store was overwritten")
	}
}

// TestOrchestrationRunnerHeartbeatsThenDone pins the runner loop: heartbeats
// refresh state without touching the session, worker_done persists as pending
// and is injected exactly once, and the batch is acknowledged before the next
// wait.
func TestOrchestrationRunnerHeartbeatsThenDone(t *testing.T) {
	dir := t.TempDir()
	m := NewOrchestrationManager(dir)

	oldRetry := orchestrationRetryInterval
	orchestrationRetryInterval = 20 * time.Millisecond
	defer func() { orchestrationRetryInterval = oldRetry }()

	var calls [][]string
	var waitCalls int
	var injectCalls int
	var injectFailures int
	injected := make(chan string, 4)

	m.checkRunner = func(ctx context.Context, args []string) ([]byte, error) {
		calls = append(calls, args)
		hasWait, hasAck := false, false
		for _, a := range args {
			switch a {
			case "--wait":
				hasWait = true
			case "--ack":
				hasAck = true
			}
		}
		if hasAck && !hasWait {
			// Pure ack call: acknowledges the prior batch, returns nothing.
			return []byte(`{"ok":true,"result":null}`), nil
		}
		waitCalls++
		switch waitCalls {
		case 1, 2:
			return []byte(heartbeatJSON(fmt.Sprint("hb-", waitCalls))), nil
		default:
			return []byte(workerDoneJSON("done-1", "task_a")), nil
		}
	}
	m.injectFn = func(project, sessionKey, content string) error {
		// Simulate a foreground turn holding the session on the first try.
		if injectFailures > 0 {
			injectCalls++
			injected <- content
			return nil
		}
		injectFailures++
		injectCalls++
		return ErrOrchestrationSessionBusy
	}

	if _, _, err := m.RegisterWatch("proj", "discord:chan:user", "run_test", []string{"task_a"}); err != nil {
		t.Fatal(err)
	}

	select {
	case content := <-injected:
		if !strings.Contains(content, "worker_done") || !strings.Contains(content, "run_test") {
			t.Fatalf("prompt missing completion details:\n%s", content)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("completion was never injected")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		w := m.GetWatch("run_test")
		if w != nil && w.Status == WatchDelivered {
			break
		}
		if time.Now().After(deadline) {
			w := m.GetWatch("run_test")
			status := "nil"
			if w != nil {
				status = w.Status
			}
			t.Fatalf("watch not delivered, status=%s", status)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The first injection was deferred (busy), the second succeeded.
	if injectCalls != 2 {
		t.Fatalf("inject calls = %d, want 2 (1 busy + 1 success)", injectCalls)
	}
	// The wait calls after each processed batch carry the ack flag.
	ackCalls := 0
	for _, c := range calls {
		for _, a := range c {
			if a == "--ack" {
				ackCalls++
				break
			}
		}
	}
	if ackCalls == 0 {
		t.Fatal("no check call carried --ack")
	}
	m.Stop()
}

// TestOrchestrationStallNotification pins the second-line verification: two
// consecutive empty waits produce a stall warning injected into the session
// (with a worker-list snapshot), and a heartbeat resets the stall counter.
func TestOrchestrationStallNotification(t *testing.T) {
	dir := t.TempDir()
	m := NewOrchestrationManager(dir)

	oldRetry := orchestrationRetryInterval
	orchestrationRetryInterval = 20 * time.Millisecond
	defer func() { orchestrationRetryInterval = oldRetry }()

	var waitCalls int
	var injected []string
	m.checkRunner = func(ctx context.Context, args []string) ([]byte, error) {
		hasWait, hasAck := false, false
		for _, a := range args {
			switch a {
			case "--wait":
				hasWait = true
			case "--ack":
				hasAck = true
			}
		}
		if hasAck && !hasWait {
			return []byte(`{"ok":true,"result":null}`), nil
		}
		for _, a := range args {
			if a == "worker-list" {
				return []byte(`{"ok":true,"result":{"rows":[{"id":"dispatch_1","type":"liveness","status":"exited"}]}}`), nil
			}
		}
		waitCalls++
		return []byte(`{"ok":true,"result":{"deliveries":[]}}`), nil // empty batch
	}
	m.injectFn = func(project, sessionKey, content string) error {
		injected = append(injected, content)
		return nil
	}

	if _, _, err := m.RegisterWatch("proj", "discord:chan", "run_stall", []string{"task_never"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(injected) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("stall warning never injected; waitCalls=%d", waitCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(injected[0], "stall warning") || !strings.Contains(injected[0], "run_stall") || !strings.Contains(injected[0], "exited") {
		t.Fatalf("stall prompt missing details:\n%s", injected[0])
	}
	m.Stop()
}

func TestExtractCheckDeliveries(t *testing.T) {
	deliveries, err := extractCheckDeliveries([]byte(workerDoneJSON("d-1", "t-9")))
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("deliveries = %d, want 1", len(deliveries))
	}
	if firstString(deliveries[0], "id") != "d-1" || firstString(deliveries[0], "task_id") != "t-9" {
		t.Fatalf("row fields lost: %v", deliveries[0])
	}

	_, err = extractCheckDeliveries([]byte(`{"ok":false,"error":{"code":"runtime_unavailable","message":"no orca"}}`))
	if err == nil {
		t.Fatal("expected error for !ok envelope")
	}
}

func TestBuildCompletionPromptContent(t *testing.T) {
	m := NewOrchestrationManager(t.TempDir())
	w, _, err := m.RegisterWatch("proj", "discord:chan", "run_x", []string{"task_x"})
	if err != nil {
		t.Fatal(err)
	}
	d := &OrchestrationDelivery{ID: "d-1", Kind: "worker_done", TaskID: "task_x", RawJSON: `{"summary":"done"}`}
	prompt := buildCompletionPrompt(w, d)
	for _, want := range []string{"run_x", "task_x", "d-1", "worker_done", `{"summary":"done"}`, "Do NOT start another orchestration wait"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(d.RawJSON), &parsed); err != nil || parsed["summary"] != "done" {
		t.Fatalf("payload json invalid: %v", err)
	}
}
