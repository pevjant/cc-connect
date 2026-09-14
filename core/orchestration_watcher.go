package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Orchestration watch lifecycle. registered/waiting watches survive daemon
// restarts (reloaded from disk); completed means work settled and the
// completion is pending delivery to the original session; delivered/failed
// are terminal.
const (
	WatchRegistered = "registered"
	WatchWaiting    = "waiting"
	WatchCompleted  = "completed"
	WatchDelivered  = "delivered"
	WatchFailed     = "failed"
	WatchCancelled  = "cancelled"
)

const (
	orchestrationWaitTimeoutMS = 900000 // 15 min per check --wait, mirroring the coordinator loop
	orchestrationDeliveryMax   = time.Hour
)

// orchestrationRetryInterval is a var so tests can shorten the busy-session
// redelivery backoff.
var orchestrationRetryInterval = 10 * time.Second

// watchIDRe constrains externally supplied ids before they reach exec argv
// or file paths (fixed argv arrays only, never shell strings).
var watchIDRe = regexp.MustCompile(`^[A-Za-z0-9._:@/-]{1,200}$`)

// OrchestrationDelivery is one settled orca message kept for crash-safe
// redelivery: persisted before the orca batch is acknowledged and cleared
// only after the completion turn was started in the session.
type OrchestrationDelivery struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	TaskID     string `json:"task_id,omitempty"`
	RawJSON    string `json:"raw_json"`
	ReceivedAt string `json:"received_at"`
}

// OrchestrationWatch tracks one orca Run on behalf of a Discord session.
// All field access is guarded by the owning manager's mutex; the runner
// never holds that mutex across blocking calls (orca invocations, injection
// turns).
type OrchestrationWatch struct {
	ID             string                 `json:"id"`
	Project        string                 `json:"project"`
	SessionKey     string                 `json:"session_key"`
	RunID          string                 `json:"run_id"`
	TaskIDs        []string               `json:"task_ids"`
	Status         string                 `json:"status"`
	SeenDeliveries map[string]bool        `json:"seen_deliveries"`
	SettledTasks   map[string]bool        `json:"settled_tasks"`
	Pending        *OrchestrationDelivery `json:"pending,omitempty"`
	LastHeartbeat  string                 `json:"last_heartbeat,omitempty"`
	LastError      string                 `json:"last_error,omitempty"`
	CreatedAt      string                 `json:"created_at"`
	UpdatedAt      string                 `json:"updated_at"`
}

func (w *OrchestrationWatch) clone() *OrchestrationWatch {
	c := *w
	c.TaskIDs = append([]string(nil), w.TaskIDs...)
	c.SeenDeliveries = make(map[string]bool, len(w.SeenDeliveries))
	for k, v := range w.SeenDeliveries {
		c.SeenDeliveries[k] = v
	}
	c.SettledTasks = make(map[string]bool, len(w.SettledTasks))
	for k, v := range w.SettledTasks {
		c.SettledTasks[k] = v
	}
	if w.Pending != nil {
		pendingCopy := *w.Pending
		c.Pending = &pendingCopy
	}
	return &c
}

var ErrOrchestrationSessionBusy = errors.New("orchestration: session busy, retry later")

// OrchestrationManager supervises daemon-side watchers that consume
// `orca orchestration check --wait` for a Run so the coordinator's Codex
// turn never blocks on the wait. Settled deliveries are re-entered into the
// original Discord session as a new agent turn.
type OrchestrationManager struct {
	dataDir string
	path    string

	mu      sync.Mutex
	watches map[string]*OrchestrationWatch // keyed by RunID
	cancels map[string]context.CancelFunc
	engines map[string]*Engine

	// injectFn delivers a completion prompt into a session. Default resolves
	// the project engine and starts a turn; tests swap it out.
	injectFn func(project, sessionKey, content string) error
	// checkRunner runs one `orca orchestration check` invocation (args are
	// the subcommand and flags, without the binary). Default execs orca;
	// tests inject canned stdout.
	checkRunner func(ctx context.Context, args []string) ([]byte, error)

	baseCtx context.Context
	stop    context.CancelFunc
	wg      sync.WaitGroup
}

func NewOrchestrationManager(dataDir string) *OrchestrationManager {
	ctx, stop := context.WithCancel(context.Background())
	m := &OrchestrationManager{
		dataDir: dataDir,
		path:    filepath.Join(dataDir, "orchestration-watches.json"),
		watches: make(map[string]*OrchestrationWatch),
		cancels: make(map[string]context.CancelFunc),
		engines: make(map[string]*Engine),
		baseCtx: ctx,
		stop:    stop,
	}
	m.checkRunner = func(ctx context.Context, args []string) ([]byte, error) {
		bin := os.Getenv("ORCA_CLI_COMMAND")
		if bin == "" {
			bin = "orca"
		}
		return runCommandOutput(ctx, append([]string{bin}, args...))
	}
	m.injectFn = m.defaultInject
	return m
}

func (m *OrchestrationManager) RegisterEngine(name string, e *Engine) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.engines[name] = e
}

// ── registration / status / cancel ────────────────────────────

// RegisterWatch creates or idempotently updates a watch for runID and starts
// (or resumes) its runner. deduplicated reports whether the watch already
// existed.
func (m *OrchestrationManager) RegisterWatch(project, sessionKey, runID string, taskIDs []string) (*OrchestrationWatch, bool, error) {
	if !watchIDRe.MatchString(runID) {
		return nil, false, fmt.Errorf("invalid run_id")
	}
	if !watchIDRe.MatchString(sessionKey) {
		return nil, false, fmt.Errorf("invalid session_key")
	}
	for _, t := range taskIDs {
		if !watchIDRe.MatchString(t) {
			return nil, false, fmt.Errorf("invalid task id %q", t)
		}
	}

	m.mu.Lock()
	existing, ok := m.watches[runID]
	if ok {
		for _, t := range taskIDs {
			if !containsStr(existing.TaskIDs, t) {
				existing.TaskIDs = append(existing.TaskIDs, t)
			}
		}
		existing.UpdatedAt = nowRFC3339()
		startRunnerLocked(m, existing)
		snapshot := existing.clone()
		m.mu.Unlock()
		m.persist()
		return snapshot, true, nil
	}

	now := time.Now().UTC().Format(time.RFC3339)
	w := &OrchestrationWatch{
		ID:             fmt.Sprintf("watch_%d", time.Now().UnixNano()),
		Project:        project,
		SessionKey:     sessionKey,
		RunID:          runID,
		TaskIDs:        append([]string(nil), taskIDs...),
		Status:         WatchRegistered,
		SeenDeliveries: map[string]bool{},
		SettledTasks:   map[string]bool{},
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	m.watches[runID] = w
	startRunnerLocked(m, w)
	snapshot := w.clone()
	m.mu.Unlock()

	m.persist()
	slog.Info("orchestration watch registered", "watch_id", w.ID, "run_id", runID,
		"project", project, "session_key", sessionKey, "tasks", len(taskIDs))
	return snapshot, false, nil
}

func (m *OrchestrationManager) GetWatch(runID string) *OrchestrationWatch {
	m.mu.Lock()
	defer m.mu.Unlock()
	if w, ok := m.watches[runID]; ok {
		return w.clone()
	}
	return nil
}

func (m *OrchestrationManager) ListWatches() []*OrchestrationWatch {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*OrchestrationWatch, 0, len(m.watches))
	for _, w := range m.watches {
		out = append(out, w.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out
}

func (m *OrchestrationManager) CancelWatch(runID string) error {
	m.mu.Lock()
	w, ok := m.watches[runID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("watch %q not found", runID)
	}
	if cancel, running := m.cancels[runID]; running {
		cancel()
		delete(m.cancels, runID)
	}
	w.Status = WatchCancelled
	w.UpdatedAt = nowRFC3339()
	m.mu.Unlock()
	m.persist()
	return nil
}

// Resume loads persisted watches and restarts runners for live ones plus
// delivery retries for settled-but-undelivered completions. A corrupted
// store is reported and left untouched — never overwritten.
func (m *OrchestrationManager) Resume() error {
	data, err := os.ReadFile(m.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read orchestration watches: %w", err)
	}
	var watches map[string]*OrchestrationWatch
	if err := json.Unmarshal(data, &watches); err != nil {
		return fmt.Errorf("orchestration watch store %s is corrupted (left in place): %w", m.path, err)
	}

	m.mu.Lock()
	m.watches = watches
	for _, w := range m.watches {
		startRunnerLocked(m, w)
	}
	count := len(m.watches)
	m.mu.Unlock()

	slog.Info("orchestration watches loaded", "count", count)
	return nil
}

func (m *OrchestrationManager) Stop() {
	m.stop()
	m.wg.Wait()
}

// ── runner ────────────────────────────────────────────────────

// startRunnerLocked launches the runner goroutine for watches that still
// need work. Callers hold m.mu.
func startRunnerLocked(m *OrchestrationManager, w *OrchestrationWatch) {
	if _, running := m.cancels[w.RunID]; running {
		return
	}
	needsRunner := w.Status == WatchRegistered || w.Status == WatchWaiting ||
		(w.Status == WatchCompleted && w.Pending != nil)
	if !needsRunner {
		return
	}
	ctx, cancel := context.WithCancel(m.baseCtx)
	m.cancels[w.RunID] = cancel
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			m.mu.Lock()
			delete(m.cancels, w.RunID)
			m.mu.Unlock()
		}()
		m.runWatch(ctx, w)
	}()
}

// runWatch consumes the run's coordinator inbox with rolling
// `check --wait` calls, acknowledging each processed batch. Heartbeats only
// refresh state; worker_done/escalation/question are persisted as pending
// and re-entered into the session as a new agent turn. The runner exits
// when every registered task is settled (or on cancel/stop).
func (m *OrchestrationManager) runWatch(ctx context.Context, w *OrchestrationWatch) {
	m.mu.Lock()
	w.Status = WatchWaiting
	w.UpdatedAt = nowRFC3339()
	m.mu.Unlock()

	backoff := 2 * time.Second
	var lastAck string
	for {
		if ctx.Err() != nil {
			return
		}
		args := []string{
			"orchestration", "check", "--wait",
			"--run", w.RunID, "--json",
			"--timeout-ms", fmt.Sprint(orchestrationWaitTimeoutMS),
		}
		if lastAck != "" {
			args = append(args, "--ack", lastAck)
		}
		out, err := m.checkRunner(ctx, args)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.mu.Lock()
			w.LastError = err.Error()
			m.mu.Unlock()
			slog.Warn("orchestration watch retrying", "run_id", w.RunID, "error", err, "backoff", backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff *= 2
			if backoff > time.Minute {
				backoff = time.Minute
			}
			continue
		}
		backoff = 2 * time.Second

		deliveries, perr := extractCheckDeliveries(out)
		if perr != nil {
			// Unparseable output is treated as transient (version drift):
			// the batch stays unacked and replays on the next check.
			m.mu.Lock()
			w.LastError = perr.Error()
			m.mu.Unlock()
			slog.Warn("orchestration watch unparseable check output", "run_id", w.RunID, "error", perr)
			continue
		}
		if len(deliveries) == 0 {
			continue // wait timeout — checkpoint, keep rolling
		}

		for _, d := range deliveries {
			if ctx.Err() != nil {
				return
			}
			m.mu.Lock()
			id := firstString(d, "id", "message_id", "delivery_id")
			kind := firstString(d, "type", "kind")
			if kind == "" {
				kind = "unknown"
			}
			if id == "" || w.SeenDeliveries[id] {
				m.mu.Unlock()
				continue
			}
			w.SeenDeliveries[id] = true
			w.UpdatedAt = nowRFC3339()
			taskID := firstString(d, "task_id", "task")
			if kind == "heartbeat" {
				w.LastHeartbeat = w.UpdatedAt
			}
			if kind == "worker_done" && taskID != "" {
				w.SettledTasks[taskID] = true
			}
			m.mu.Unlock()
			lastAck = id

			if kind == "heartbeat" {
				m.persist()
				slog.Info("orchestration heartbeat received", "run_id", w.RunID, "delivery_id", id)
				continue
			}

			raw, _ := json.Marshal(d)
			delivery := &OrchestrationDelivery{
				ID: id, Kind: kind, TaskID: taskID,
				RawJSON:    string(raw),
				ReceivedAt: nowRFC3339(),
			}
			// Persist as pending BEFORE acknowledging so a crash cannot lose
			// the completion: orca would replay an unacked batch, but after
			// ack our store owns redelivery.
			m.mu.Lock()
			w.Pending = delivery
			m.mu.Unlock()
			m.persist()
			m.ackBatch(ctx, w, id)
			m.deliverPending(ctx, w)
			if watchIsTerminal(m, w) {
				return
			}
		}

		if watchAllTasksSettled(m, w) {
			m.mu.Lock()
			settled := len(w.SettledTasks)
			m.mu.Unlock()
			slog.Info("orchestration watch settled", "run_id", w.RunID, "tasks", settled)
			return
		}
	}
}

// deliverPending injects the pending completion into the session, retrying
// while a foreground turn holds the session lock. User messages always win.
func (m *OrchestrationManager) deliverPending(ctx context.Context, w *OrchestrationWatch) {
	m.mu.Lock()
	w.Status = WatchCompleted
	w.UpdatedAt = nowRFC3339()
	pending := w.Pending
	m.mu.Unlock()
	if pending == nil {
		return
	}

	deadline := time.Now().Add(orchestrationDeliveryMax)
	for {
		if ctx.Err() != nil {
			return
		}
		err := m.injectFn(w.Project, w.SessionKey, buildCompletionPrompt(w, pending))
		if err == nil {
			m.mu.Lock()
			w.Pending = nil
			w.Status = WatchDelivered
			w.UpdatedAt = nowRFC3339()
			deliveryID := pending.ID
			m.mu.Unlock()
			m.persist()
			slog.Info("orchestration completion delivered", "run_id", w.RunID,
				"delivery_id", deliveryID, "session_key", w.SessionKey)
			return
		}
		if !errors.Is(err, ErrOrchestrationSessionBusy) {
			m.mu.Lock()
			w.LastError = err.Error()
			w.Status = WatchFailed
			w.UpdatedAt = nowRFC3339()
			m.mu.Unlock()
			m.persist()
			slog.Error("orchestration completion inject failed", "run_id", w.RunID, "error", err)
			return
		}
		slog.Debug("orchestration completion deferred, session busy", "run_id", w.RunID)

		if time.Now().After(deadline) {
			m.mu.Lock()
			w.LastError = "delivery timed out; completion kept in watch store for manual recovery"
			w.Status = WatchFailed
			w.UpdatedAt = nowRFC3339()
			m.mu.Unlock()
			m.persist()
			return
		}
		if !sleepCtx(ctx, orchestrationRetryInterval) {
			return
		}
	}
}

func watchIsTerminal(m *OrchestrationManager, w *OrchestrationWatch) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch w.Status {
	case WatchDelivered, WatchFailed, WatchCancelled:
		return true
	}
	return false
}

func watchAllTasksSettled(m *OrchestrationManager, w *OrchestrationWatch) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(w.TaskIDs) == 0 {
		return false // no task list: watch runs until cancelled
	}
	for _, t := range w.TaskIDs {
		if !w.SettledTasks[t] {
			return false
		}
	}
	return true
}

// ── injection into the session ────────────────────────────────

func (m *OrchestrationManager) defaultInject(project, sessionKey, content string) error {
	m.mu.Lock()
	engine, ok := m.engines[project]
	if !ok && len(m.engines) == 1 {
		// Fall back to the single configured engine, mirroring the API
		// server's project resolution.
		for _, e := range m.engines {
			engine = e
			ok = true
		}
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("project %q not found", project)
	}
	return engine.injectOrchestrationCompletion(sessionKey, content)
}

// injectOrchestrationCompletion starts a new agent turn in sessionKey with
// the completion prompt — the same path cron prompts take. It refuses while
// a foreground turn holds the session lock; the watcher retries.
func (e *Engine) injectOrchestrationCompletion(sessionKey, content string) error {
	iKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state := e.interactiveStates[iKey]
	e.interactiveMu.Unlock()

	var platform Platform
	var replyCtx any
	if state != nil {
		state.mu.Lock()
		platform = state.platform
		replyCtx = state.replyCtx
		state.mu.Unlock()
	}
	if platform == nil {
		var err error
		platform, err = e.platformForSessionKey(sessionKey)
		if err != nil {
			return err
		}
	}
	if replyCtx == nil {
		rc, ok := platform.(ReplyContextReconstructor)
		if !ok {
			return fmt.Errorf("platform %q cannot reconstruct reply context", platform.Name())
		}
		reconstructed, err := rc.ReconstructReplyCtx(sessionKey)
		if err != nil {
			return fmt.Errorf("reconstruct reply context: %w", err)
		}
		replyCtx = reconstructed
	}

	session := e.sessions.GetOrCreateActive(sessionKey)
	if !session.TryLock() {
		return ErrOrchestrationSessionBusy
	}

	msg := &Message{
		SessionKey: sessionKey,
		Platform:   platform.Name(),
		UserID:     "orchestration",
		UserName:   "orchestration",
		Content:    content,
		ReplyCtx:   replyCtx,
	}
	slog.Info("orchestration completion queued", "session_key", sessionKey)
	e.processInteractiveMessageWith(platform, msg, session, e.agent, e.sessions, iKey, "", sessionKey)
	return nil
}

// platformForSessionKey resolves a platform by the session key's prefix,
// mirroring ExecuteCronJob (including workspace-prefixed keys).
func (e *Engine) platformForSessionKey(sessionKey string) (Platform, error) {
	platformName := ""
	if idx := strings.Index(sessionKey, ":"); idx > 0 {
		platformName = sessionKey[:idx]
	}
	for _, p := range e.platforms {
		if p.Name() == platformName {
			return p, nil
		}
	}
	for _, p := range e.platforms {
		if idx := strings.Index(sessionKey, ":"+p.Name()+":"); idx >= 0 {
			return p, nil
		}
	}
	return nil, fmt.Errorf("platform %q not found for session %q", platformName, sessionKey)
}

// ── orca invocation ───────────────────────────────────────────

func runCommandOutput(ctx context.Context, args []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr // orca --wait keepalive lines land here, never in stdout
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// extractCheckDeliveries parses `orca orchestration check --json` stdout.
// The envelope is {ok, result, error}; delivery rows live somewhere inside
// result and field names drift across orca versions, so rows are matched
// defensively: any object carrying an id-like and a type-like field.
func extractCheckDeliveries(stdout []byte) ([]map[string]any, error) {
	var envelope struct {
		OK     *bool `json:"ok"`
		Result any   `json:"result"`
		Error  *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(stdout, &envelope); err != nil {
		return nil, fmt.Errorf("decode check output: %w", err)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("orca: %s: %s", envelope.Error.Code, envelope.Error.Message)
	}
	if envelope.OK != nil && !*envelope.OK {
		return nil, fmt.Errorf("orca check failed")
	}

	var deliveries []map[string]any
	var walk func(v any)
	walk = func(v any) {
		switch tv := v.(type) {
		case []any:
			for _, item := range tv {
				walk(item)
			}
		case map[string]any:
			if isDeliveryRow(tv) {
				deliveries = append(deliveries, tv)
				return
			}
			for _, item := range tv {
				walk(item)
			}
		}
	}
	walk(envelope.Result)
	return deliveries, nil
}

func isDeliveryRow(m map[string]any) bool {
	return firstString(m, "id", "message_id", "delivery_id") != "" &&
		firstString(m, "type", "kind") != ""
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func containsStr(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

// ── persistence ───────────────────────────────────────────────

func (m *OrchestrationManager) persist() {
	if err := m.save(); err != nil {
		slog.Warn("orchestration: persist failed", "error", err)
	}
}

// save writes the store atomically: clone every watch under m.mu, then
// write+rename outside the lock.
func (m *OrchestrationManager) save() error {
	m.mu.Lock()
	clones := make(map[string]*OrchestrationWatch, len(m.watches))
	for runID, w := range m.watches {
		clones[runID] = w.clone()
	}
	m.mu.Unlock()

	data, err := json.MarshalIndent(clones, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.path), 0o755); err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}

// ── helpers ───────────────────────────────────────────────────

func (m *OrchestrationManager) ackBatch(ctx context.Context, w *OrchestrationWatch, deliveryID string) {
	args := []string{"orchestration", "check", "--run", w.RunID, "--json", "--ack", deliveryID}
	if _, err := m.checkRunner(ctx, args); err != nil {
		// An unacked batch replays, but persistence already owns redelivery
		// once pending is saved — ack failure only risks a stale batch
		// surfacing in the agent's next manual check.
		slog.Warn("orchestration ack failed", "run_id", w.RunID, "delivery_id", deliveryID, "error", err)
	}
}

func buildCompletionPrompt(w *OrchestrationWatch, d *OrchestrationDelivery) string {
	var b strings.Builder
	switch d.Kind {
	case "escalation":
		b.WriteString("[orchestration] Orca worker ESCALATION received — this needs your attention now.\n\n")
	case "question":
		b.WriteString("[orchestration] Orca worker QUESTION received — answer it via `orca orchestration reply --id <message_id> --body \"<answer>\" --json`.\n\n")
	default:
		b.WriteString("[orchestration] Orca orchestration completion received.\n\n")
	}
	fmt.Fprintf(&b, "Run: %s\n", w.RunID)
	if d.TaskID != "" {
		fmt.Fprintf(&b, "Task: %s\n", d.TaskID)
	}
	fmt.Fprintf(&b, "Delivery: %s\nType: %s\n", d.ID, d.Kind)
	fmt.Fprintf(&b, "\nPayload:\n%s\n", d.RawJSON)
	b.WriteString(`
This is an asynchronous completion for a worker you dispatched earlier; the user was NOT blocked while it ran.
- Validate the result (files, reports, tests) and continue the original workflow, then report to the user in this channel.
- Do NOT start another orchestration wait for this delivery.`)
	return b.String()
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
