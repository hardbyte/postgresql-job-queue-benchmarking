// Scenario controls (CONTRIBUTING_ADAPTERS.md "Scenario controls").
//
// Default-off: JOB_FAILURE_CONTROL_FILE tags jobs enqueued while a failure
// plan is active as transient/poison failures; SCHEDULE_CONTROL_FILE asks
// instance 0 to enqueue a scheduled herd. Untagged jobs keep their payload
// and code path unchanged.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const herdBatchMax = 500

type failurePlan struct {
	TransientPct      float64 `json:"transient_pct"`
	PoisonPct         float64 `json:"poison_pct"`
	TransientFailures int     `json:"transient_failures"`
}

type herdCommand struct {
	ID       string `json:"id"`
	Count    int    `json:"count"`
	RunAtMs  int64  `json:"run_at_ms"`
	SpreadMs int64  `json:"spread_ms"`
}

type scenarioControls struct {
	failurePath  string
	schedulePath string
	maxAttempts  int // 0 = system default

	planMu     sync.Mutex
	plan       *failurePlan
	planReadAt time.Time
	lastHerdID string

	failedAttempts       atomic.Uint64
	retriedCompletions   atomic.Uint64
	poisonExhausted      atomic.Uint64
	scheduledCompletions atomic.Uint64
	scheduleEnqueued     atomic.Uint64
	scheduleEarly        atomic.Uint64
	schedulePreloadMs    atomic.Int64 // -1 until a preload finishes

	latenessMu sync.Mutex
	latenessMs []float64

	lastRates map[string]uint64
}

func newScenarioControls() *scenarioControls {
	c := &scenarioControls{
		failurePath:  os.Getenv("JOB_FAILURE_CONTROL_FILE"),
		schedulePath: os.Getenv("SCHEDULE_CONTROL_FILE"),
		lastRates:    map[string]uint64{},
	}
	if n, err := strconv.Atoi(os.Getenv("JOB_MAX_ATTEMPTS")); err == nil && n > 0 {
		c.maxAttempts = n
	}
	c.schedulePreloadMs.Store(-1)
	return c
}

func (c *scenarioControls) failureEnabled() bool  { return c.failurePath != "" }
func (c *scenarioControls) scheduleEnabled() bool { return c.schedulePath != "" }

func readJSONFile(path string, out interface{}) bool {
	buf, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return json.Unmarshal(buf, out) == nil
}

// tag returns the failure tag for seq under the current plan ("" = none).
func (c *scenarioControls) tag(seq int64) (string, int) {
	if !c.failureEnabled() {
		return "", 0
	}
	c.planMu.Lock()
	if time.Since(c.planReadAt) >= time.Second {
		var plan failurePlan
		if readJSONFile(c.failurePath, &plan) {
			c.plan = &plan
		} else {
			c.plan = nil
		}
		c.planReadAt = time.Now()
	}
	plan := c.plan
	c.planMu.Unlock()
	if plan == nil {
		return "", 0
	}
	poisonCut := int64(math.Round(plan.PoisonPct * 100))
	transientCut := poisonCut + int64(math.Round(plan.TransientPct*100))
	bucket := (seq % 10000) * 7919 % 10000
	switch {
	case bucket < poisonCut:
		return "poison", 0
	case bucket < transientCut:
		failures := plan.TransientFailures
		if failures < 1 {
			failures = 1
		}
		return "transient", failures
	}
	return "", 0
}

// failureOutcome: "" = succeed, "retry", or "exhausted" (last attempt).
func failureOutcome(kind string, failAttempts, attempt, maxAttempts int) string {
	failing := kind == "poison" || (kind == "transient" && attempt <= max(failAttempts, 1))
	if !failing {
		return ""
	}
	if maxAttempts > 0 && attempt >= maxAttempts {
		return "exhausted"
	}
	return "retry"
}

func (c *scenarioControls) onStart(runAtMs int64) {
	if runAtMs == 0 {
		return
	}
	lateness := float64(time.Now().UnixMilli() - runAtMs)
	if lateness < 0 {
		c.scheduleEarly.Add(1)
		lateness = 0
	}
	c.latenessMu.Lock()
	c.latenessMs = append(c.latenessMs, lateness)
	c.latenessMu.Unlock()
}

func (c *scenarioControls) recordFailure(kind, outcome string) {
	c.failedAttempts.Add(1)
	if outcome == "exhausted" && kind == "poison" {
		c.poisonExhausted.Add(1)
	}
}

func (c *scenarioControls) onComplete(kind string, runAtMs int64) {
	if kind == "transient" {
		c.retriedCompletions.Add(1)
	}
	if runAtMs != 0 {
		c.scheduledCompletions.Add(1)
	}
}

func (c *scenarioControls) pollHerd() *herdCommand {
	var cmd herdCommand
	if !readJSONFile(c.schedulePath, &cmd) || cmd.ID == "" || cmd.ID == c.lastHerdID {
		return nil
	}
	c.lastHerdID = cmd.ID
	return &cmd
}

// herdBatches mirrors adapter_common/bench_controls.py::herd_batches.
func herdBatches(cmd *herdCommand) [][2]int64 {
	batch := int64(herdBatchMax)
	if cmd.SpreadMs > 0 {
		batch = min(max(int64(cmd.Count)*100/cmd.SpreadMs, 1), herdBatchMax)
	}
	var out [][2]int64
	count := int64(cmd.Count)
	for done := int64(0); done < count; {
		size := min(batch, count-done)
		offset := int64(0)
		if cmd.SpreadMs > 0 {
			offset = cmd.SpreadMs * done / count
		}
		out = append(out, [2]int64{size, cmd.RunAtMs + offset})
		done += size
	}
	return out
}

// runHerdPreload watches the schedule control file (instance 0 only) and
// enqueues each herd via enqueue(firstSeq, size, runAtMs), retrying a
// failed batch so the herd is never silently short.
func (c *scenarioControls) runHerdPreload(shutdown <-chan struct{}, enqueue func(firstSeq int64, size int, runAtMs int64) error) {
	if !c.scheduleEnabled() || instanceID() != 0 {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-shutdown:
			return
		case <-ticker.C:
		}
		cmd := c.pollHerd()
		if cmd == nil {
			continue
		}
		started := time.Now()
		c.schedulePreloadMs.Store(-1)
		var seq int64
		for _, b := range herdBatches(cmd) {
			for {
				select {
				case <-shutdown:
					return
				default:
				}
				if err := enqueue(seq, int(b[0]), b[1]); err != nil {
					logStderr("[river] herd enqueue failed: %v", err)
					time.Sleep(200 * time.Millisecond)
					continue
				}
				break
			}
			seq += b[0]
			c.scheduleEnqueued.Add(uint64(b[0]))
		}
		c.schedulePreloadMs.Store(time.Since(started).Milliseconds())
		logStderr("[river] herd %s: %d jobs enqueued in %.1fs", cmd.ID, seq, time.Since(started).Seconds())
	}
}

func (c *scenarioControls) rate(name string, value uint64, dt float64) float64 {
	r := float64(value-c.lastRates[name]) / dt
	c.lastRates[name] = value
	return r
}

type controlMetric struct {
	name    string
	value   float64
	windowS float64
}

func (c *scenarioControls) metrics(dt, windowS float64) []controlMetric {
	var out []controlMetric
	if c.failureEnabled() {
		out = append(out,
			controlMetric{"injected_failure_rate", c.rate("failed", c.failedAttempts.Load(), dt), windowS},
			controlMetric{"retried_completion_rate", c.rate("retried", c.retriedCompletions.Load(), dt), windowS},
			controlMetric{"poison_exhausted_rate", c.rate("exhausted", c.poisonExhausted.Load(), dt), windowS},
		)
	}
	if !c.scheduleEnabled() {
		return out
	}
	c.latenessMu.Lock()
	sort.Float64s(c.latenessMs)
	values := append([]float64(nil), c.latenessMs...)
	c.latenessMu.Unlock()
	enqueued := c.scheduleEnqueued.Load()
	if len(values) == 0 && enqueued == 0 {
		return out
	}
	out = append(out, controlMetric{"scheduled_completion_rate", c.rate("scheduled", c.scheduledCompletions.Load(), dt), windowS})
	if n := len(values); n > 0 {
		q := func(p float64) float64 {
			idx := int(math.Round(p * float64(n-1)))
			return values[min(max(idx, 0), n-1)]
		}
		out = append(out,
			controlMetric{"schedule_lateness_p50_ms", q(0.50), 0},
			controlMetric{"schedule_lateness_p95_ms", q(0.95), 0},
			controlMetric{"schedule_lateness_p99_ms", q(0.99), 0},
			controlMetric{"schedule_lateness_max_ms", values[n-1], 0},
		)
	}
	out = append(out,
		controlMetric{"schedule_started_total", float64(len(values)), 0},
		controlMetric{"schedule_enqueued_total", float64(enqueued), 0},
		controlMetric{"schedule_early_total", float64(c.scheduleEarly.Load()), 0},
	)
	if ms := c.schedulePreloadMs.Load(); ms >= 0 {
		out = append(out, controlMetric{"schedule_preload_s", float64(ms) / 1000, 0})
	}
	return out
}

// injectedFailureFilter drops River's per-attempt "Job errored" log lines
// for injected failures so a storm doesn't flood stderr.
type injectedFailureFilter struct{ slog.Handler }

func (h injectedFailureFilter) Handle(ctx context.Context, record slog.Record) error {
	injected := false
	record.Attrs(func(attr slog.Attr) bool {
		if strings.Contains(attr.Value.String(), "injected ") {
			injected = true
			return false
		}
		return true
	})
	if injected {
		return nil
	}
	return h.Handler.Handle(ctx, record)
}

func (h injectedFailureFilter) WithAttrs(attrs []slog.Attr) slog.Handler {
	return injectedFailureFilter{h.Handler.WithAttrs(attrs)}
}

func (h injectedFailureFilter) WithGroup(name string) slog.Handler {
	return injectedFailureFilter{h.Handler.WithGroup(name)}
}

func logStderr(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}
