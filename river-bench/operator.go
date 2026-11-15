// Operator scenarios (docs/operator-scenarios.md).
//
// Default-off: with OPERATOR_CONTROL_FILE set, every replica counts handler
// starts relative to the latest op, and instance 0 performs each op
// (haystack, probe, pause, resume) through a separate insert-only River
// client on its own small pool, so operator work never takes a worker
// connection.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

const (
	operatorPollInterval = 100 * time.Millisecond
	haystackBatchSize    = 1000
	haystackKeptPairs    = 10000
)

// BenchAltArgs is the second job kind in the haystack. No worker is
// registered for it; the haystack queue is never consumed.
type BenchAltArgs struct {
	Seq int64 `json:"seq"`
}

func (BenchAltArgs) Kind() string { return "bench_alt" }

type operatorCommand struct {
	ID       string         `json:"id"`
	Op       string         `json:"op"`
	Args     map[string]any `json:"args"`
	IssuedMs int64          `json:"issued_ms"`
}

type haystackPair struct {
	id  int64
	seq int64
}

// haystackInfo describes what the haystack op enqueued, as ground truth for
// the probes.
type haystackInfo struct {
	queue    string
	jobs     int64 // total enqueued into queue by this process
	tenants  int64
	altEvery int64
	pairs    []haystackPair
}

type operatorState struct {
	controlPath string
	resultPath  string
	lastID      string

	jobStarts      atomic.Uint64
	lastStartMs    atomic.Int64
	firstAfterOpMs atomic.Int64
	opSeen         atomic.Bool

	resultMu   sync.Mutex
	haystackMu sync.Mutex
	haystack   haystackInfo
}

// newOperatorState adopts the control file's current id as already seen, so
// a process started mid-scenario doesn't act on an old op.
func newOperatorState() *operatorState {
	o := &operatorState{
		controlPath: os.Getenv("OPERATOR_CONTROL_FILE"),
		resultPath:  os.Getenv("OPERATOR_RESULT_FILE"),
	}
	if o.enabled() {
		var cmd operatorCommand
		if readJSONFile(o.controlPath, &cmd) {
			o.lastID = cmd.ID
		}
	}
	return o
}

func (o *operatorState) enabled() bool { return o.controlPath != "" }

func (o *operatorState) onJobStart() {
	now := time.Now().UnixMilli()
	o.jobStarts.Add(1)
	o.lastStartMs.Store(now)
	if o.opSeen.Load() {
		o.firstAfterOpMs.CompareAndSwap(0, now)
	}
}

func (o *operatorState) metrics() []controlMetric {
	if !o.enabled() {
		return nil
	}
	return []controlMetric{
		{"job_starts_total", float64(o.jobStarts.Load()), 0},
		{"last_job_start_ms", float64(o.lastStartMs.Load()), 0},
		{"first_start_after_op_ms", float64(o.firstAfterOpMs.Load()), 0},
	}
}

// riverOperator performs ops on instance 0.
type riverOperator struct {
	state       *operatorState
	pool        *pgxpool.Pool
	client      *river.Client[pgx.Tx]
	benchQueues []string
}

// runOperator polls the control file until shutdown. Every replica notes new
// ops; instance 0 also performs them.
func runOperator(ctx context.Context, shutdown <-chan struct{}, state *operatorState, benchQueues []string) {
	if !state.enabled() {
		return
	}
	var operator *riverOperator
	if instanceID() == 0 {
		poolConfig, err := pgxpool.ParseConfig(databaseURL())
		if err != nil {
			logStderr("[river] operator: parse database URL: %v", err)
			return
		}
		poolConfig.MaxConns = 2
		pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			logStderr("[river] operator: connect: %v", err)
			return
		}
		defer pool.Close()
		// Insert-only client: no queues or workers, so it never fetches.
		client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
		if err != nil {
			logStderr("[river] operator: client: %v", err)
			return
		}
		operator = &riverOperator{state: state, pool: pool, client: client, benchQueues: benchQueues}
	}
	ticker := time.NewTicker(operatorPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-shutdown:
			return
		case <-ticker.C:
		}
		var cmd operatorCommand
		if !readJSONFile(state.controlPath, &cmd) || cmd.ID == "" || cmd.ID == state.lastID {
			continue
		}
		state.lastID = cmd.ID
		state.firstAfterOpMs.Store(0)
		state.opSeen.Store(true)
		logStderr("[river] operator: saw %s (%s)", cmd.ID, cmd.Op)
		if operator != nil {
			go operator.perform(ctx, cmd)
		}
	}
}

func (r *riverOperator) perform(ctx context.Context, cmd operatorCommand) {
	var outcome map[string]any
	var err error
	switch cmd.Op {
	case "haystack":
		outcome, err = r.haystack(ctx, cmd.Args)
	case "probe":
		outcome, err = r.probe(ctx, cmd.Args)
	case "pause", "resume":
		outcome, err = r.pauseOrResume(ctx, cmd.Op)
	default:
		err = fmt.Errorf("unknown op %q", cmd.Op)
	}
	if outcome == nil {
		outcome = map[string]any{}
	}
	if err != nil {
		logStderr("[river] operator: %s failed: %v", cmd.ID, err)
		outcome["error"] = err.Error()
	}
	if _, ok := outcome["done_ms"]; !ok {
		outcome["done_ms"] = time.Now().UnixMilli()
	}
	outcome["done"] = true
	if err := r.state.writeResult(cmd.ID, outcome); err != nil {
		logStderr("[river] operator: write result: %v", err)
	}
}

// writeResult merges {id: outcome} into the result file by atomic rename.
func (o *operatorState) writeResult(id string, outcome map[string]any) error {
	o.resultMu.Lock()
	defer o.resultMu.Unlock()
	all := map[string]any{}
	readJSONFile(o.resultPath, &all)
	all[id] = outcome
	buf, err := json.Marshal(all)
	if err != nil {
		return err
	}
	tmp := o.resultPath + ".river.tmp"
	if err := os.WriteFile(tmp, buf, 0o666); err != nil {
		return err
	}
	return os.Rename(tmp, o.resultPath)
}

func argInt(args map[string]any, key string, def int64) int64 {
	if v, ok := args[key].(float64); ok {
		return int64(v)
	}
	return def
}

func argString(args map[string]any, key, def string) string {
	if v, ok := args[key].(string); ok && v != "" {
		return v
	}
	return def
}

// haystack enqueues the jobs through InsertMany (which, unlike
// InsertManyFast, returns the inserted rows and so their ids), keeping a
// uniform reservoir sample of (id, seq) pairs for the probes.
func (r *riverOperator) haystack(ctx context.Context, args map[string]any) (map[string]any, error) {
	jobs := argInt(args, "jobs", 0)
	queue := argString(args, "queue", "haystack")
	tenants := max(argInt(args, "tenants", 1000), 1)
	altEvery := max(argInt(args, "alt_every", 20), 1)
	started := time.Now()
	pairs := make([]haystackPair, 0, min(jobs, haystackKeptPairs))
	var enqueued int64
	for enqueued < jobs {
		size := min(int64(haystackBatchSize), jobs-enqueued)
		params := make([]river.InsertManyParams, 0, size)
		for i := int64(0); i < size; i++ {
			seq := enqueued + i
			metadata, _ := json.Marshal(map[string]string{
				"h": fmt.Sprintf("h%d", seq),
				"t": fmt.Sprintf("t%d", seq%tenants),
			})
			var jobArgs river.JobArgs = LongHorizonArgs{Seq: seq}
			if seq%altEvery == 0 {
				jobArgs = BenchAltArgs{Seq: seq}
			}
			params = append(params, river.InsertManyParams{
				Args:       jobArgs,
				InsertOpts: &river.InsertOpts{Queue: queue, Metadata: metadata},
			})
		}
		results, err := r.client.InsertMany(ctx, params)
		if err != nil {
			return map[string]any{"enqueued": enqueued, "seconds": time.Since(started).Seconds()}, err
		}
		for i, result := range results {
			pair := haystackPair{id: result.Job.ID, seq: enqueued + int64(i)}
			if len(pairs) < haystackKeptPairs {
				pairs = append(pairs, pair)
			} else if j := rand.Int64N(pair.seq + 1); j < haystackKeptPairs {
				pairs[j] = pair
			}
		}
		enqueued += size
	}
	seconds := time.Since(started).Seconds()
	r.state.haystackMu.Lock()
	previous := r.state.haystack.jobs
	if r.state.haystack.queue != queue {
		previous = 0
	}
	r.state.haystack = haystackInfo{queue: queue, jobs: previous + enqueued, tenants: tenants, altEvery: altEvery, pairs: pairs}
	r.state.haystackMu.Unlock()
	logStderr("[river] operator: haystack %d jobs into %q in %.1fs", enqueued, queue, seconds)
	return map[string]any{"enqueued": enqueued, "seconds": seconds}, nil
}

type probeTally struct {
	mode      string
	latencies []float64
	rows      []float64
	correct   int
	errors    int
	timeouts  int
}

type probeResult struct {
	rows    int
	correct bool
}

type probeFunc func(ctx context.Context, target haystackPair) (probeResult, error)

func (r *riverOperator) probe(ctx context.Context, args map[string]any) (map[string]any, error) {
	durationS := argInt(args, "duration_s", 180)
	rate := 2.0
	if v, ok := args["rate"].(float64); ok && v > 0 {
		rate = v
	}
	page := int(max(argInt(args, "page", 100), 1))
	timeout := time.Duration(max(argInt(args, "timeout_ms", 30000), 1)) * time.Millisecond

	r.state.haystackMu.Lock()
	hay := r.state.haystack
	r.state.haystackMu.Unlock()
	queue := argString(args, "queue", hay.queue)
	if len(hay.pairs) == 0 {
		return nil, errors.New("no haystack in this process to probe")
	}

	tenantMatches := func(tenant int64) int64 {
		matches := hay.jobs / hay.tenants
		if tenant < hay.jobs%hay.tenants {
			matches++
		}
		return matches
	}
	altJobs := (hay.jobs + hay.altEvery - 1) / hay.altEvery

	names := []string{"get_by_id", "find_handle", "find_tenant_page", "kind_state_page", "count_state"}
	modes := map[string]string{
		"get_by_id":        "native",
		"find_handle":      "native",
		"find_tenant_page": "native",
		"kind_state_page":  "native",
		// River has no count API; this is the obvious query on its own table.
		"count_state": "sql",
	}
	probes := map[string]probeFunc{
		"get_by_id": func(ctx context.Context, target haystackPair) (probeResult, error) {
			job, err := r.client.JobGet(ctx, target.id)
			if err != nil {
				return probeResult{}, err
			}
			return probeResult{rows: 1, correct: metadataHandle(job) == fmt.Sprintf("h%d", target.seq)}, nil
		},
		"find_handle": func(ctx context.Context, target haystackPair) (probeResult, error) {
			fragment := fmt.Sprintf(`{"h":"h%d"}`, target.seq)
			result, err := r.client.JobList(ctx, river.NewJobListParams().Metadata(fragment).First(2))
			if err != nil {
				return probeResult{}, err
			}
			ok := len(result.Jobs) == 1 && result.Jobs[0].ID == target.id
			return probeResult{rows: len(result.Jobs), correct: ok}, nil
		},
		"find_tenant_page": func(ctx context.Context, target haystackPair) (probeResult, error) {
			tenant := target.seq % hay.tenants
			fragment := fmt.Sprintf(`{"t":"t%d"}`, tenant)
			result, err := r.client.JobList(ctx, river.NewJobListParams().Metadata(fragment).First(page))
			if err != nil {
				return probeResult{}, err
			}
			want := min(int64(page), tenantMatches(tenant))
			return probeResult{rows: len(result.Jobs), correct: int64(len(result.Jobs)) == want}, nil
		},
		"kind_state_page": func(ctx context.Context, _ haystackPair) (probeResult, error) {
			params := river.NewJobListParams().
				Kinds(BenchAltArgs{}.Kind()).
				States(rivertype.JobStateAvailable).
				Queues(queue).
				First(page)
			result, err := r.client.JobList(ctx, params)
			if err != nil {
				return probeResult{}, err
			}
			want := min(int64(page), altJobs)
			return probeResult{rows: len(result.Jobs), correct: int64(len(result.Jobs)) == want}, nil
		},
		"count_state": func(ctx context.Context, _ haystackPair) (probeResult, error) {
			var count int64
			err := r.pool.QueryRow(ctx,
				"SELECT count(*) FROM river_job WHERE queue = $1 AND state = 'available'", queue,
			).Scan(&count)
			if err != nil {
				return probeResult{}, err
			}
			diff := count - hay.jobs
			if diff < 0 {
				diff = -diff
			}
			return probeResult{rows: 1, correct: float64(diff) <= 0.01*float64(hay.jobs)}, nil
		},
	}

	tallies := map[string]*probeTally{}
	for _, name := range names {
		tallies[name] = &probeTally{mode: modes[name]}
	}
	interval := time.Duration(float64(time.Second) / rate)
	deadline := time.Now().Add(time.Duration(durationS) * time.Second)
	next := time.Now()
	for i := 0; time.Now().Before(deadline); i++ {
		if wait := time.Until(next); wait > 0 {
			time.Sleep(wait)
		}
		next = next.Add(interval)
		name := names[i%len(names)]
		tally := tallies[name]
		target := hay.pairs[rand.IntN(len(hay.pairs))]
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		started := time.Now()
		result, err := probes[name](probeCtx, target)
		elapsedMs := float64(time.Since(started).Microseconds()) / 1000
		timedOut := errors.Is(probeCtx.Err(), context.DeadlineExceeded)
		cancel()
		switch {
		case err != nil && timedOut:
			tally.timeouts++
		case err != nil:
			tally.errors++
			logStderr("[river] operator: probe %s: %v", name, err)
		default:
			tally.latencies = append(tally.latencies, elapsedMs)
			tally.rows = append(tally.rows, float64(result.rows))
			if result.correct {
				tally.correct++
			}
		}
		if now := time.Now(); next.Before(now) {
			next = now
		}
	}

	out := map[string]any{}
	for _, name := range names {
		tally := tallies[name]
		n := len(tally.latencies) + tally.errors + tally.timeouts
		block := map[string]any{
			"mode":     tally.mode,
			"n":        n,
			"errors":   tally.errors,
			"timeouts": tally.timeouts,
		}
		if n > 0 {
			block["correct"] = float64(tally.correct) / float64(n)
		}
		if len(tally.latencies) > 0 {
			sort.Float64s(tally.latencies)
			sort.Float64s(tally.rows)
			block["p50_ms"] = quantile(tally.latencies, 0.50)
			block["p99_ms"] = quantile(tally.latencies, 0.99)
			block["max_ms"] = tally.latencies[len(tally.latencies)-1]
			block["rows_median"] = quantile(tally.rows, 0.50)
		}
		out[name] = block
	}
	return map[string]any{"probes": out}, nil
}

func quantile(sorted []float64, q float64) float64 {
	idx := int(q*float64(len(sorted)-1) + 0.5)
	return sorted[min(max(idx, 0), len(sorted)-1)]
}

func metadataHandle(job *rivertype.JobRow) string {
	var metadata struct {
		H string `json:"h"`
	}
	_ = json.Unmarshal(job.Metadata, &metadata)
	return metadata.H
}

// pauseOrResume uses River's QueuePause/QueueResume, which store the pause
// in river_queue and notify every client.
func (r *riverOperator) pauseOrResume(ctx context.Context, op string) (map[string]any, error) {
	started := time.Now()
	for _, queue := range r.benchQueues {
		var err error
		if op == "pause" {
			err = r.client.QueuePause(ctx, queue, nil)
		} else {
			err = r.client.QueueResume(ctx, queue, nil)
		}
		if err != nil {
			return map[string]any{"mode": "native"}, fmt.Errorf("%s %s: %w", op, queue, err)
		}
	}
	apiMs := float64(time.Since(started).Microseconds()) / 1000
	return map[string]any{"mode": "native", "api_ms": apiMs, "done_ms": time.Now().UnixMilli()}, nil
}
