defmodule ObanBench.OperatorRepo do
  @moduledoc "Small repo for operator probes, so they never take a worker connection."
  use Ecto.Repo,
    otp_app: :oban_bench,
    adapter: Ecto.Adapters.Postgres
end

defmodule ObanBench.BenchAltWorker do
  @moduledoc "Second job kind in the haystack; its queue is never started."
  use Oban.Worker, queue: :haystack

  @impl Oban.Worker
  def perform(%Oban.Job{}), do: :ok
end

defmodule ObanBench.Operator do
  @moduledoc """
  Operator scenarios (docs/operator-scenarios.md).

  Default-off: with `OPERATOR_CONTROL_FILE` set, every replica counts handler
  starts relative to the latest op, and instance 0 performs each op
  (haystack, probe, pause, resume). Probes query `Oban.Job` through
  `ObanBench.OperatorRepo`, a separate two-connection pool.
  """

  import Ecto.Query
  alias ObanBench.{BenchAltWorker, LongHorizonWorker, OperatorRepo}

  @counters :operator_counters
  @state :operator_state
  # :atomics slots
  @starts 1
  @last_start 2
  @first_after_op 3
  @op_seen 4

  @poll_ms 100
  @haystack_batch 1000
  @kept_pairs 10_000
  @probe_names ~w(get_by_id find_handle find_tenant_page kind_state_page count_state)

  def control_path, do: System.get_env("OPERATOR_CONTROL_FILE")
  def result_path, do: System.get_env("OPERATOR_RESULT_FILE")
  def enabled?, do: control_path() != nil

  @doc "Adopts the control file's current id as seen and starts the poller."
  def init(bench_queues) do
    :persistent_term.put(@counters, :atomics.new(4, signed: true))

    if enabled?() do
      :ets.new(@state, [:public, :named_table, :set])
      :ets.insert(@state, {:bench_queues, bench_queues})
      :ets.insert(@state, {:haystack, nil})

      if instance_id() == 0 do
        {:ok, _} = OperatorRepo.start_link()
      end

      last_id = read_json(control_path())["id"]
      spawn(fn -> poll_loop(last_id) end)
    end

    :ok
  end

  def on_job_start do
    case :persistent_term.get(@counters, nil) do
      nil ->
        :ok

      ref ->
        now = System.system_time(:millisecond)
        :atomics.add(ref, @starts, 1)
        :atomics.put(ref, @last_start, now)

        if :atomics.get(ref, @op_seen) == 1 do
          :atomics.compare_exchange(ref, @first_after_op, 0, now)
        end

        :ok
    end
  end

  @doc "Sampler metrics as {name, value, window_s} tuples."
  def metrics do
    if enabled?() do
      ref = :persistent_term.get(@counters)

      [
        {"job_starts_total", :atomics.get(ref, @starts) * 1.0, 0},
        {"last_job_start_ms", :atomics.get(ref, @last_start) * 1.0, 0},
        {"first_start_after_op_ms", :atomics.get(ref, @first_after_op) * 1.0, 0}
      ]
    else
      []
    end
  end

  # Polls the control file and, as the only writer, merges results from op
  # processes into the result file.
  defp poll_loop(last_id) do
    receive do
      {:result, id, outcome} ->
        write_result(id, outcome)
        poll_loop(last_id)
    after
      @poll_ms ->
        case read_json(control_path()) do
          %{"id" => id, "op" => op} = command when is_binary(id) and id != last_id ->
            ref = :persistent_term.get(@counters)
            :atomics.put(ref, @first_after_op, 0)
            :atomics.put(ref, @op_seen, 1)
            IO.puts(:stderr, "[oban] operator: saw #{id} (#{op})")

            if instance_id() == 0 do
              poller = self()
              spawn(fn -> send(poller, {:result, id, perform(command)}) end)
            end

            poll_loop(id)

          _ ->
            poll_loop(last_id)
        end
    end
  end

  defp perform(%{"op" => op} = command) do
    args = command["args"] || %{}

    outcome =
      try do
        case op do
          "haystack" -> haystack(args)
          "probe" -> probe(args)
          "pause" -> pause_or_resume(:pause)
          "resume" -> pause_or_resume(:resume)
          other -> %{"error" => "unknown op #{inspect(other)}"}
        end
      rescue
        error -> %{"error" => Exception.message(error)}
      catch
        kind, reason -> %{"error" => "#{kind}: #{inspect(reason)}"}
      end

    if outcome["error"], do: IO.puts(:stderr, "[oban] operator: #{op} failed: #{outcome["error"]}")

    outcome
    |> Map.put_new("done_ms", System.system_time(:millisecond))
    |> Map.put("done", true)
  end

  defp write_result(id, outcome) do
    path = result_path()
    all = path |> read_json() |> Map.put(id, outcome)
    tmp = path <> ".oban.tmp"
    File.write!(tmp, Jason.encode!(all))
    File.rename!(tmp, path)
  rescue
    error -> IO.puts(:stderr, "[oban] operator: write result: #{Exception.message(error)}")
  end

  # ── haystack ─────────────────────────────────────────────────────

  # Oban.insert_all (no worker runs `queue`), keeping a uniform reservoir
  # sample of {id, seq} pairs for the probes.
  defp haystack(args) do
    jobs = args["jobs"] || 0
    queue = args["queue"] || "haystack"
    tenants = max(args["tenants"] || 1000, 1)
    alt_every = max(args["alt_every"] || 20, 1)
    started = System.monotonic_time(:millisecond)

    reservoir =
      0..(jobs - 1)//1
      |> Stream.chunk_every(@haystack_batch)
      |> Enum.reduce(%{}, fn seqs, reservoir ->
        changesets =
          Enum.map(seqs, fn seq ->
            job_args = %{h: "h#{seq}", t: "t#{rem(seq, tenants)}"}

            if rem(seq, alt_every) == 0,
              do: BenchAltWorker.new(job_args, queue: queue),
              else: LongHorizonWorker.new(job_args, queue: queue)
          end)

        inserted = Oban.insert_all(changesets)

        inserted
        |> Enum.zip(seqs)
        |> Enum.reduce(reservoir, fn {job, seq}, acc -> keep_pair(acc, {job.id, seq}, seq) end)
      end)

    seconds = (System.monotonic_time(:millisecond) - started) / 1000

    previous =
      case :ets.lookup(@state, :haystack) do
        [{:haystack, %{queue: ^queue, jobs: n}}] -> n
        _ -> 0
      end

    :ets.insert(
      @state,
      {:haystack,
       %{
         queue: queue,
         jobs: previous + jobs,
         tenants: tenants,
         alt_every: alt_every,
         pairs: reservoir |> Map.values() |> List.to_tuple()
       }}
    )

    IO.puts(:stderr, "[oban] operator: haystack #{jobs} jobs into #{queue} in #{seconds}s")
    %{"enqueued" => jobs, "seconds" => seconds}
  end

  defp keep_pair(reservoir, pair, seq) when seq < @kept_pairs, do: Map.put(reservoir, seq, pair)

  defp keep_pair(reservoir, pair, seq) do
    slot = :rand.uniform(seq + 1) - 1
    if slot < @kept_pairs, do: Map.put(reservoir, slot, pair), else: reservoir
  end

  # ── probe ────────────────────────────────────────────────────────

  defp probe(args) do
    case :ets.lookup(@state, :haystack) do
      [{:haystack, %{pairs: pairs} = hay}] when tuple_size(pairs) > 0 -> run_probes(args, hay)
      _ -> %{"error" => "no haystack in this process to probe"}
    end
  end

  defp run_probes(args, hay) do
    duration_ms = trunc((args["duration_s"] || 180) * 1000)
    rate = args["rate"] || 2
    page = max(args["page"] || 100, 1)
    timeout = max(args["timeout_ms"] || 30_000, 1)
    queue = args["queue"] || hay.queue
    interval_ms = 1000 / rate
    started = System.monotonic_time(:millisecond)
    deadline = started + duration_ms

    tallies =
      Stream.iterate(0, &(&1 + 1))
      |> Enum.reduce_while(%{}, fn i, tallies ->
        wait = trunc(started + i * interval_ms) - System.monotonic_time(:millisecond)
        if wait > 0, do: Process.sleep(wait)

        if System.monotonic_time(:millisecond) >= deadline do
          {:halt, tallies}
        else
          name = Enum.at(@probe_names, rem(i, length(@probe_names)))
          target = elem(hay.pairs, :rand.uniform(tuple_size(hay.pairs)) - 1)
          result = timed_probe(name, target, hay, queue, page, timeout)
          {:cont, Map.update(tallies, name, [result], &[result | &1])}
        end
      end)

    probes =
      Map.new(@probe_names, fn name -> {name, summarise(Map.get(tallies, name, []))} end)

    %{"probes" => probes}
  end

  defp timed_probe(name, target, hay, queue, page, timeout) do
    t0 = System.monotonic_time(:microsecond)

    try do
      {rows, correct} = run_probe(name, target, hay, queue, page, timeout)
      {:ok, (System.monotonic_time(:microsecond) - t0) / 1000, rows, correct}
    rescue
      error ->
        elapsed_ms = (System.monotonic_time(:microsecond) - t0) / 1000

        if elapsed_ms >= timeout do
          {:timeout}
        else
          IO.puts(:stderr, "[oban] operator: probe #{name}: #{Exception.message(error)}")
          {:error}
        end
    end
  end

  defp run_probe("get_by_id", {id, seq}, _hay, _queue, _page, timeout) do
    job = Oban.Job.query(id: id) |> OperatorRepo.one(timeout: timeout)
    {if(job, do: 1, else: 0), job != nil and job.args["h"] == "h#{seq}"}
  end

  defp run_probe("find_handle", {id, seq}, _hay, _queue, _page, timeout) do
    jobs =
      Oban.Job.query(args: %{h: "h#{seq}"})
      |> limit(2)
      |> OperatorRepo.all(timeout: timeout)

    {length(jobs), match?([%{id: ^id}], jobs)}
  end

  defp run_probe("find_tenant_page", {_id, seq}, hay, _queue, page, timeout) do
    tenant = rem(seq, hay.tenants)

    jobs =
      Oban.Job.query(args: %{t: "t#{tenant}"})
      |> order_by(asc: :id)
      |> limit(^page)
      |> OperatorRepo.all(timeout: timeout)

    matches = div(hay.jobs, hay.tenants) + if(tenant < rem(hay.jobs, hay.tenants), do: 1, else: 0)
    {length(jobs), length(jobs) == min(page, matches)}
  end

  defp run_probe("kind_state_page", _target, hay, queue, page, timeout) do
    jobs =
      Oban.Job.query(worker: BenchAltWorker, state: :available, queue: queue)
      |> order_by(asc: :id)
      |> limit(^page)
      |> OperatorRepo.all(timeout: timeout)

    alt_jobs = div(hay.jobs + hay.alt_every - 1, hay.alt_every)
    {length(jobs), length(jobs) == min(page, alt_jobs)}
  end

  defp run_probe("count_state", _target, hay, queue, _page, timeout) do
    count =
      Oban.Job.query(state: :available, queue: queue)
      |> OperatorRepo.aggregate(:count, timeout: timeout)

    {1, abs(count - hay.jobs) <= 0.01 * hay.jobs}
  end

  defp summarise(results) do
    ok = for {:ok, ms, rows, correct} <- results, do: {ms, rows, correct}
    n = length(results)
    errors = Enum.count(results, &(&1 == {:error}))
    timeouts = Enum.count(results, &(&1 == {:timeout}))

    base = %{"mode" => "native", "n" => n, "errors" => errors, "timeouts" => timeouts}

    base =
      if n > 0,
        do: Map.put(base, "correct", Enum.count(ok, fn {_, _, c} -> c end) / n),
        else: base

    case ok do
      [] ->
        base

      _ ->
        latencies = ok |> Enum.map(&elem(&1, 0)) |> Enum.sort() |> List.to_tuple()
        rows = ok |> Enum.map(&(elem(&1, 1) * 1.0)) |> Enum.sort() |> List.to_tuple()

        Map.merge(base, %{
          "p50_ms" => quantile(latencies, 0.50),
          "p99_ms" => quantile(latencies, 0.99),
          "max_ms" => elem(latencies, tuple_size(latencies) - 1),
          "rows_median" => quantile(rows, 0.50)
        })
    end
  end

  defp quantile(sorted, q) do
    n = tuple_size(sorted)
    elem(sorted, min(n - 1, max(0, round(q * (n - 1)))))
  end

  # ── pause / resume ───────────────────────────────────────────────

  # Oban broadcasts the pause to every running producer; it is held in
  # memory only, so a node started later runs the queue unpaused.
  defp pause_or_resume(action) do
    [{:bench_queues, queues}] = :ets.lookup(@state, :bench_queues)
    t0 = System.monotonic_time(:microsecond)

    Enum.each(queues, fn queue ->
      :ok =
        case action do
          :pause -> Oban.pause_queue(queue: queue)
          :resume -> Oban.resume_queue(queue: queue)
        end
    end)

    api_ms = (System.monotonic_time(:microsecond) - t0) / 1000
    %{"mode" => "native", "api_ms" => api_ms, "done_ms" => System.system_time(:millisecond)}
  end

  # ── helpers ──────────────────────────────────────────────────────

  defp read_json(nil), do: %{}

  defp read_json(path) do
    with {:ok, body} <- File.read(path),
         {:ok, %{} = value} <- Jason.decode(body) do
      value
    else
      _ -> %{}
    end
  end

  defp instance_id do
    case Integer.parse(System.get_env("BENCH_INSTANCE_ID") || "0") do
      {n, ""} -> n
      _ -> 0
    end
  end
end
