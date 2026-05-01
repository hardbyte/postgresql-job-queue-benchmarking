defmodule ObanBench.LongHorizon do
  @moduledoc """
  Long-horizon scenario for the portable bench.

  Fixed-rate producer + steady consumer with clock-aligned JSONL telemetry
  samples every SAMPLE_EVERY_S seconds. Contract:
  benchmarks/portable/CONTRIBUTING_ADAPTERS.md

  Lifecycle: emits one descriptor record, runs until the BEAM exits (the
  bench harness sends SIGTERM to the Docker container, which terminates the
  release; stdout is line-buffered so all emitted samples survive).
  """

  import Ecto.Query
  alias ObanBench.{Repo, LongHorizonWorker}

  @queue :long_horizon_bench
  @latency_window_s 30.0

  # Adapter metrics that describe a *global* observation (queue depth,
  # total backlog) rather than this replica's per-instance behaviour.
  # Only instance 0 emits these. Mirror of pgmq-bench/awa-bench's
  # observer-metric set.
  @observer_metrics MapSet.new([
                      "queue_depth",
                      "running_depth",
                      "retryable_depth",
                      "scheduled_depth",
                      "total_backlog",
                      "producer_target_rate"
                    ])

  def run do
    sample_every_s = env_int("SAMPLE_EVERY_S", 10)
    producer_rate = env_int("PRODUCER_RATE", 800)
    producer_mode = System.get_env("PRODUCER_MODE", "fixed")
    target_depth = env_int("TARGET_DEPTH", 1000)
    producer_rate_control_file = System.get_env("PRODUCER_RATE_CONTROL_FILE")
    worker_count = env_int("WORKER_COUNT", 32)
    work_ms = env_int("JOB_WORK_MS", 1)
    payload_bytes = env_int("JOB_PAYLOAD_BYTES", 256)
    # Batch size for Oban's documented bulk-insert API
    # (`Oban.insert_all/2`: https://hexdocs.pm/oban/Oban.html#insert_all/2 —
    # accepts a list of changesets, issues a single multi-row INSERT).
    # Default 1 keeps existing row-by-row behaviour.
    producer_batch_max = max(1, env_int("PRODUCER_BATCH_MAX", 1))
    producer_batch_ms = max(1, env_int("PRODUCER_BATCH_MS", 10))

    db_name =
      System.get_env("DATABASE_URL", "")
      |> String.split("/")
      |> List.last()
      |> case do
        nil -> "oban_bench"
        "" -> "oban_bench"
        other -> other
      end

    emit(%{
      kind: "descriptor",
      system: "oban",
      event_tables: ["public.oban_jobs", "public.oban_peers"],
      extensions: [],
      version: "0.1.0",
      schema_version: System.get_env("OBAN_SCHEMA_VERSION", "current"),
      db_name: db_name,
      started_at: now_iso()
    })

    :ets.new(:long_horizon_state, [:public, :named_table, :set])
    :ets.insert(:long_horizon_state, {:enqueued, 0})
    :ets.insert(:long_horizon_state, {:completed, 0})
    :ets.insert(:long_horizon_state, {:work_ms, work_ms})
    :ets.insert(:long_horizon_state, {:queue_depth, 0})
    :ets.insert(:long_horizon_state, {:producer_target_rate, producer_rate * 1.0})
    :ets.new(:long_horizon_lat, [:public, :named_table, :duplicate_bag])

    Oban.scale_queue(queue: @queue, limit: worker_count)

    padding = String.duplicate("x", max(0, payload_bytes - 32))

    _producer =
      spawn(fn ->
        producer_loop(
          producer_mode,
          producer_rate,
          target_depth,
          padding,
          producer_rate_control_file,
          producer_batch_max,
          producer_batch_ms
        )
      end)

    _sampler = spawn(fn -> sampler_loop(sample_every_s) end)
    _depth = spawn(fn -> depth_loop() end)

    # Block forever — the harness SIGTERMs the container.
    Process.sleep(:infinity)
  end

  defp producer_loop("fixed", rate, _target_depth, _padding, _control_file, _batch_max, _batch_ms)
       when rate <= 0,
       do: :ok

  defp producer_loop(mode, rate, target_depth, padding, control_file, batch_max, batch_ms) do
    producer_step(
      0,
      mode,
      rate,
      target_depth,
      padding,
      control_file,
      batch_max,
      batch_ms,
      0.0,
      System.monotonic_time(:millisecond)
    )
  end

  # `rate_credit` accumulates fractional rate*dt while we wait for the
  # next batch boundary; we dispatch up to `batch_max` jobs per
  # `Oban.insert_all/2` call when the credit ≥ 1.
  defp producer_step(
         seq,
         mode,
         base_rate,
         target_depth,
         padding,
         control_file,
         batch_max,
         batch_ms,
         rate_credit,
         last_credit_tick_ms
       ) do
    {batch_count, new_credit, new_tick_ms} =
      case mode do
        "depth-target" ->
          :ets.insert(:long_horizon_state, {:producer_target_rate, 0.0})

          remaining =
            case :ets.lookup(:long_horizon_state, :queue_depth) do
              [{:queue_depth, depth}] -> max(0, target_depth - depth)
              _ -> target_depth
            end

          if remaining <= 0 do
            Process.sleep(50)
            {0, rate_credit, last_credit_tick_ms}
          else
            count = min(batch_max, remaining)
            {count, rate_credit, last_credit_tick_ms}
          end

        _ ->
          current_rate = read_producer_rate(base_rate, control_file)
          :ets.insert(:long_horizon_state, {:producer_target_rate, current_rate * 1.0})

          cond do
            current_rate <= 0 ->
              Process.sleep(100)
              {0, 0.0, System.monotonic_time(:millisecond)}

            batch_max <= 1 ->
              # Original single-row pacing.
              Process.sleep(max(1, div(1000, current_rate)))
              {1, rate_credit, last_credit_tick_ms}

            true ->
              # Sleep for batch_ms, accumulate rate*dt jobs of credit,
              # dispatch up to batch_max.
              now_ms = System.monotonic_time(:millisecond)
              elapsed_ms = now_ms - last_credit_tick_ms
              sleep_for = batch_ms - elapsed_ms
              if sleep_for > 0, do: Process.sleep(sleep_for)
              now_ms2 = System.monotonic_time(:millisecond)
              dt_s = (now_ms2 - last_credit_tick_ms) / 1000.0
              credit = rate_credit + current_rate * dt_s

              if credit < 1.0 do
                {0, credit, now_ms2}
              else
                whole = trunc(credit)
                count = min(batch_max, whole)
                {count, credit - count, now_ms2}
              end
          end
      end

    case batch_count do
      0 ->
        producer_step(
          seq,
          mode,
          base_rate,
          target_depth,
          padding,
          control_file,
          batch_max,
          batch_ms,
          new_credit,
          new_tick_ms
        )

      _ ->
        result = insert_batch(seq, batch_count, padding, batch_max)

        case result do
          :ok ->
            :ets.update_counter(:long_horizon_state, :enqueued, batch_count)

            producer_step(
              seq + batch_count,
              mode,
              base_rate,
              target_depth,
              padding,
              control_file,
              batch_max,
              batch_ms,
              new_credit,
              new_tick_ms
            )

          :stopping ->
            :ok

          :error ->
            # Skip this attempt to avoid a tight retry loop, then continue
            # with the same seq budget so we don't drop sequence ids.
            producer_step(
              seq + batch_count,
              mode,
              base_rate,
              target_depth,
              padding,
              control_file,
              batch_max,
              batch_ms,
              new_credit,
              new_tick_ms
            )
        end
    end
  end

  # batch_max == 1 → preserve the original Oban.insert/1 wire path.
  # batch_max > 1 → Oban.insert_all/2 (documented bulk path).
  defp insert_batch(seq, 1, padding, batch_max) when batch_max <= 1 do
    try do
      case Oban.insert(LongHorizonWorker.new(%{seq: seq, padding: padding})) do
        {:ok, _} -> :ok
        _ -> :error
      end
    rescue
      ArgumentError -> :stopping
      RuntimeError -> :stopping
    catch
      :exit, _reason -> :stopping
    end
  end

  defp insert_batch(seq, batch_count, padding, _batch_max) do
    changesets =
      for i <- 0..(batch_count - 1) do
        LongHorizonWorker.new(%{seq: seq + i, padding: padding})
      end

    try do
      # Documented bulk path: `Oban.insert_all/2`
      # https://hexdocs.pm/oban/Oban.html#insert_all/2 — issues a single
      # multi-row INSERT for the supplied changesets.
      case Oban.insert_all(changesets) do
        jobs when is_list(jobs) -> :ok
        _ -> :error
      end
    rescue
      ArgumentError -> :stopping
      RuntimeError -> :stopping
    catch
      :exit, _reason -> :stopping
    end
  end

  defp depth_loop do
    if not observer_enabled?() do
      # Non-zero replicas don't emit observer metrics; idle this
      # process instead of polling. Saves N-1 connections of polling
      # work on a multi-replica run.
      depth_idle_loop()
    else
      Process.sleep(1000)

      depth =
        try do
          case Repo.one(
                 from(j in "oban_jobs",
                   where: j.state == "available" and j.queue == "long_horizon_bench",
                   select: count(j.id)
                 )
               ) do
            n when is_integer(n) -> n
            _ -> 0
          end
        catch
          :exit, _reason -> :stopping
        end

      case depth do
        :stopping ->
          :ok

        value ->
          :ets.insert(:long_horizon_state, {:queue_depth, value})
          depth_loop()
      end
    end
  end

  defp depth_idle_loop do
    Process.sleep(250)
    depth_idle_loop()
  end

  defp sampler_loop(sample_every_s) do
    # Align to wall-clock boundary.
    now_epoch = System.system_time(:second)
    sleep_for_s = sample_every_s - rem(now_epoch, sample_every_s)
    Process.sleep(sleep_for_s * 1000)
    sampler_step(0, 0, System.monotonic_time(:millisecond), sample_every_s)
  end

  defp sampler_step(last_enq, last_cmp, last_tick_ms, sample_every_s) do
    Process.sleep(sample_every_s * 1000)

    [{:enqueued, enq}] = :ets.lookup(:long_horizon_state, :enqueued)
    [{:completed, cmp}] = :ets.lookup(:long_horizon_state, :completed)
    [{:queue_depth, depth}] = :ets.lookup(:long_horizon_state, :queue_depth)

    [{:producer_target_rate, producer_target_rate}] =
      :ets.lookup(:long_horizon_state, :producer_target_rate)

    now_ms = System.monotonic_time(:millisecond)
    dt_s = max(0.001, (now_ms - last_tick_ms) / 1000.0)
    enq_rate = (enq - last_enq) / dt_s
    cmp_rate = (cmp - last_cmp) / dt_s

    {p50, p95, p99} = percentiles_ms(@latency_window_s)
    ts = now_iso()

    Enum.each(
      [
        {"claim_p50_ms", p50, 30},
        {"claim_p95_ms", p95, 30},
        {"claim_p99_ms", p99, 30},
        {"enqueue_rate", enq_rate, sample_every_s},
        {"completion_rate", cmp_rate, sample_every_s},
        {"queue_depth", depth * 1.0, 0},
        {"producer_target_rate", producer_target_rate, 0}
      ],
      fn {name, value, window_s} ->
        if MapSet.member?(@observer_metrics, name) and not observer_enabled?() do
          :ok
        else
          emit(%{
            t: ts,
            system: "oban",
            kind: "adapter",
            subject_kind: "adapter",
            subject: "",
            metric: name,
            value: value,
            window_s: window_s
          })
        end
      end
    )

    sampler_step(enq, cmp, now_ms, sample_every_s)
  end

  defp percentiles_ms(window_s) do
    cutoff = System.monotonic_time(:millisecond) - trunc(window_s * 1000)

    values =
      :ets.tab2list(:long_horizon_lat)
      |> Enum.flat_map(fn {_, ts_ms, lat_ms} ->
        if ts_ms >= cutoff, do: [lat_ms], else: []
      end)

    case values do
      [] ->
        {0.0, 0.0, 0.0}

      _ ->
        sorted = Enum.sort(values)
        n = length(sorted)

        pick = fn q ->
          idx = min(n - 1, max(0, round(q * (n - 1))))
          Enum.at(sorted, idx) * 1.0
        end

        {pick.(0.50), pick.(0.95), pick.(0.99)}
    end
  end

  # Read BENCH_INSTANCE_ID (0 if unset or malformed). Stamped onto every
  # descriptor + sample record so the harness attributes samples to the
  # right replica without per-subprocess state.
  defp instance_id do
    case System.get_env("BENCH_INSTANCE_ID") do
      nil ->
        0

      raw ->
        case Integer.parse(raw) do
          {n, ""} -> n
          _ -> 0
        end
    end
  end

  # Mirror of awa-bench's `observer_enabled`. Only instance 0 emits
  # cross-system observer metrics (queue depth, total backlog, producer
  # target rate) so multi-replica runs report a single global observation
  # instead of one per replica that the summary aggregator would have to
  # de-duplicate later.
  defp observer_enabled?, do: instance_id() == 0

  defp emit(record) do
    record = Map.put_new(record, :instance_id, instance_id())
    IO.puts(Jason.encode!(record))
  end

  defp now_iso do
    DateTime.utc_now()
    |> DateTime.to_iso8601()
    |> String.replace(~r/\+00:00$/, "Z")
  end

  defp env_int(key, default) do
    case System.get_env(key) do
      nil -> default
      val -> String.to_integer(val)
    end
  end

  defp read_producer_rate(default, nil), do: default

  defp read_producer_rate(default, path) do
    case File.read(path) do
      {:ok, contents} ->
        case Float.parse(String.trim(contents)) do
          {value, _} when value >= 0 -> trunc(value)
          _ -> default
        end

      _ ->
        default
    end
  end
end

defmodule ObanBench.LongHorizonWorker do
  use Oban.Worker, queue: :long_horizon_bench, max_attempts: 3

  @impl Oban.Worker
  def perform(%Oban.Job{inserted_at: inserted_at}) do
    now = DateTime.utc_now()
    latency_ms = max(0, DateTime.diff(now, inserted_at, :millisecond))
    ts_ms = System.monotonic_time(:millisecond)

    :ets.insert(:long_horizon_lat, {:lat, ts_ms, latency_ms * 1.0})

    # Coarse trim: when the ETS bag gets too large, drop everything older than
    # the rolling window. Cheap enough at this frequency.
    if :rand.uniform(200) == 1 do
      cutoff = ts_ms - 30_000

      :ets.foldl(
        fn {_, t, _} = obj, acc ->
          if t < cutoff, do: [obj | acc], else: acc
        end,
        [],
        :long_horizon_lat
      )
      |> Enum.each(&:ets.delete_object(:long_horizon_lat, &1))
    end

    work_ms =
      case :ets.lookup(:long_horizon_state, :work_ms) do
        [{:work_ms, v}] -> v
        _ -> 1
      end

    if work_ms > 0, do: Process.sleep(work_ms)
    :ets.update_counter(:long_horizon_state, :completed, 1)
    :ok
  end
end
