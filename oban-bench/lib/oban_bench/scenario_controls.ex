defmodule ObanBench.ScenarioControls do
  @moduledoc """
  Scenario controls (CONTRIBUTING_ADAPTERS.md "Scenario controls").

  Default-off: `JOB_FAILURE_CONTROL_FILE` tags jobs enqueued while a failure
  plan is active as transient/poison failures; `SCHEDULE_CONTROL_FILE` asks
  instance 0 to enqueue a scheduled herd. Untagged jobs keep their payload
  and code path unchanged.
  """

  @state :scenario_controls
  @lateness :scenario_lateness
  @herd_batch_max 500

  def init do
    :ets.new(@state, [:public, :named_table, :set])
    :ets.new(@lateness, [:public, :named_table, :duplicate_bag])

    for key <- [:failed, :retried, :exhausted, :scheduled_done, :started, :enqueued, :early] do
      :ets.insert(@state, {key, 0})
    end

    :ets.insert(@state, {:preload_ms, -1})
    :ets.insert(@state, {:plan, %{}, -1_000_000})
    :ets.insert(@state, {:last_herd_id, nil})
    :ok
  end

  def failure_path, do: System.get_env("JOB_FAILURE_CONTROL_FILE")
  def schedule_path, do: System.get_env("SCHEDULE_CONTROL_FILE")

  def max_attempts do
    case Integer.parse(System.get_env("JOB_MAX_ATTEMPTS") || "") do
      {n, ""} when n > 0 -> n
      _ -> nil
    end
  end

  defp read_json(nil), do: %{}

  defp read_json(path) do
    with {:ok, body} <- File.read(path),
         {:ok, %{} = value} <- Jason.decode(body) do
      value
    else
      _ -> %{}
    end
  end

  @doc "Failure tag (string-keyed map) for `seq` under the current plan."
  def tag(seq) do
    case failure_path() do
      nil -> %{}
      path -> failure_tag(current_plan(path), seq)
    end
  end

  defp current_plan(path) do
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@state, :plan) do
      [{:plan, plan, read_at}] when now - read_at < 1000 ->
        plan

      _ ->
        plan = read_json(path)
        :ets.insert(@state, {:plan, plan, now})
        plan
    end
  end

  def failure_tag(plan, _seq) when map_size(plan) == 0, do: %{}

  def failure_tag(plan, seq) do
    poison_cut = round((plan["poison_pct"] || 0) * 100)
    transient_cut = poison_cut + round((plan["transient_pct"] || 0) * 100)
    bucket = rem(rem(seq, 10_000) * 7919, 10_000)

    cond do
      bucket < poison_cut -> %{"fail" => "poison"}
      bucket < transient_cut -> %{"fail" => "transient", "fail_attempts" => plan["transient_failures"] || 1}
      true -> %{}
    end
  end

  def tagged?(args), do: Map.has_key?(args, "fail") or Map.has_key?(args, "run_at_ms")

  @doc "nil = succeed, :retry, or :exhausted (failing on the last attempt)."
  def failure_outcome(args, attempt, max_attempts) do
    failing =
      case args["fail"] do
        "poison" -> true
        "transient" -> attempt <= max(args["fail_attempts"] || 1, 1)
        _ -> false
      end

    cond do
      not failing -> nil
      is_integer(max_attempts) and attempt >= max_attempts -> :exhausted
      true -> :retry
    end
  end

  def on_start(%{"run_at_ms" => run_at_ms}) do
    lateness = System.system_time(:millisecond) - run_at_ms
    if lateness < 0, do: :ets.update_counter(@state, :early, 1)
    :ets.insert(@lateness, {:l, max(lateness, 0) * 1.0})
    :ets.update_counter(@state, :started, 1)
  end

  def on_start(_args), do: :ok

  def record_failure(args, outcome) do
    :ets.update_counter(@state, :failed, 1)

    if outcome == :exhausted and args["fail"] == "poison" do
      :ets.update_counter(@state, :exhausted, 1)
    end
  end

  def on_complete(args) do
    if args["fail"] == "transient", do: :ets.update_counter(@state, :retried, 1)
    if Map.has_key?(args, "run_at_ms"), do: :ets.update_counter(@state, :scheduled_done, 1)
    :ok
  end

  defp count(key) do
    [{^key, value}] = :ets.lookup(@state, key)
    value
  end

  @doc "Instance 0 only: poll for herd commands and enqueue each herd."
  def herd_loop(insert_fun) do
    if schedule_path() != nil and instance_id() == 0 do
      herd_step(insert_fun)
    end
  end

  defp herd_step(insert_fun) do
    Process.sleep(1000)
    command = read_json(schedule_path())
    [{:last_herd_id, last_id}] = :ets.lookup(@state, :last_herd_id)

    case command do
      %{"id" => id} when id != last_id ->
        :ets.insert(@state, {:last_herd_id, id})
        :ets.insert(@state, {:preload_ms, -1})
        started = System.monotonic_time(:millisecond)

        Enum.reduce(herd_batches(command), 0, fn {size, run_at_ms}, seq ->
          insert_until_ok(insert_fun, seq, size, run_at_ms)
          :ets.update_counter(@state, :enqueued, size)
          seq + size
        end)

        elapsed = System.monotonic_time(:millisecond) - started
        :ets.insert(@state, {:preload_ms, elapsed})
        IO.puts(:stderr, "[oban] herd #{id}: #{command["count"]} jobs enqueued in #{elapsed / 1000}s")

      _ ->
        :ok
    end

    herd_step(insert_fun)
  end

  defp insert_until_ok(insert_fun, seq, size, run_at_ms) do
    case insert_fun.(seq, size, run_at_ms) do
      :ok ->
        :ok

      _ ->
        Process.sleep(200)
        insert_until_ok(insert_fun, seq, size, run_at_ms)
    end
  end

  # Mirrors adapter_common/bench_controls.py::herd_batches.
  def herd_batches(%{"count" => count, "run_at_ms" => run_at_ms} = command) do
    spread_ms = command["spread_ms"] || 0

    batch =
      if spread_ms <= 0,
        do: @herd_batch_max,
        else: min(max(div(count * 100, spread_ms), 1), @herd_batch_max)

    Stream.unfold(0, fn
      done when done >= count ->
        nil

      done ->
        size = min(batch, count - done)
        offset = if spread_ms > 0, do: div(spread_ms * done, count), else: 0
        {{size, run_at_ms + offset}, done + size}
    end)
  end

  defp rate(name, value, dt_s) do
    last =
      case :ets.lookup(@state, {:last, name}) do
        [{_, v}] -> v
        _ -> 0
      end

    :ets.insert(@state, {{:last, name}, value})
    (value - last) / dt_s
  end

  @doc "Extra sampler metrics as {name, value, window_s} tuples."
  def metrics(dt_s, window_s) do
    failure =
      if failure_path() != nil do
        [
          {"injected_failure_rate", rate(:failed, count(:failed), dt_s), window_s},
          {"retried_completion_rate", rate(:retried, count(:retried), dt_s), window_s},
          {"poison_exhausted_rate", rate(:exhausted, count(:exhausted), dt_s), window_s}
        ]
      else
        []
      end

    started = count(:started)
    enqueued = count(:enqueued)

    schedule =
      if schedule_path() != nil and (started > 0 or enqueued > 0) do
        values = :ets.tab2list(@lateness) |> Enum.map(fn {_, v} -> v end) |> Enum.sort()
        n = length(values)
        tuple = List.to_tuple(values)
        q = fn p -> elem(tuple, min(n - 1, max(0, round(p * (n - 1))))) end

        lateness =
          if n > 0 do
            [
              {"schedule_lateness_p50_ms", q.(0.50), 0},
              {"schedule_lateness_p95_ms", q.(0.95), 0},
              {"schedule_lateness_p99_ms", q.(0.99), 0},
              {"schedule_lateness_max_ms", elem(tuple, n - 1), 0}
            ]
          else
            []
          end

        preload =
          case count(:preload_ms) do
            ms when ms >= 0 -> [{"schedule_preload_s", ms / 1000, 0}]
            _ -> []
          end

        [{"scheduled_completion_rate", rate(:scheduled_done, count(:scheduled_done), dt_s), window_s}] ++
          lateness ++
          [
            {"schedule_started_total", started * 1.0, 0},
            {"schedule_enqueued_total", enqueued * 1.0, 0},
            {"schedule_early_total", count(:early) * 1.0, 0}
          ] ++ preload
      else
        []
      end

    failure ++ schedule
  end

  defp instance_id do
    case Integer.parse(System.get_env("BENCH_INSTANCE_ID") || "0") do
      {n, ""} -> n
      _ -> 0
    end
  end
end
