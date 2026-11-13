import Config

config :oban_bench, ecto_repos: [ObanBench.Repo]

worker_count =
  case System.get_env("WORKER_COUNT") do
    nil -> 50
    val -> String.to_integer(val)
  end

pool_size =
  case System.get_env("MAX_CONNECTIONS") do
    nil -> max(20, min(worker_count, 100) + 8)
    val -> String.to_integer(val)
  end

config :oban_bench, ObanBench.Repo,
  url: System.get_env("DATABASE_URL", "postgres://bench:bench@localhost:15555/oban_bench"),
  pool_size: pool_size,
  timeout: String.to_integer(System.get_env("REPO_TIMEOUT_MS", "15000"))

# Oban config — queues will be overridden at runtime by the benchmark runner
rescue_after_secs =
  case System.get_env("RESCUE_AFTER_SECS") do
    nil -> 15
    val -> String.to_integer(val)
  end

oban_node =
  "oban-bench-#{System.get_env("BENCH_INSTANCE_ID", "0")}-" <>
    Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)

# Unset keeps Oban's default dispatch_cooldown (5 ms).
dispatch_cooldown_opts =
  case System.get_env("OBAN_DISPATCH_COOLDOWN_MS") do
    nil -> []
    val -> [dispatch_cooldown: String.to_integer(val)]
  end

oban_opts =
  [
    engine: Oban.Engines.Basic,
    repo: ObanBench.Repo,
    node: oban_node,
    queues: [benchmark: 50, chaos: 10],
    lifeline: [rescue_after: :timer.seconds(rescue_after_secs)]
  ] ++ dispatch_cooldown_opts

config :oban_bench, Oban, oban_opts

config :logger, level: :warning
