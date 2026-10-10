import Config

config :oban_bench, ecto_repos: [ObanBench.Repo]

worker_count =
  case System.get_env("WORKER_COUNT") do
    nil -> 50
    val -> String.to_integer(val)
  end

pool_size =
  case System.get_env("MAX_CONNECTIONS") do
    nil -> max(20, worker_count + 8)
    val -> String.to_integer(val)
  end

config :oban_bench, ObanBench.Repo,
  url: System.get_env("DATABASE_URL", "postgres://bench:bench@localhost:15555/oban_bench"),
  pool_size: pool_size

# Oban config — queues will be overridden at runtime by the benchmark runner
rescue_after_secs =
  case System.get_env("RESCUE_AFTER_SECS") do
    nil -> 15
    val -> String.to_integer(val)
  end

config :oban_bench, Oban,
  engine: Oban.Engines.Basic,
  repo: ObanBench.Repo,
  queues: [benchmark: 50, chaos: 10, long_horizon_bench: 32],
  lifeline: [rescue_after: :timer.seconds(rescue_after_secs)]

config :logger, level: :warning
