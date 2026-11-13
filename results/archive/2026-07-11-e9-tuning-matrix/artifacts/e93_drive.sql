\timing on
SET auto_explain.log_min_duration = 0;
SET auto_explain.log_analyze = on;
SET auto_explain.log_nested_statements = on;
SHOW plan_cache_mode;
SELECT 1 AS exec_no, count(*) AS claimed FROM awa.claim_ready_runtime('awa_longhorizon_bench', 512, 0, 0);
SELECT 2 AS exec_no, count(*) AS claimed FROM awa.claim_ready_runtime('awa_longhorizon_bench', 512, 0, 0);
SELECT 3 AS exec_no, count(*) AS claimed FROM awa.claim_ready_runtime('awa_longhorizon_bench', 512, 0, 0);
SELECT 4 AS exec_no, count(*) AS claimed FROM awa.claim_ready_runtime('awa_longhorizon_bench', 512, 0, 0);
SELECT 5 AS exec_no, count(*) AS claimed FROM awa.claim_ready_runtime('awa_longhorizon_bench', 512, 0, 0);
SELECT 6 AS exec_no, count(*) AS claimed FROM awa.claim_ready_runtime('awa_longhorizon_bench', 512, 0, 0);
SELECT 7 AS exec_no, count(*) AS claimed FROM awa.claim_ready_runtime('awa_longhorizon_bench', 512, 0, 0);
SELECT 8 AS exec_no, count(*) AS claimed FROM awa.claim_ready_runtime('awa_longhorizon_bench', 512, 0, 0);
