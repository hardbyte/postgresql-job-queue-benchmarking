-- Test pgque-specific additions
-- Run against a database with pgque installed

-- Test 1: config table
do $$
begin
    assert (select count(*) from pgque.config) = 1, 'config should have exactly 1 row';
    assert (select singleton from pgque.config) = true, 'singleton should be true';
    raise notice 'PASS: config table';
end $$;

-- Test 2: queue_max_retries column exists
do $$
begin
    assert exists (
        select 1 from information_schema.columns
        where table_schema = 'pgque' and table_name = 'queue'
        and column_name = 'queue_max_retries'
    ), 'queue_max_retries column should exist';
    raise notice 'PASS: queue_max_retries column';
end $$;

-- Test 3: roles exist
do $$
begin
    assert exists (select 1 from pg_roles where rolname = 'pgque_reader'), 'pgque_reader role should exist';
    assert exists (select 1 from pg_roles where rolname = 'pgque_writer'), 'pgque_writer role should exist';
    assert exists (select 1 from pg_roles where rolname = 'pgque_admin'), 'pgque_admin role should exist';
    raise notice 'PASS: roles exist';
end $$;

-- Test 4: lifecycle functions exist
do $$
begin
    perform pgque.version();
    raise notice 'PASS: version() works, returned %', pgque.version();
end $$;

-- Test 5: idempotency - re-running additions should not error
-- (This will be tested as part of install idempotency in Issue #4)
