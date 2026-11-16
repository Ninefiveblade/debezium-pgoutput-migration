-- Read-only checks used before, during and after moving Debezium connectors to pgoutput.
-- Run with psql against the primary (not through pgbouncer).

-- 1. Logical slots: how much WAL each one pins and how far behind it is.
SELECT slot_name,
       plugin,
       active,
       wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))         AS restart_lag,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS flush_lag,
       pg_size_pretty(safe_wal_size)                                              AS safe_wal_size
FROM pg_replication_slots
WHERE slot_type = 'logical'
ORDER BY pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;

-- 2. Walsenders: is each connector actually streaming, and over TLS or not.
SELECT sl.slot_name,
       r.pid,
       r.state,
       r.backend_start,
       s.ssl
FROM pg_stat_replication r
LEFT JOIN pg_replication_slots sl ON sl.active_pid = r.pid
LEFT JOIN pg_stat_ssl s ON s.pid = r.pid
ORDER BY sl.slot_name;

-- 3. Long transactions. They hold back restart_lsn of every logical slot, block
--    pg_create_logical_replication_slot(), and make each reconnect re-decode from far behind.
SELECT pid,
       usename,
       application_name,
       now() - xact_start AS xact_age,
       state,
       left(query, 80)    AS query
FROM pg_stat_activity
WHERE xact_start < now() - interval '5 minutes'
ORDER BY xact_start;

-- 4. What each publication contains. With publication.autocreate.mode=disabled this must equal
--    table.include.list plus the heartbeat and signal tables.
SELECT pubname, schemaname, tablename
FROM pg_publication_tables
ORDER BY pubname, schemaname, tablename;

-- 5. Published tables without a usable replica identity: UPDATE/DELETE on them fails
--    once they are in a publication.
SELECT pt.pubname, pt.schemaname, pt.tablename, c.relreplident
FROM pg_publication_tables pt
JOIN pg_namespace n ON n.nspname = pt.schemaname
JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
WHERE c.relreplident = 'n'
   OR (c.relreplident = 'd' AND NOT EXISTS (
         SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary));

-- 6. Switch gate: the old connector has committed past the point where the new slot starts.
--    Set old_slot and new_slot_lsn (the lsn returned by pg_create_logical_replication_slot).
-- \set old_slot 'dbz_orders_slot'
-- \set new_slot_lsn '0/0'
SELECT slot_name,
       confirmed_flush_lsn,
       pg_wal_lsn_diff(confirmed_flush_lsn, :'new_slot_lsn') >= 0 AS safe_to_switch
FROM pg_replication_slots
WHERE slot_name = :'old_slot';
