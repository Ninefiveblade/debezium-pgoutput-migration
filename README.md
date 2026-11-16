# Debezium: moving PostgreSQL connectors from decoderbufs to pgoutput

Notes from moving 7 production Debezium PostgreSQL connectors from `decoderbufs` to `pgoutput`
without a snapshot and without a gap in the change stream. Network traffic from PG to Kafka Connect
went down about 6×, and along the way we got rid of daily replication drops caused by
`SSL error: bad length` on PostgreSQL 18.

Stack: PostgreSQL 18.3, Debezium 3.5.0.Final, pgjdbc 42.7.7, Kafka / Kafka Connect 4.1.1,
ClickHouse sinks (ReplacingMergeTree). Source tables go up to ~400M rows, WAL generation
60–100 MB/s during the day.

- [Why: decoderbufs ships everything to every slot](#why-decoderbufs-ships-everything-to-every-slot)
- [Result](#result)
- [Before you start](#before-you-start)
- [Migration of one connector, no gap](#migration-of-one-connector-no-gap)
- [Living with pgoutput](#living-with-pgoutput)
- [PG 18: "SSL error: bad length" on walsenders](#pg-18-ssl-error-bad-length-on-walsenders)
- [Other things that cost us time](#other-things-that-cost-us-time)
- Files: [`examples/connector.json`](examples/connector.json),
  [`examples/publication.sql`](examples/publication.sql), [`sql/checks.sql`](sql/checks.sql)

## Why: decoderbufs ships everything to every slot

`decoderbufs` ignores publications. Every slot decodes the full WAL change stream and sends all of it
to the connector, and Debezium throws away what is not in `table.include.list` on the client side.

With one connector that is fine. With 7 connectors on one database it means 7 walsenders each
pushing the whole change stream over the wire. In our case Connect sat in another data center
(~40 ms RTT), every connection carried ~11 MB/s, 65+ MB/s in total, and more than 90% of the volume
belonged to a single schema that only one connector actually needed.

`pgoutput` filters by publication on the server side: a slot only sends changes of the tables
in its publication. It is also built into PostgreSQL, so there is no extension to build and upgrade
together with the server.

## Result

| | decoderbufs | pgoutput |
|---|---|---|
| Busiest connection | ~11 MB/s | 10.5 MB/s |
| Other 6 connections | ~11 MB/s each | 0–1 MB/s each |
| Total PG → Connect | 65+ MB/s | ~11 MB/s |

All 7 connectors were moved in one evening, one by one. Small connectors switched in under a minute,
two waited 27–41 minutes for long transactions (see step 2 below). Old slots were dropped right after
verification and released their pinned WAL; one of them was holding 1.4 TB.

## Before you start

1. **Every published table needs a replica identity.** A primary key is enough. Tables without one
   need `REPLICA IDENTITY FULL` (or a unique index with `USING INDEX`), otherwise UPDATE and DELETE
   on them start failing the moment they are added to a publication. Query 5 in
   [`sql/checks.sql`](sql/checks.sql) lists offenders.
2. **Decide who owns the publication.** `publication.autocreate.mode=filtered` lets Debezium create
   and alter it from the include list, which requires the connector user to own the tables.
   We use `disabled`: the publication is created and changed by hand and reviewed like any
   other DDL. Then it must exist before the connector starts.
3. **Heartbeat and signal tables go into the publication.** With pgoutput a slot only moves forward
   when it sees changes of published tables. If the heartbeat table is not published, a connector
   whose tables are quiet never advances `confirmed_flush_lsn`, and the slot pins WAL forever.
   Same for the signal table: if it is not published, signals never arrive.
4. **Your sink must survive duplicates.** The switch below is at-least-once: a few events around
   the switch point can be delivered twice. An upserting sink (ReplacingMergeTree, `INSERT ... ON
   CONFLICT`, Kafka compaction by key) absorbs them.
5. **Check long transactions** (query 3). They matter in step 2.

## Migration of one connector, no gap

The idea: create the new pgoutput slot while the old connector keeps running, wait until the old
connector has committed past the new slot's starting point, then switch the connector config
to the new slot. Debezium keeps its stored offset, so it resumes from where it was; nothing between
the two slots is lost.

**1. Create the publication** (see [`examples/publication.sql`](examples/publication.sql)) with
exactly the tables of `table.include.list` plus heartbeat and signal tables.

**2. Create the new slot yourself:**

```sql
SELECT * FROM pg_create_logical_replication_slot('dbz_orders_slot_pgout', 'pgoutput');
-- returns lsn, remember it as new_slot_lsn
```

This call waits until every transaction that was running at that moment finishes.
With a bulk loader running hour-long transactions it can hang for a long time: for us it took
27–41 minutes on two connectors. It is not stuck, it is waiting for a consistent point.

**3. Gate: the old connector must be past `new_slot_lsn`.**

```sql
SELECT pg_wal_lsn_diff(confirmed_flush_lsn, '<new_slot_lsn>') >= 0 AS safe_to_switch
FROM pg_replication_slots WHERE slot_name = '<old_slot>';
```

Wait until it is `true`. If you switch earlier, Debezium's stored offset is behind the new slot's
start and the changes in between are gone.

**4. Switch the connector.** GET the current config, change these keys, PUT the whole document back
(a PUT restarts the connector):

```json
"plugin.name": "pgoutput",
"slot.name": "dbz_orders_slot_pgout",
"publication.name": "dbz_orders_publication",
"publication.autocreate.mode": "disabled"
```

Keep `topic.prefix` and the connector name. The topic names stay the same, and the offsets stay
with the connector.

**5. Verify.**

- Connector and task are `RUNNING`.
- The new slot is `active` and its walsender is `streaming` (queries 1 and 2).
- End-to-end probe: update one row in a published table and see the event in the topic.
  For us it took 11 seconds.
- On restart Debezium first logs `Searching for WAL resume position`: it scans from `restart_lsn`
  to the stored offset. A new walsender pid during this phase is not a disconnect.

**6. Drop the old slot right away.**

```sql
SELECT pg_drop_replication_slot('<old_slot>');
```

An inactive slot keeps pinning WAL until it is dropped. And dropping does not free disk immediately:
WAL is removed only after the next checkpoint completes. With 1.4 TB pinned that took tens of minutes.

**Rollback:** until step 6 the old slot still exists. PUT the old config back.

## Living with pgoutput

- **Adding a table:** `ALTER PUBLICATION ... ADD TABLE`, then add it to `table.include.list`
  and PUT the config. Changes made before the table is in both are not delivered, so backfill
  only after both steps.
- **Backfill without a snapshot:** with `snapshot.mode=no_data` existing rows do not appear by
  themselves. For a small table `UPDATE t SET col = col` emits an update for every row.
  For a big one do not do that (WAL storm, bloat): load the history into the sink directly
  from PG by primary key ranges and let the stream take it from there. Incremental snapshots
  are an option, but on a 384M-row table ours never finished.
- **Removing a table:** remove it from the include list and from the publication. Leftovers in the
  publication still cost decoding and network.

## PG 18: "SSL error: bad length" on walsenders

Before the migration every connector dropped 1–7 times a day, 87 drops in one week across 7 slots.
On the PG side:

```
LOG:  SSL error: bad length
CONTEXT:  slot "...", output plugin "decoderbufs", in the change callback, associated LSN ...
LOG:  could not receive data from client: Connection reset by peer
```

On the Debezium side `SocketException: Socket is closed` and a retriable restart 10 seconds later.
Each restart was expensive: the walsender re-decodes WAL from `restart_lsn` (see below).

`bad length` is OpenSSL's `SSL_R_BAD_LENGTH`: a retried `SSL_write()` was called with less data
than the pending partial write. It happens in the server-side TLS write path of the walsender under
sustained load. It looks related to PostgreSQL bug #18907, but that one was fixed on the libpq side,
and this one is in the backend. We did not find an upstream fix as of August 2026.

**Workaround:** `database.sslmode=disable` on the connectors. Before: up to 46 drops a day.
After: 0 drops over the next 27+ hours, checked against the full PG log. Do this only if the link is otherwise protected (private network or an encrypted
tunnel) and `pg_hba.conf` allows plain `host` connections for the replication user.

## Other things that cost us time

- **The connector stays RUNNING while PostgreSQL is down.** Debezium retries the connection
  internally, so the task state never becomes FAILED. During an 8-hour PG outage every alert on
  connector state stayed green. Alert on the slot instead: `active`, walsender `state`, and lag
  in bytes (queries 1–2).
- **Long transactions are paid for on every reconnect.** All logical slots share the same
  `restart_lsn` floor: the oldest running transaction. Our loader ran 1–1.5 hour transactions,
  so `restart_lsn` was 270–480 GB behind, and each reconnect re-decoded all of it: 20–35 minutes
  to catch up. Capping transaction length on the writer side fixed more than any connector setting.
- **A slot that nobody reads fills the disk.** We got `PANIC: could not write to file
  "pg_wal/xlogtemp": No space left on device`. Set `max_slot_wal_keep_size`, monitor `wal_status`
  and `safe_wal_size`, and drop slots you no longer use.
- **`REPLICA IDENTITY FULL` is not free.** Every UPDATE writes the whole old row into WAL.
  Use it only where consumers need the full before-image or there is no key.
- **Small `max_wal_size` at high write rates.** With 220+ GB/h of WAL and `max_wal_size=160GB`,
  checkpoints were triggered by size every ~19 minutes, and each one restarts full-page writes.
  Raising it to 512 GB (and `checkpoint_timeout` to 1h) cut that loop.
- **`pg_stat_statements.wal_bytes` lies about timing.** Statistics are recorded when a statement
  finishes, so a multi-hour statement shows up as a sudden "explosion". Measure live WAL rate with
  `pg_current_wal_lsn()` deltas instead.

## License

MIT
