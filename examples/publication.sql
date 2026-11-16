-- With publication.autocreate.mode=disabled the publication is yours to maintain.
-- It must contain every table from table.include.list plus the heartbeat and signal tables.

CREATE TABLE IF NOT EXISTS inventory.debezium_heartbeat (
    id             int PRIMARY KEY,
    last_heartbeat timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory.debezium_signal_orders (
    id   varchar(42) PRIMARY KEY,
    type varchar(32) NOT NULL,
    data varchar(2048)
);

CREATE PUBLICATION dbz_orders_publication FOR TABLE
    app.orders,
    app.order_items,
    inventory.debezium_heartbeat,
    inventory.debezium_signal_orders
WITH (publish = 'insert, update, delete, truncate');

-- Only where consumers need the full "before" image or the table has no primary key.
-- FULL writes the whole old row into WAL on every UPDATE and DELETE.
-- ALTER TABLE app.order_items REPLICA IDENTITY FULL;

-- Adding a table later: publication first, include list second, then backfill.
-- ALTER PUBLICATION dbz_orders_publication ADD TABLE app.refunds;
