-- From Tiger Data's "Ingest real-time financial data" tutorial.
-- Run each statement in the Tiger Console SQL editor.

CREATE TABLE crypto_ticks (
    "time" TIMESTAMPTZ,
    symbol TEXT,
    price DOUBLE PRECISION,
    day_volume NUMERIC
) WITH (
   tsdb.hypertable,
   tsdb.segmentby='symbol',
   tsdb.orderby='time DESC'
);

CREATE TABLE crypto_assets (
    symbol TEXT UNIQUE,
    "name" TEXT
);

INSERT INTO crypto_assets (symbol, "name") VALUES
  ('BTC/USD', 'Bitcoin'),
  ('ETH/USD', 'Ethereum');

-- After the script has run for a minute or two:
SELECT a."name", t.symbol, count(*) AS ticks, round(avg(t.price)::numeric, 2) AS avg_price, max(t."time") AS latest
FROM crypto_ticks t
JOIN crypto_assets a ON a.symbol = t.symbol
GROUP BY a."name", t.symbol
ORDER BY t.symbol;
