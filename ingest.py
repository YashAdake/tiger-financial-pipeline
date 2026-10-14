"""Stream real-time crypto prices from Twelve Data into a Tiger Cloud hypertable.

Based on Tiger Data's "Ingest real-time financial data" tutorial:
https://www.tigerdata.com/docs/build/examples/ingest-real-time-financial-data

Secrets are never stored in this file. The Twelve Data API key and the database
password are read from environment variables, or asked for with a hidden prompt.
Press Ctrl+C to stop; any rows still in the current batch are written first.
"""

import os
import time
from datetime import datetime, timezone
from getpass import getpass

import psycopg2
from psycopg2.extras import execute_values
from twelvedata import TDClient

DB_HOST = os.environ.get("TIGER_DB_HOST", "wf4o5xirjp.vjg8lwuqgr.tsdb.cloud.timescale.com")
DB_PORT = int(os.environ.get("TIGER_DB_PORT", "35625"))
DB_NAME = os.environ.get("TIGER_DB_NAME", "tsdb")
DB_USER = os.environ.get("TIGER_DB_USER", "tsdbadmin")

SYMBOLS = ["BTC/USD", "ETH/USD"]
MAX_BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))


class WebsocketPipeline:
    DB_TABLE = "crypto_ticks"
    DB_COLUMNS = ["time", "symbol", "price", "day_volume"]

    def __init__(self, conn, api_key):
        self.conn = conn
        self.api_key = api_key
        self.current_batch = []
        self.insert_counter = 0
        self.rows_written = 0

    def _insert_values(self, data):
        sql = f"INSERT INTO {self.DB_TABLE} ({','.join(self.DB_COLUMNS)}) VALUES %s;"
        with self.conn.cursor() as cursor:
            execute_values(cursor, sql, data)
        self.conn.commit()
        self.insert_counter += 1
        self.rows_written += len(data)
        print(f"Batch insert #{self.insert_counter}: {len(data)} rows (total {self.rows_written})")

    def _on_event(self, event):
        if event.get("event") == "subscribe-status":
            print(f"Subscription: {event.get('status')} {event.get('success') or ''} {event.get('fails') or ''}")
            return
        if event.get("event") != "price":
            return
        timestamp = datetime.fromtimestamp(event["timestamp"], tz=timezone.utc)
        row = (timestamp, event["symbol"], event["price"], event.get("day_volume"))
        self.current_batch.append(row)
        print(f"{timestamp:%H:%M:%S} {event['symbol']} {event['price']}  (batch {len(self.current_batch)}/{MAX_BATCH_SIZE})")
        if len(self.current_batch) >= MAX_BATCH_SIZE:
            self._insert_values(self.current_batch)
            self.current_batch = []

    def start(self, symbols):
        td = TDClient(apikey=self.api_key)
        ws = td.websocket(on_event=self._on_event)
        ws.subscribe(symbols)
        ws.connect()
        print(f"Streaming {', '.join(symbols)} into {self.DB_TABLE}. Press Ctrl+C to stop.")
        try:
            while True:
                ws.heartbeat()
                time.sleep(10)
        except KeyboardInterrupt:
            print("\nStopping...")
        finally:
            if self.current_batch:
                self._insert_values(self.current_batch)
                self.current_batch = []
            ws.disconnect()
            print(f"Done. Wrote {self.rows_written} rows in {self.insert_counter} batches.")


def main():
    api_key = os.environ.get("TWELVE_DATA_API_KEY") or getpass("Twelve Data API key (hidden): ")
    password = os.environ.get("TIGER_DB_PASSWORD") or getpass("Tiger database password (hidden): ")
    conn = psycopg2.connect(
        dbname=DB_NAME, host=DB_HOST, port=DB_PORT, user=DB_USER, password=password, sslmode="require"
    )
    try:
        WebsocketPipeline(conn, api_key).start(SYMBOLS)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
