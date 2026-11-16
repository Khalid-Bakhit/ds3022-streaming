"""Prefect flow: raw Parquet -> DuckDB (both kinds of duplicates removed) -> marts -> charts.

  python flow.py                # full data: data/raw
  python flow.py data/sample    # the committed sample (no Kafka needed)

Adapted from the class script quakes_flow.py (Mon 02 Nov).
Research question: is there more buying or selling pressure on Coinbase,
and does it differ by product? Measured per product in 1-minute tumbling
windows by trade time (event time).
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

DB = "coinbase.duckdb"
RAW = (sys.argv[1] if len(sys.argv) > 1 else "data/raw") + "/*.parquet"
CHART_MINUTES = "charts/buy_share_per_minute.png"
CHART_PRODUCTS = "charts/buy_share_by_product.png"


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    """Every landed row, as it is (malformed and duplicates included)."""
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of coinbase.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_trades AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_trades").fetchone()[0]


@task
def stage():
    """Duplicate kind 1: the same Kafka record landed twice (at-least-once).
    Then keep only real trades and give them proper types."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_trades
            WHERE action <> 'malformed'
        """)
        # 'subscriptions' and 'last_match' are not new trades: drop them here.
        # Coinbase's 'side' is the MAKER's side; the taker (who started the trade)
        # did the opposite. Buying pressure = trades started by buyers.
        con.execute("""
            CREATE OR REPLACE TABLE stg_trades AS
            SELECT kafka_partition, kafka_offset, product_id, trade_id,
                   CAST(event_time AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS trade_time,
                   price, size, price * size AS usd,
                   CASE side WHEN 'sell' THEN 'buy' WHEN 'buy' THEN 'sell' END AS taker_side
            FROM stg_records
            WHERE msg_type = 'match'
              AND product_id IS NOT NULL AND trade_id IS NOT NULL
              AND price > 0 AND size > 0 AND event_time IS NOT NULL
        """)
        return con.execute("SELECT count(*) FROM stg_records").fetchone()[0]


@task
def entities():
    """Duplicate kind 2: the same trade sent twice. A trade never changes,
    so one row per product_id + trade_id (the first one landed)."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE trades AS
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *, row_number() OVER (PARTITION BY product_id, trade_id
                                             ORDER BY kafka_partition, kafka_offset) AS rn
                FROM stg_trades
            )
            WHERE rn = 1
        """)
        return con.execute("SELECT count(*) FROM trades").fetchone()[0]


@task
def marts():
    """1-minute tumbling windows by trade time, per product: who pushed the price?"""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_pressure_1min AS
            SELECT product_id,
                   time_bucket(INTERVAL '1 minute', trade_time) AS minute,
                   count(*) AS trades,
                   sum(usd) FILTER (WHERE taker_side = 'buy')  AS buy_usd,
                   sum(usd) FILTER (WHERE taker_side = 'sell') AS sell_usd,
                   sum(usd) AS total_usd,
                   coalesce(sum(usd) FILTER (WHERE taker_side = 'buy'), 0) / sum(usd) AS buy_share,
                   arg_min(price, trade_time) AS open_price,
                   arg_max(price, trade_time) AS close_price
            FROM trades
            GROUP BY ALL
        """)
        con.execute("""
            CREATE OR REPLACE TABLE mart_pressure_product AS
            SELECT product_id,
                   count(*) AS trades,
                   sum(usd) AS total_usd,
                   coalesce(sum(usd) FILTER (WHERE taker_side = 'buy'), 0) / sum(usd) AS buy_share,
                   min(trade_time) AS first_trade,
                   max(trade_time) AS last_trade
            FROM trades
            GROUP BY product_id
            ORDER BY total_usd DESC
        """)
        return {
            "malformed": con.execute("SELECT count(*) FROM raw_trades WHERE action = 'malformed'").fetchone()[0],
            "minutes": con.execute("SELECT count(*) FROM mart_pressure_1min").fetchone()[0],
            "by_product": con.execute("SELECT product_id, trades, round(buy_share, 3) FROM mart_pressure_product").fetchall(),
        }


@task
def chart_minutes():
    """Buy share of USD volume per minute for the two busiest products."""
    with duckdb.connect(DB, read_only=True) as con:
        top = [r[0] for r in con.execute(
            "SELECT product_id FROM mart_pressure_product ORDER BY trades DESC LIMIT 2").fetchall()]
        os.makedirs("charts", exist_ok=True)
        plt.figure(figsize=(10, 4.5))
        for product in top:
            rows = con.execute("""
                SELECT minute, buy_share FROM mart_pressure_1min
                WHERE product_id = ? ORDER BY minute
            """, [product]).fetchall()
            xs, ys = [], []
            for i, (minute, share) in enumerate(rows):
                if i and (minute - rows[i - 1][0]).total_seconds() > 60:
                    xs.append(minute)  # a minute with no data (producer stopped):
                    ys.append(None)    # break the line instead of drawing across it
                xs.append(minute)
                ys.append(100 * share)
            plt.plot(xs, ys, marker=".", label=product)
    plt.axhline(50, color="grey", linestyle="--", linewidth=1, label="balanced (50%)")
    plt.ylim(0, 100)
    plt.title("Buying pressure per minute: share of USD volume from buyer-initiated trades")
    plt.xlabel("Trade time (UTC), 1-minute tumbling windows")
    plt.ylabel("Buyer-initiated share of volume (%)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(CHART_MINUTES, dpi=120)
    plt.close()
    return CHART_MINUTES


@task
def chart_products():
    """Overall buy share by product, with the number of trades."""
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute(
            "SELECT product_id, buy_share, trades FROM mart_pressure_product ORDER BY trades DESC").fetchall()
    os.makedirs("charts", exist_ok=True)
    plt.figure(figsize=(8, 4.5))
    bars = plt.bar([r[0] for r in rows], [100 * r[1] for r in rows], color="#2a7fb8")
    for bar, r in zip(bars, rows):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1, f"{r[2]:,} trades",
                 ha="center", fontsize=8)
    plt.axhline(50, color="grey", linestyle="--", linewidth=1)
    plt.ylim(0, 100)
    plt.title("Buyer-initiated share of USD volume by product (whole collection)")
    plt.xlabel("Product")
    plt.ylabel("Buyer-initiated share of volume (%)")
    plt.tight_layout()
    plt.savefig(CHART_PRODUCTS, dpi=120)
    plt.close()
    return CHART_PRODUCTS


@flow(name="coinbase-pipeline")
def coinbase_pipeline():
    log = get_run_logger()
    raw = load_raw()
    records = stage()
    n_trades = entities()
    m = marts()
    paths = [chart_minutes(), chart_products()]
    log.info("Data folder:               %s", RAW)
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", m["malformed"])
    log.info("Unique Kafka records:      %d", records)
    log.info("Unique trades:             %d", n_trades)
    log.info("1-minute windows:          %d", m["minutes"])
    for product, trades, share in m["by_product"]:
        log.info("  %-9s %7d trades  buyer-initiated share %.1f%%", product, trades, 100 * share)
    log.info("Charts: %s", ", ".join(paths))


if __name__ == "__main__":
    coinbase_pipeline()
