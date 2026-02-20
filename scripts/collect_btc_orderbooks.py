#!/usr/bin/env python3
"""Polymarket BTC 5-Min Latency Arb — Orderbook Collector.

Collects synchronized high-frequency data from Binance and Polymarket
for BTC 5-minute up/down prediction markets.

Three concurrent async tasks:
1. Binance stream — every BTC/USDT trade tick (~200ms)
2. Polymarket poller — orderbook snapshots every 1s, trades every 5s
3. Market manager — discovers/rotates active 5-min markets every 30s

Market discovery uses the deterministic slug pattern:
  btc-updown-5m-{epoch}  where epoch = floor(now / 300) * 300
  fetched via: GET gamma-api.polymarket.com/events/slug/{slug}

Storage: data/btc_arb_collector/{date}/*.jsonl.gz (one dir per UTC day)

Usage:
    uv run scripts/collect_btc_orderbooks.py
    BINANCE_WS=wss://stream.binance.us:9443/ws/btcusdt@trade uv run scripts/collect_btc_orderbooks.py
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import websockets

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATA_DIR = Path(os.environ.get("DATA_DIR", "data/btc_arb_collector"))

# Binance: default to .com (works outside US), set env var for .us if in US
BINANCE_WS_URL = os.environ.get(
    "BINANCE_WS",
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
)
CLOB_API_URL = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"

ORDERBOOK_INTERVAL = 1.0  # seconds between orderbook polls
MARKET_CHECK_INTERVAL = 10.0  # seconds between market discovery checks

# Slug pattern: btc-updown-5m-{epoch} where epoch = start of 5-min window
SLUG_PREFIX = "btc-updown-5m-"
WINDOW_SECONDS = 300  # 5 minutes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("btc_collector")


# ---------------------------------------------------------------------------
# JSONL gzip writer (one file per type per day, rotates at midnight UTC)
# ---------------------------------------------------------------------------


class JsonlWriter:
    """Append-only gzip JSONL writer with daily rotation."""

    def __init__(self, base_dir: Path, filename: str):
        self.base_dir = base_dir
        self.filename = filename
        self._current_date: str | None = None
        self._file = None
        self._records = 0

    def _ensure_open(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._current_date:
            self.close()
            self._current_date = today
            day_dir = self.base_dir / today
            day_dir.mkdir(parents=True, exist_ok=True)
            path = day_dir / self.filename
            self._file = gzip.open(path, "at", encoding="utf-8")
            log.info("Opened %s", path)
        return self._file

    def write(self, record: dict):
        f = self._ensure_open()
        f.write(json.dumps(record, separators=(",", ":")) + "\n")
        self._records += 1
        # Flush every 10 records so data survives unclean shutdown
        if self._records % 10 == 0:
            f.flush()

    def close(self):
        if self._file:
            self._file.close()
            self._file = None


# ---------------------------------------------------------------------------
# Active market state
# ---------------------------------------------------------------------------


class ActiveMarket:
    """Holds the currently-tracked 5-min BTC market."""

    def __init__(self):
        self.slug: str | None = None
        self.condition_id: str | None = None
        self.token_up: str | None = None
        self.token_down: str | None = None
        self.end_time: str | None = None
        self.end_ts_ms: int | None = None
        self.question: str | None = None
        self.extra: dict = {}
        self.lock = asyncio.Lock()

    def is_active(self) -> bool:
        if self.end_ts_ms is None:
            return False
        return _now_ms() < self.end_ts_ms

    def clear(self):
        self.slug = None
        self.condition_id = None
        self.token_up = None
        self.token_down = None
        self.end_time = None
        self.end_ts_ms = None
        self.question = None
        self.extra = {}

    def summary(self) -> dict:
        return {
            "slug": self.slug,
            "condition_id": self.condition_id,
            "token_up": self.token_up,
            "token_down": self.token_down,
            "end_time": self.end_time,
            "question": self.question,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_ms() -> int:
    return int(time.time() * 1000)


def _now_s() -> int:
    return int(time.time())


def _current_window_epoch() -> int:
    """Compute the epoch (start) of the current 5-min window."""
    return (_now_s() // WINDOW_SECONDS) * WINDOW_SECONDS


def _parse_iso_to_ms(iso_str: str) -> int:
    """Parse ISO 8601 timestamp to milliseconds UTC."""
    if iso_str.endswith("Z"):
        iso_str = iso_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)


async def _http_get(
    client: httpx.AsyncClient, url: str, params: dict | None = None, retries: int = 3
) -> dict | list | None:
    """GET with simple retry logic."""
    for attempt in range(retries):
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException) as e:
            if attempt < retries - 1:
                wait = 2**attempt
                log.warning("HTTP error %s (attempt %d/%d), retrying in %ds: %s", url, attempt + 1, retries, wait, e)
                await asyncio.sleep(wait)
            else:
                log.error("HTTP request failed after %d attempts: %s — %s", retries, url, e)
                return None


# ---------------------------------------------------------------------------
# Task 1: Binance WebSocket stream
# ---------------------------------------------------------------------------


async def binance_stream(writer: JsonlWriter, shutdown: asyncio.Event):
    """Connect to Binance BTC/USDT trade stream and log every tick."""
    log.info("Binance WS URL: %s", BINANCE_WS_URL)
    while not shutdown.is_set():
        try:
            log.info("Connecting to Binance WebSocket...")
            async with websockets.connect(BINANCE_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                log.info("Binance WebSocket connected")
                while not shutdown.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        continue
                    data = json.loads(msg)
                    writer.write({
                        "ts": data.get("T", _now_ms()),  # Trade time
                        "price": float(data["p"]),
                        "qty": float(data["q"]),
                    })
        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            if shutdown.is_set():
                break
            log.warning("Binance WS disconnected (%s), reconnecting in 3s...", e)
            await asyncio.sleep(3)
        except Exception:
            if shutdown.is_set():
                break
            log.exception("Unexpected error in Binance stream")
            await asyncio.sleep(5)

    log.info("Binance stream stopped")


# ---------------------------------------------------------------------------
# Task 2: Polymarket orderbook poller
# ---------------------------------------------------------------------------


async def polymarket_poller(
    market: ActiveMarket,
    ob_writer: JsonlWriter,
    shutdown: asyncio.Event,
):
    """Poll Polymarket orderbook every 1s for the active market."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        while not shutdown.is_set():
            if not market.is_active() or market.token_up is None:
                await asyncio.sleep(1)
                continue

            ts = _now_ms()
            slug = market.slug
            token_up = market.token_up
            token_down = market.token_down

            for side, token_id in [("UP", token_up), ("DOWN", token_down)]:
                book = await _http_get(client, f"{CLOB_API_URL}/book", params={"token_id": token_id})
                if book is None:
                    continue
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                best_bid = float(bids[0]["price"]) if bids else None
                best_ask = float(asks[0]["price"]) if asks else None
                mid = (best_bid + best_ask) / 2 if (best_bid is not None and best_ask is not None) else None

                ob_writer.write({
                    "ts": _now_ms(),
                    "slug": slug,
                    "side": side,
                    "token_id": token_id,
                    "bids": bids[:10],  # Top 10 levels
                    "asks": asks[:10],
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "mid": mid,
                })

            # Sleep until next orderbook poll
            elapsed = (_now_ms() - ts) / 1000
            sleep_time = max(0, ORDERBOOK_INTERVAL - elapsed)
            await asyncio.sleep(sleep_time)

    log.info("Polymarket poller stopped")


# ---------------------------------------------------------------------------
# Task 3: Market manager — deterministic slug-based discovery
# ---------------------------------------------------------------------------


async def market_manager(
    market: ActiveMarket,
    markets_writer: JsonlWriter,
    shutdown: asyncio.Event,
):
    """Discover the active BTC 5-min market using deterministic slug pattern."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        while not shutdown.is_set():
            try:
                await _discover_market(client, market, markets_writer)
            except Exception:
                log.exception("Error in market discovery")

            # Wait before next check (but break early on shutdown)
            for _ in range(int(MARKET_CHECK_INTERVAL)):
                if shutdown.is_set():
                    break
                await asyncio.sleep(1)

    log.info("Market manager stopped")


async def _discover_market(
    client: httpx.AsyncClient,
    market: ActiveMarket,
    markets_writer: JsonlWriter,
):
    """Find the currently active BTC 5-min market via deterministic slug.

    Slug pattern: btc-updown-5m-{epoch}
    Endpoint: GET gamma-api.polymarket.com/events/slug/{slug}
    The epoch = floor(now / 300) * 300 (start of current 5-min window).
    """
    # If we have an active market that hasn't expired, no-op
    if market.is_active():
        return

    if market.slug:
        log.info("Market %s expired, searching for next...", market.slug)
        markets_writer.write({
            "ts": _now_ms(),
            "event": "expired",
            **market.summary(),
        })

    # Compute slug from current time
    epoch = _current_window_epoch()
    slug = f"{SLUG_PREFIX}{epoch}"

    data = await _http_get(client, f"{GAMMA_API_URL}/events/slug/{slug}")

    if not data or not isinstance(data, dict) or "markets" not in data:
        log.warning("No event found for slug %s", slug)
        async with market.lock:
            market.clear()
        return

    markets = data.get("markets", [])
    if not markets:
        log.warning("Event %s has no markets", slug)
        async with market.lock:
            market.clear()
        return

    m = markets[0]

    # Parse end time
    end_str = m.get("endDate") or data.get("endDate")
    if not end_str:
        log.warning("Market %s missing endDate", slug)
        return
    end_ms = _parse_iso_to_ms(end_str)

    if end_ms <= _now_ms():
        log.info("Market %s already ended, will pick up next window", slug)
        async with market.lock:
            market.clear()
        return

    # Extract token IDs
    clob_token_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    if not clob_token_ids or len(clob_token_ids) < 2:
        log.warning("Market %s missing token IDs: %s", slug, clob_token_ids)
        return

    # Determine which token is UP vs DOWN from outcomes
    outcomes = m.get("outcomes") or "[]"
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    token_up, token_down = clob_token_ids[0], clob_token_ids[1]
    if len(outcomes) >= 2:
        for i, outcome in enumerate(outcomes):
            label = outcome.lower() if isinstance(outcome, str) else ""
            if "up" in label:
                token_up = clob_token_ids[i]
                token_down = clob_token_ids[1 - i]
                break

    async with market.lock:
        market.slug = slug
        market.condition_id = m.get("conditionId") or m.get("condition_id")
        market.token_up = token_up
        market.token_down = token_down
        market.end_time = end_str
        market.end_ts_ms = end_ms
        market.question = m.get("question")
        market.extra = {
            "outcomes": outcomes,
            "outcome_prices": m.get("outcomePrices") or m.get("outcome_prices"),
        }

    log.info(
        "Active market: %s | %s | UP=%s... DOWN=%s... | ends=%s",
        slug,
        market.question,
        token_up[:16],
        token_down[:16],
        end_str,
    )

    markets_writer.write({
        "ts": _now_ms(),
        "event": "discovered",
        "slug": slug,
        "condition_id": market.condition_id,
        "token_up": token_up,
        "token_down": token_down,
        "end_time": end_str,
        "end_ts_ms": end_ms,
        "question": market.question,
        **market.extra,
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main():
    log.info("Starting BTC 5-min orderbook collector")
    log.info("Data directory: %s", DATA_DIR.resolve())

    shutdown = asyncio.Event()

    # Handle signals for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: shutdown.set())

    # Writers
    binance_writer = JsonlWriter(DATA_DIR, "binance_ticks.jsonl.gz")
    ob_writer = JsonlWriter(DATA_DIR, "orderbooks.jsonl.gz")
    markets_writer = JsonlWriter(DATA_DIR, "markets.jsonl.gz")

    # Shared state
    market = ActiveMarket()

    # Run all tasks concurrently
    tasks = [
        asyncio.create_task(binance_stream(binance_writer, shutdown), name="binance"),
        asyncio.create_task(polymarket_poller(market, ob_writer, shutdown), name="poller"),
        asyncio.create_task(market_manager(market, markets_writer, shutdown), name="manager"),
    ]

    log.info("All tasks started. Press Ctrl+C to stop.")

    # Wait for shutdown signal
    await shutdown.wait()
    log.info("Shutdown signal received, stopping tasks...")

    # Cancel tasks and wait
    for task in tasks:
        task.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for task, result in zip(tasks, results):
        if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
            log.error("Task %s ended with error: %s", task.get_name(), result)

    # Close writers
    for w in [binance_writer, ob_writer, markets_writer]:
        w.close()

    log.info("Collector stopped cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
