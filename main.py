import json
import logging
import math
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import gspread
import requests


# =========================
# Time windows
# =========================

NY_TZ = ZoneInfo("America/New_York")

# Daily rollover / maintenance buffer
ROLLOVER_BLOCK_START_MINUTE = 16 * 60 + 55   # 16:55 NY
ROLLOVER_BLOCK_END_MINUTE = 17 * 60 + 10     # 17:10 NY

# Weekly close/open buffer
WEEKLY_CLOSE_BUFFER_START_MINUTE = 4 * 60 + 59   # Friday 04:59 NY
WEEKLY_OPEN_BUFFER_END_MINUTE = 5 * 60 + 5       # Monday 05:05 NY


# =========================
# Environment helpers
# =========================


def env_str(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise ValueError(f"Missing required env var: {name}")
    return "" if value is None else str(value)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return float(default)
    return float(value)


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return int(default)
    return int(value)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


# =========================
# Config
# =========================


@dataclass(frozen=True)
class Config:
    oanda_account_id: str
    oanda_api_token: str
    oanda_environment: str
    sheet_id: str
    worksheet_name: str
    poll_seconds: int
    order_fraction: float
    dry_run: bool
    log_level: str
    request_timeout_seconds: int

    @property
    def oanda_base_url(self) -> str:
        env_name = self.oanda_environment.strip().lower()
        if env_name in {"practice", "demo", "sandbox"}:
            return "https://api-fxpractice.oanda.com/v3"
        if env_name in {"live", "fxtrade", "prod", "production"}:
            return "https://api-fxtrade.oanda.com/v3"
        raise ValueError("OANDA_ENVIRONMENT must be one of: practice, live")


def load_config() -> Config:
    return Config(
        oanda_account_id=env_str("OANDA_ACCOUNT_ID", required=True),
        oanda_api_token=env_str("OANDA_API_TOKEN", required=True),
        oanda_environment=env_str("OANDA_ENVIRONMENT", "practice"),
        sheet_id=env_str("SHEET_ID", required=True),
        worksheet_name=env_str("SIGNALS_WORKSHEET", "Signals"),
        poll_seconds=max(3, env_int("POLL_SECONDS", 10)),
        order_fraction=env_float("ORDER_FRACTION", 0.02),
        dry_run=env_bool("DRY_RUN", False),
        log_level=env_str("LOG_LEVEL", "INFO"),
        request_timeout_seconds=max(5, env_int("REQUEST_TIMEOUT_SECONDS", 20)),
    )


# =========================
# Logging
# =========================


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )


logger = logging.getLogger("oanda-signals-bot")


# =========================
# Google Sheets client
# =========================


def build_gspread_client() -> gspread.Client:
    credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "").strip()
    if credentials_json:
        creds = json.loads(credentials_json)
        return gspread.service_account_from_dict(creds)

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if credentials_path:
        return gspread.service_account(filename=credentials_path)

    raise ValueError(
        "Provide GOOGLE_SHEETS_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS"
    )


# =========================
# OANDA client
# =========================


class OandaError(RuntimeError):
    pass


class OandaClient:
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {config.oanda_api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _url(self, path: str) -> str:
        return f"{self.config.oanda_base_url}{path}"

    def _request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        response = self.session.request(
            method=method,
            url=self._url(path),
            timeout=self.config.request_timeout_seconds,
            **kwargs,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {"raw": response.text}

        if not response.ok:
            raise OandaError(
                f"OANDA {method} {path} failed: HTTP {response.status_code} | {payload}"
            )
        return payload

    def get_account(self) -> Dict[str, Any]:
        return self._request("GET", f"/accounts/{self.config.oanda_account_id}")

    @lru_cache(maxsize=256)
    def get_instrument_details(self, instrument: str) -> Dict[str, Any]:
        payload = self._request(
            "GET",
            f"/accounts/{self.config.oanda_account_id}/instruments",
            params={"instruments": instrument},
        )
        instruments = payload.get("instruments", [])
        if not instruments:
            raise OandaError(f"Instrument not found or not tradable for account: {instrument}")
        return instruments[0]

    def get_pricing(self, instrument: str) -> Dict[str, Any]:
        payload = self._request(
            "GET",
            f"/accounts/{self.config.oanda_account_id}/pricing",
            params={"instruments": instrument, "includeHomeConversions": True},
        )
        prices = payload.get("prices", [])
        if not prices:
            raise OandaError(f"No pricing returned for instrument: {instrument}")
        return payload

    def place_market_order(
        self,
        instrument: str,
        units: int,
        client_id: str,
        side: str,
        row_number: int,
    ) -> Dict[str, Any]:
        body = {
            "order": {
                "type": "MARKET",
                "instrument": instrument,
                "units": str(units),
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
                "clientExtensions": {
                    "id": client_id,
                    "tag": "signals-sheet-bot",
                    "comment": f"{side} row {row_number}",
                },
            }
        }
        return self._request(
            "POST",
            f"/accounts/{self.config.oanda_account_id}/orders",
            json=body,
        )


# =========================
# Domain helpers
# =========================


@dataclass
class SignalRow:
    row_number: int
    pair_raw: str
    side_raw: str
    trigger_raw: Any

    @property
    def instrument(self) -> str:
        return normalize_instrument(self.pair_raw)

    @property
    def side(self) -> str:
        value = (self.side_raw or "").strip().upper()
        if value not in {"LONG", "SHORT"}:
            raise ValueError(f"Invalid side '{self.side_raw}'")
        return value

    @property
    def triggered(self) -> bool:
        return parse_bool(self.trigger_raw)


@dataclass
class OrderPlan:
    instrument: str
    side: str
    units: int
    margin_available: float
    target_margin_budget: float
    sizing_method: str
    reference_price: float
    client_id: str


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().upper() == "TRUE"


def normalize_instrument(value: str) -> str:
    raw = (value or "").strip().upper()
    if not raw:
        raise ValueError("Missing forex pair")

    cleaned = raw.replace("-", "_").replace("/", "_").replace(" ", "")
    if "_" in cleaned:
        parts = [p for p in cleaned.split("_") if p]
        if len(parts) != 2 or any(len(p) != 3 for p in parts):
            raise ValueError(f"Unsupported pair format '{value}'")
        return f"{parts[0]}_{parts[1]}"

    if len(cleaned) == 6:
        return f"{cleaned[:3]}_{cleaned[3:]}"

    raise ValueError(f"Unsupported pair format '{value}'")


def round_down(value: float, decimals: int) -> float:
    factor = 10 ** decimals
    return math.floor(value * factor) / factor


# =========================
# Sheet access (read-only)
# =========================


class SheetClient:
    def __init__(self, config: Config):
        self.config = config
        self.gc = build_gspread_client()
        self.sheet = self.gc.open_by_key(config.sheet_id)
        self.ws = self.sheet.worksheet(config.worksheet_name)

    def read_signal_rows(self) -> List[SignalRow]:
        values = self.ws.get("B2:E", value_render_option="UNFORMATTED_VALUE")
        rows: List[SignalRow] = []
        for idx, row in enumerate(values, start=2):
            padded = list(row) + [""] * (4 - len(row))
            pair = str(padded[0]).strip() if padded[0] is not None else ""
            side = str(padded[2]).strip() if padded[2] is not None else ""
            trigger = padded[3]
            if not pair:
                continue
            rows.append(
                SignalRow(
                    row_number=idx,
                    pair_raw=pair,
                    side_raw=side,
                    trigger_raw=trigger,
                )
            )
        return rows


# =========================
# Trading bot
# =========================


class SignalsBot:
    def __init__(self, config: Config):
        self.config = config
        self.sheet = SheetClient(config)
        self.oanda = OandaClient(config)
        self.running = True
        self.armed_signals: Set[Tuple[int, str, str]] = set()

    def stop(self, *_args: Any) -> None:
        logger.info("Shutdown signal received. Stopping after current loop.")
        self.running = False

    def run_forever(self) -> None:
        logger.info(
            "Bot started | worksheet=%s | poll_seconds=%s | dry_run=%s | sheet_mode=read_only",
            self.config.worksheet_name,
            self.config.poll_seconds,
            self.config.dry_run,
        )
        while self.running:
            started = time.time()
            try:
                self.run_once()
            except Exception as exc:
                logger.exception("Top-level loop error: %s", exc)
            elapsed = time.time() - started
            sleep_for = max(0.0, self.config.poll_seconds - elapsed)
            if self.running and sleep_for > 0:
                time.sleep(sleep_for)

    def run_once(self) -> None:
        rows = self.sheet.read_signal_rows()
        logger.info("Scanned %s populated signal rows", len(rows))
        for row in rows:
            self.process_row(row)

    def signal_key(self, row: SignalRow) -> Optional[Tuple[int, str, str]]:
        try:
            return (row.row_number, row.instrument, row.side)
        except Exception:
            return None

    def entry_block_reason(self, now: Optional[datetime] = None) -> Optional[str]:
        now = now or datetime.now(NY_TZ)
        weekday = now.weekday()  # Mon=0 ... Sun=6
        minute_of_day = now.hour * 60 + now.minute

        # Daily rollover / maintenance window
        if ROLLOVER_BLOCK_START_MINUTE <= minute_of_day < ROLLOVER_BLOCK_END_MINUTE:
            return "daily rollover / maintenance window"

        # Weekly close/open protection window
        # Friday 04:59 NY -> Monday 05:05 NY
        if weekday == 4 and minute_of_day >= WEEKLY_CLOSE_BUFFER_START_MINUTE:
            return "within 12h of weekly close"
        if weekday == 5:
            return "weekend market closure"
        if weekday == 6:
            return "within 12h of weekly reopen"
        if weekday == 0 and minute_of_day < WEEKLY_OPEN_BUFFER_END_MINUTE:
            return "within 12h of weekly reopen"

        return None

    def process_row(self, row: SignalRow) -> None:
        signal_key = self.signal_key(row)

        if not row.triggered:
            if signal_key and signal_key in self.armed_signals:
                logger.info(
                    "Row %s trigger reset. Re-arming row for the next signal.",
                    row.row_number,
                )
                self.armed_signals.discard(signal_key)
            return

        if signal_key and signal_key in self.armed_signals:
            logger.debug(
                "Row %s already executed for current TRUE trigger. Waiting for reset.",
                row.row_number,
            )
            return

        block_reason = self.entry_block_reason()
        if block_reason:
            logger.info(
                "ENTRY BLOCKED | row=%s | pair=%s | side=%s | reason=%s",
                row.row_number,
                row.pair_raw,
                row.side_raw,
                block_reason,
            )
            return

        try:
            plan = self.build_order_plan(row)

            if self.config.dry_run:
                logger.info(
                    "DRY RUN | row=%s | instrument=%s | side=%s | units=%s | sizing=%s | ref_price=%s | margin_available=%.2f | target_budget=%.2f",
                    row.row_number,
                    plan.instrument,
                    plan.side,
                    plan.units,
                    plan.sizing_method,
                    plan.reference_price,
                    plan.margin_available,
                    plan.target_margin_budget,
                )
                self.armed_signals.add((row.row_number, plan.instrument, plan.side))
                return

            response = self.oanda.place_market_order(
                instrument=plan.instrument,
                units=plan.units,
                client_id=plan.client_id,
                side=plan.side,
                row_number=row.row_number,
            )

            order_id = (
                response.get("orderFillTransaction", {}).get("id")
                or response.get("orderCreateTransaction", {}).get("id")
                or ""
            )

            logger.info(
                "ORDER PLACED | row=%s | instrument=%s | side=%s | units=%s | order_id=%s",
                row.row_number,
                plan.instrument,
                plan.side,
                plan.units,
                order_id,
            )
            self.armed_signals.add((row.row_number, plan.instrument, plan.side))
        except Exception as exc:
            logger.exception("Row %s failed: %s", row.row_number, exc)

    def build_order_plan(self, row: SignalRow) -> OrderPlan:
        instrument = row.instrument
        side = row.side
        is_long = side == "LONG"

        account_payload = self.oanda.get_account()
        account = account_payload.get("account", {})
        margin_available = float(account.get("marginAvailable", 0.0))
        if margin_available <= 0:
            raise OandaError("Account marginAvailable is zero or negative")

        pricing_payload = self.oanda.get_pricing(instrument)
        price = pricing_payload["prices"][0]
        if price.get("status") and str(price["status"]).lower() != "tradeable":
            raise OandaError(f"Instrument {instrument} is not tradeable right now")

        reference_price = float(
            price["asks"][0]["price"] if is_long else price["bids"][0]["price"]
        )
        target_margin_budget = margin_available * self.config.order_fraction

        units_available = price.get("unitsAvailable") or {}
        default_bucket = units_available.get("default") or {}
        side_key = "long" if is_long else "short"
        raw_units_from_availability = default_bucket.get(side_key)

        if raw_units_from_availability is not None:
            raw_units = float(raw_units_from_availability) * self.config.order_fraction
            sized_units = max(1, int(math.floor(raw_units)))
            signed_units = sized_units if is_long else -sized_units
            return OrderPlan(
                instrument=instrument,
                side=side,
                units=signed_units,
                margin_available=margin_available,
                target_margin_budget=target_margin_budget,
                sizing_method="unitsAvailable.default",
                reference_price=reference_price,
                client_id=self._client_id(row.row_number, instrument, side),
            )

        instrument_meta = self.oanda.get_instrument_details(instrument)
        margin_rate = float(instrument_meta.get("marginRate", 0.0))
        if margin_rate <= 0:
            raise OandaError(f"Invalid marginRate for {instrument}")

        trade_units_precision = int(instrument_meta.get("tradeUnitsPrecision", 0))
        minimum_trade_size = float(instrument_meta.get("minimumTradeSize", 1))

        quote_ccy = instrument.split("_")[1]
        home_conversions = pricing_payload.get("homeConversions", [])
        quote_conversion = None
        for item in home_conversions:
            if item.get("currency") == quote_ccy:
                quote_conversion = float(item.get("positionValue", 0.0))
                break
        if quote_conversion is None or quote_conversion <= 0:
            raise OandaError(
                f"No usable home conversion returned for quote currency {quote_ccy}"
            )

        raw_units = target_margin_budget / (reference_price * quote_conversion * margin_rate)
        raw_units = max(minimum_trade_size, raw_units)
        rounded_units = round_down(raw_units, trade_units_precision)
        if rounded_units < minimum_trade_size:
            rounded_units = minimum_trade_size

        if trade_units_precision == 0:
            final_units = int(rounded_units)
        else:
            final_units = int(round(rounded_units))

        if final_units <= 0:
            raise OandaError(f"Calculated non-positive units for {instrument}: {final_units}")

        signed_units = final_units if is_long else -final_units

        return OrderPlan(
            instrument=instrument,
            side=side,
            units=signed_units,
            margin_available=margin_available,
            target_margin_budget=target_margin_budget,
            sizing_method="marginAvailable/homeConversions/marginRate",
            reference_price=reference_price,
            client_id=self._client_id(row.row_number, instrument, side),
        )

    @staticmethod
    def _client_id(row_number: int, instrument: str, side: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        compact_instrument = instrument.replace("_", "")
        return f"sig-{row_number}-{compact_instrument}-{side.lower()}-{stamp}"[:128]


# =========================
# Entrypoint
# =========================


def main() -> None:
    config = load_config()
    setup_logging(config.log_level)
    logger.info("Initializing bot")
    bot = SignalsBot(config)

    signal.signal(signal.SIGTERM, bot.stop)
    signal.signal(signal.SIGINT, bot.stop)

    bot.run_forever()


if __name__ == "__main__":
    main()
