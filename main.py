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
    max_margin_usage_pct: float
    dry_run: bool
    log_level: str
    request_timeout_seconds: int
    enable_entry_time_blocks: bool

    @property
    def oanda_base_url(self) -> str:
        env_name = self.oanda_environment.strip().lower()
        if env_name in {"practice", "demo", "sandbox"}:
            return "https://api-fxpractice.oanda.com/v3"
        if env_name in {"live", "fxtrade", "prod", "production"}:
            return "https://api-fxtrade.oanda.com/v3"
        raise ValueError("OANDA_ENVIRONMENT must be one of: practice, live")


def load_config() -> Config:
    max_margin_usage_pct = env_float("MAX_MARGIN_USAGE_PCT", 0.90)
    if not (0.0 < max_margin_usage_pct < 1.0):
        raise ValueError("MAX_MARGIN_USAGE_PCT must be > 0 and < 1 (example: 0.90)")

    return Config(
        oanda_account_id=env_str("OANDA_ACCOUNT_ID", required=True),
        oanda_api_token=env_str("OANDA_API_TOKEN", required=True),
        oanda_environment=env_str("OANDA_ENVIRONMENT", "practice"),
        sheet_id=env_str("SHEET_ID", required=True),
        worksheet_name=env_str("SIGNALS_WORKSHEET", "Signals"),
        poll_seconds=max(3, env_int("POLL_SECONDS", 10)),
        order_fraction=env_float("ORDER_FRACTION", 0.02),
        max_margin_usage_pct=max_margin_usage_pct,
        dry_run=env_bool("DRY_RUN", False),
        log_level=env_str("LOG_LEVEL", "INFO"),
        request_timeout_seconds=max(5, env_int("REQUEST_TIMEOUT_SECONDS", 20)),
        enable_entry_time_blocks=env_bool("ENABLE_ENTRY_TIME_BLOCKS", True),
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
        strategy_id: str,
        arm_pct: float,
        trail_drop_pct: float,
        stop_loss_pct: float,
    ) -> Dict[str, Any]:
        comment = (
            f"{side} row {row_number} strat={strategy_id} "
            f"arm={arm_pct:.4f} trail={trail_drop_pct:.4f} stop={stop_loss_pct:.4f}"
        )[:128]
        body = {
            "order": {
                "type": "MARKET",
                "instrument": instrument,
                "units": str(units),
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
                "clientExtensions": {
                    "id": client_id,
                    "tag": f"buyer-{strategy_id}"[:128],
                    "comment": comment,
                },
                "tradeClientExtensions": {
                    "id": f"trade-{client_id}"[:128],
                    "tag": f"strategy:{strategy_id}"[:128],
                    "comment": (
                        f"arm={arm_pct:.4f}|trail={trail_drop_pct:.4f}|stop={stop_loss_pct:.4f}"
                    )[:128],
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
    strategy_id_raw: str
    arm_pct_raw: Any
    trail_drop_pct_raw: Any
    stop_loss_pct_raw: Any
    score_raw: Any = None
    threshold_raw: Any = None
    trained_at_raw: Any = None

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

    @property
    def strategy_id(self) -> str:
        value = (self.strategy_id_raw or "").strip()
        if not value:
            raise ValueError("Missing strategy_id in Signals row")
        return value

    @property
    def arm_pct(self) -> float:
        return parse_required_float(self.arm_pct_raw, "arm_pct")

    @property
    def trail_drop_pct(self) -> float:
        return parse_required_float(self.trail_drop_pct_raw, "trail_drop_pct")

    @property
    def stop_loss_pct(self) -> float:
        return parse_required_float(self.stop_loss_pct_raw, "stop_loss_pct")

    @property
    def score(self) -> Optional[float]:
        return parse_optional_float(self.score_raw)

    @property
    def threshold(self) -> Optional[float]:
        return parse_optional_float(self.threshold_raw)

    @property
    def trained_at(self) -> str:
        return "" if self.trained_at_raw is None else str(self.trained_at_raw).strip()


@dataclass
class OrderPlan:
    instrument: str
    side: str
    units: int
    margin_available: float
    margin_used: float
    uncapped_target_margin_budget: float
    target_margin_budget: float
    max_additional_margin_allowed: float
    projected_margin_used: float
    projected_margin_used_pct: float
    sizing_method: str
    reference_price: float
    client_id: str
    strategy_id: str
    arm_pct: float
    trail_drop_pct: float
    stop_loss_pct: float
    score: Optional[float]
    threshold: Optional[float]
    trained_at: str


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().upper() == "TRUE"


def parse_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return None
    return float(text)


def parse_required_float(value: Any, field_name: str) -> float:
    parsed = parse_optional_float(value)
    if parsed is None:
        raise ValueError(f"Missing {field_name} in Signals row")
    return parsed


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


def normalize_header(value: Any) -> str:
    return str(value or "").strip().lower()


# =========================
# Sheet access (read-only)
# =========================

class SheetClient:
    REQUIRED_HEADERS = {
        "symbol",
        "side",
        "entry_signal",
        "strategy_id",
        "arm_pct",
        "trail_drop_pct",
        "stop_loss_pct",
    }

    def __init__(self, config: Config):
        self.config = config
        self.gc = build_gspread_client()
        self.sheet = self.gc.open_by_key(config.sheet_id)
        self.ws = self.sheet.worksheet(config.worksheet_name)

    def read_signal_rows(self) -> List[SignalRow]:
        all_values = self.ws.get_all_values()
        if not all_values:
            return []

        headers = [normalize_header(h) for h in all_values[0]]
        header_map = {name: idx for idx, name in enumerate(headers) if name}
        missing = sorted(self.REQUIRED_HEADERS - set(header_map))
        if missing:
            raise ValueError(
                f"Signals sheet is missing required headers: {', '.join(missing)}"
            )

        rows: List[SignalRow] = []
        for idx, row in enumerate(all_values[1:], start=2):
            padded = list(row) + [""] * max(0, len(headers) - len(row))

            def get(name: str) -> Any:
                pos = header_map.get(name)
                if pos is None or pos >= len(padded):
                    return ""
                return padded[pos]

            pair = str(get("symbol")).strip()
            if not pair:
                continue

            rows.append(
                SignalRow(
                    row_number=idx,
                    pair_raw=pair,
                    side_raw=str(get("side")).strip(),
                    trigger_raw=get("entry_signal"),
                    strategy_id_raw=str(get("strategy_id")).strip(),
                    arm_pct_raw=get("arm_pct"),
                    trail_drop_pct_raw=get("trail_drop_pct"),
                    stop_loss_pct_raw=get("stop_loss_pct"),
                    score_raw=get("score"),
                    threshold_raw=get("threshold"),
                    trained_at_raw=get("trained_at"),
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
        self.armed_signals: Set[Tuple[int, str, str, str]] = set()

    def stop(self, *_args: Any) -> None:
        logger.info("Shutdown signal received. Stopping after current loop.")
        self.running = False

    def run_forever(self) -> None:
        logger.info(
            "Bot started | worksheet=%s | poll_seconds=%s | dry_run=%s | sheet_mode=read_only | entry_time_blocks=%s | order_fraction=%.4f | max_margin_usage_pct=%.2f",
            self.config.worksheet_name,
            self.config.poll_seconds,
            self.config.dry_run,
            self.config.enable_entry_time_blocks,
            self.config.order_fraction,
            self.config.max_margin_usage_pct,
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

    def signal_key(self, row: SignalRow) -> Optional[Tuple[int, str, str, str]]:
        try:
            return (row.row_number, row.instrument, row.side, row.strategy_id)
        except Exception:
            return None

    def entry_block_reason(self, now: Optional[datetime] = None) -> Optional[str]:
        if not self.config.enable_entry_time_blocks:
            return None

        now = now or datetime.now(NY_TZ)
        weekday = now.weekday()  # Mon=0 ... Sun=6
        minute_of_day = now.hour * 60 + now.minute

        if ROLLOVER_BLOCK_START_MINUTE <= minute_of_day < ROLLOVER_BLOCK_END_MINUTE:
            return "daily rollover / maintenance window"

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
                "ENTRY BLOCKED | row=%s | pair=%s | side=%s | strategy_id=%s | arm_pct=%.4f | trail_drop_pct=%.4f | stop_loss_pct=%.4f | reason=%s",
                row.row_number,
                row.pair_raw,
                row.side_raw,
                row.strategy_id,
                row.arm_pct,
                row.trail_drop_pct,
                row.stop_loss_pct,
                block_reason,
            )
            return

        try:
            plan = self.build_order_plan(row)
            if plan is None:
                return

            if self.config.dry_run:
                logger.info(
                    "DRY RUN ENTRY | row=%s | instrument=%s | side=%s | units=%s | strategy_id=%s | score=%s | threshold=%s | arm_pct=%.4f | trail_drop_pct=%.4f | stop_loss_pct=%.4f | sizing=%s | ref_price=%s | margin_available=%.2f | margin_used=%.2f | uncapped_budget=%.2f | capped_budget=%.2f | max_additional_margin=%.2f | projected_margin_used=%.2f | projected_margin_used_pct=%.2f | trained_at=%s",
                    row.row_number,
                    plan.instrument,
                    plan.side,
                    plan.units,
                    plan.strategy_id,
                    f"{plan.score:.2f}" if plan.score is not None else "",
                    f"{plan.threshold:.2f}" if plan.threshold is not None else "",
                    plan.arm_pct,
                    plan.trail_drop_pct,
                    plan.stop_loss_pct,
                    plan.sizing_method,
                    plan.reference_price,
                    plan.margin_available,
                    plan.margin_used,
                    plan.uncapped_target_margin_budget,
                    plan.target_margin_budget,
                    plan.max_additional_margin_allowed,
                    plan.projected_margin_used,
                    plan.projected_margin_used_pct,
                    plan.trained_at,
                )
                self.armed_signals.add((row.row_number, plan.instrument, plan.side, plan.strategy_id))
                return

            response = self.oanda.place_market_order(
                instrument=plan.instrument,
                units=plan.units,
                client_id=plan.client_id,
                side=plan.side,
                row_number=row.row_number,
                strategy_id=plan.strategy_id,
                arm_pct=plan.arm_pct,
                trail_drop_pct=plan.trail_drop_pct,
                stop_loss_pct=plan.stop_loss_pct,
            )

            order_id = (
                response.get("orderFillTransaction", {}).get("id")
                or response.get("orderCreateTransaction", {}).get("id")
                or ""
            )
            trade_id = (
                response.get("orderFillTransaction", {}).get("tradeOpened", {}).get("tradeID")
                or response.get("orderFillTransaction", {}).get("tradeReduced", {}).get("tradeID")
                or response.get("orderFillTransaction", {}).get("tradesOpened", [{}])[0].get("tradeID", "")
                or ""
            )

            logger.info(
                "ORDER PLACED | row=%s | instrument=%s | side=%s | units=%s | order_id=%s | trade_id=%s | strategy_id=%s | score=%s | threshold=%s | arm_pct=%.4f | trail_drop_pct=%.4f | stop_loss_pct=%.4f | margin_available=%.2f | margin_used=%.2f | uncapped_budget=%.2f | capped_budget=%.2f | projected_margin_used=%.2f | projected_margin_used_pct=%.2f | trained_at=%s",
                row.row_number,
                plan.instrument,
                plan.side,
                plan.units,
                order_id,
                trade_id,
                plan.strategy_id,
                f"{plan.score:.2f}" if plan.score is not None else "",
                f"{plan.threshold:.2f}" if plan.threshold is not None else "",
                plan.arm_pct,
                plan.trail_drop_pct,
                plan.stop_loss_pct,
                plan.margin_available,
                plan.margin_used,
                plan.uncapped_target_margin_budget,
                plan.target_margin_budget,
                plan.projected_margin_used,
                plan.projected_margin_used_pct,
                plan.trained_at,
            )
            self.armed_signals.add((row.row_number, plan.instrument, plan.side, plan.strategy_id))
        except Exception as exc:
            logger.exception("Row %s failed: %s", row.row_number, exc)

    def build_order_plan(self, row: SignalRow) -> Optional[OrderPlan]:
        instrument = row.instrument
        side = row.side
        is_long = side == "LONG"

        account_payload = self.oanda.get_account()
        account = account_payload.get("account", {})

        margin_available = float(account.get("marginAvailable", 0.0))
        margin_used = float(account.get("marginUsed", 0.0))

        if margin_available <= 0:
            raise OandaError("Account marginAvailable is zero or negative")

        total_margin_capacity = margin_available + margin_used
        if total_margin_capacity <= 0:
            raise OandaError("Account total margin capacity is zero or negative")

        uncapped_target_margin_budget = margin_available * self.config.order_fraction
        max_allowed_margin_used = total_margin_capacity * self.config.max_margin_usage_pct
        max_additional_margin_allowed = max(0.0, max_allowed_margin_used - margin_used)
        target_margin_budget = min(uncapped_target_margin_budget, max_additional_margin_allowed)

        if max_additional_margin_allowed <= 0 or target_margin_budget <= 0:
            logger.info(
                "ENTRY SKIPPED | row=%s | instrument=%s | side=%s | strategy_id=%s | reason=margin usage cap reached | margin_used=%.2f | margin_available=%.2f | max_margin_usage_pct=%.2f",
                row.row_number,
                instrument,
                side,
                row.strategy_id,
                margin_used,
                margin_available,
                self.config.max_margin_usage_pct,
            )
            return None

        effective_fraction = target_margin_budget / margin_available
        projected_margin_used = margin_used + target_margin_budget
        projected_margin_used_pct = (projected_margin_used / total_margin_capacity) * 100.0

        pricing_payload = self.oanda.get_pricing(instrument)
        price = pricing_payload["prices"][0]
        if price.get("status") and str(price["status"]).lower() != "tradeable":
            raise OandaError(f"Instrument {instrument} is not tradeable right now")

        reference_price = float(
            price["asks"][0]["price"] if is_long else price["bids"][0]["price"]
        )
        client_id = self._client_id(row.row_number, instrument, side, row.strategy_id)

        units_available = price.get("unitsAvailable") or {}
        default_bucket = units_available.get("default") or {}
        side_key = "long" if is_long else "short"
        raw_units_from_availability = default_bucket.get(side_key)

        if raw_units_from_availability is not None:
            raw_units = float(raw_units_from_availability) * effective_fraction
            sized_units = max(1, int(math.floor(raw_units)))
            signed_units = sized_units if is_long else -sized_units
            return OrderPlan(
                instrument=instrument,
                side=side,
                units=signed_units,
                margin_available=margin_available,
                margin_used=margin_used,
                uncapped_target_margin_budget=uncapped_target_margin_budget,
                target_margin_budget=target_margin_budget,
                max_additional_margin_allowed=max_additional_margin_allowed,
                projected_margin_used=projected_margin_used,
                projected_margin_used_pct=projected_margin_used_pct,
                sizing_method="unitsAvailable.default.capped",
                reference_price=reference_price,
                client_id=client_id,
                strategy_id=row.strategy_id,
                arm_pct=row.arm_pct,
                trail_drop_pct=row.trail_drop_pct,
                stop_loss_pct=row.stop_loss_pct,
                score=row.score,
                threshold=row.threshold,
                trained_at=row.trained_at,
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

        min_trade_margin = minimum_trade_size * reference_price * quote_conversion * margin_rate
        if min_trade_margin > max_additional_margin_allowed:
            logger.info(
                "ENTRY SKIPPED | row=%s | instrument=%s | side=%s | strategy_id=%s | reason=minimum trade size exceeds margin cap | min_trade_margin=%.4f | max_additional_margin=%.4f",
                row.row_number,
                instrument,
                side,
                row.strategy_id,
                min_trade_margin,
                max_additional_margin_allowed,
            )
            return None

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

        estimated_margin_for_units = abs(final_units) * reference_price * quote_conversion * margin_rate
        if estimated_margin_for_units > max_additional_margin_allowed:
            logger.info(
                "ENTRY SKIPPED | row=%s | instrument=%s | side=%s | strategy_id=%s | reason=rounded order would exceed margin cap | estimated_margin=%.4f | max_additional_margin=%.4f",
                row.row_number,
                instrument,
                side,
                row.strategy_id,
                estimated_margin_for_units,
                max_additional_margin_allowed,
            )
            return None

        signed_units = final_units if is_long else -final_units

        return OrderPlan(
            instrument=instrument,
            side=side,
            units=signed_units,
            margin_available=margin_available,
            margin_used=margin_used,
            uncapped_target_margin_budget=uncapped_target_margin_budget,
            target_margin_budget=target_margin_budget,
            max_additional_margin_allowed=max_additional_margin_allowed,
            projected_margin_used=projected_margin_used,
            projected_margin_used_pct=projected_margin_used_pct,
            sizing_method="marginAvailable/homeConversions/marginRate.capped",
            reference_price=reference_price,
            client_id=client_id,
            strategy_id=row.strategy_id,
            arm_pct=row.arm_pct,
            trail_drop_pct=row.trail_drop_pct,
            stop_loss_pct=row.stop_loss_pct,
            score=row.score,
            threshold=row.threshold,
            trained_at=row.trained_at,
        )

    @staticmethod
    def _client_id(row_number: int, instrument: str, side: str, strategy_id: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        compact_instrument = instrument.replace("_", "")
        compact_strategy = "".join(ch for ch in strategy_id if ch.isalnum())[:16] or "nostrat"
        return f"sig-{row_number}-{compact_instrument}-{side.lower()}-{compact_strategy}-{stamp}"[:128]


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
