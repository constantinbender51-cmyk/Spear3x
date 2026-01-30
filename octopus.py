#!/usr/bin/env python3
"""
Octopus Grid: Execution Engine for GA Grid Strategy
- Source: Connects to app.py endpoint (/api/parameters)
- Logic: Grid Entry (All Lines) + Dynamic SL/TP
- Schedule: Entry (1 min), Protection & Cleanup (3 sec)
- Assets: Multi-Asset Support (Maps app.py symbols to Kraken Futures)
- Accounts: Dual-Key (Long Account for Buys, Short Account for Sells)
"""

import os
import sys
import time
import logging
import requests
import threading
import random
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple, Any, List

# --- Local Imports ---
try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    print("CRITICAL: 'kraken_futures.py' not found.")
    sys.exit(1)

# --- Configuration ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# API Keys (Dual Account Setup)
KF_LONG_KEY = os.getenv("KRAKEN_LONG_KEY")
KF_LONG_SECRET = os.getenv("KRAKEN_LONG_SECRET")

KF_SHORT_KEY = os.getenv("KRAKEN_SHORT_KEY")
KF_SHORT_SECRET = os.getenv("KRAKEN_SHORT_SECRET")

# Global Settings
MAX_WORKERS = 4 
LEVERAGE = 0.5*15*5
TEST_ASSET_LIMIT = 15

# Strategy Endpoint (The app.py server)
STRATEGY_URL = "https://spear3.up.railway.app/api/parameters"

# Asset Mapping: App Symbol -> Kraken Futures Symbol
SYMBOL_MAP = {
    "BTC": "PF_XBTUSD",
    "ETH": "PF_ETHUSD",
    "XRP": "PF_XRPUSD",
    "SOL": "PF_SOLUSD",
    "DOGE": "PF_DOGEUSD",
    "ADA": "PF_ADAUSD",
    "BCH": "PF_BCHUSD",
    "LINK": "PF_LINKUSD",
    "XLM": "PF_XLMUSD",
    "SUI": "PF_SUIUSD",
    "AVAX": "PF_AVAXUSD",
    "LTC": "PF_LTCUSD",
    "HBAR": "PF_HBARUSD",
    "SHIB": "PF_SHIBUSD",
    "TON": "PF_TONUSD",
}

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[logging.FileHandler("grid_octopus.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OctopusGrid")
LOG_LOCK = threading.Lock()

def bot_log(msg, level="info"):
    with LOG_LOCK:
        if level == "info": logger.info(msg)
        elif level == "warning": logger.warning(msg)
        elif level == "error": logger.error(msg)

# --- Strategy Fetcher ---

class GridParamFetcher:
    def fetch_all_params(self) -> Dict[str, Dict[str, Any]]:
        try:
            resp = requests.get(STRATEGY_URL, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    valid_strategies = {}
                    for sym, params in data.items():
                        try:
                            if "line_prices" in params and "stop_percent" in params:
                                valid_strategies[sym] = params
                        except TypeError:
                            continue
                    return valid_strategies
            else:
                bot_log(f"HTTP Error {resp.status_code}", level="warning")
        except Exception as e:
            bot_log(f"Param Fetch Failed: {e}", level="error")
        return {}

# --- Main Engine ---

class OctopusGridBot:
    def __init__(self):
        # Initialize Dual Clients
        if not KF_LONG_KEY or not KF_SHORT_KEY:
            bot_log("CRITICAL: Missing API Keys (Long or Short)", level="error")
            sys.exit(1)
            
        self.kf_long = KrakenFuturesApi(KF_LONG_KEY, KF_LONG_SECRET)
        self.kf_short = KrakenFuturesApi(KF_SHORT_KEY, KF_SHORT_SECRET)
        
        self.fetcher = GridParamFetcher()
        self.instrument_specs = {}
        self.active_params = {}
        self.param_lock = threading.Lock()

    def initialize(self):
        bot_log("--- Initializing Octopus Grid Bot (Dual-Key / All Lines) ---")
        
        self._fetch_instrument_specs()
        
        try:
            # Check Connections
            acc_long = self.kf_long.get_accounts()
            acc_short = self.kf_short.get_accounts()
            
            if "error" in acc_long or "error" in acc_short:
                bot_log(f"API Error. Long: {acc_long.get('error')} | Short: {acc_short.get('error')}", level="error")
                sys.exit(1)
            else:
                bot_log("Both API Connections Successful.")

            bot_log("Startup: Canceling all open orders for mapped assets on BOTH accounts...")
            unique_symbols = set(SYMBOL_MAP.values())
            
            for i, sym in enumerate(unique_symbols):
                try:
                    self.kf_long.cancel_all_orders({"symbol": sym.lower()})
                    self.kf_short.cancel_all_orders({"symbol": sym.lower()})
                    time.sleep(0.5) 
                except Exception as e:
                    bot_log(f"Failed to cancel {sym}: {e}", level="warning")
                    
        except Exception as e:
            bot_log(f"Startup Failed: {e}", level="error")
            sys.exit(1)

    def _fetch_instrument_specs(self):
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                target_kraken_symbols = set(SYMBOL_MAP.values())
                for inst in resp["instruments"]:
                    sym = inst["symbol"].upper()
                    if sym in target_kraken_symbols:
                        precision = inst.get("contractValueTradePrecision", 3)
                        self.instrument_specs[sym] = {
                            "sizeStep": 10 ** (-int(precision)) if precision is not None else 1.0,
                            "tickSize": float(inst.get("tickSize", 0.5))
                        }
                        bot_log(f"Loaded Specs for {sym}: {self.instrument_specs[sym]}")
        except Exception as e:
            bot_log(f"Error fetching specs: {e}", level="error")

    def _round_to_step(self, value: float, step: float) -> float:
        if step == 0: return value
        rounded = round(value / step) * step
        return float(f"{rounded:.10g}")

    def run(self):
        bot_log("Bot started. Running Dual-Loop: Entries (1m), Exits (3s).")
        last_entry_run = 0

        while True:
            cycle_start = time.time()
            
            # --- 1. Fast Loop: Exit Monitor, Protection & Cleanup ---
            self._monitor_exits()
            
            # --- 2. Slow Loop: Entry Grid Logic ---
            dt_now = datetime.now(timezone.utc)
            if dt_now.second >= 5 and dt_now.second < 10:
                if cycle_start - last_entry_run > 45:
                    bot_log(f">>> ENTRY TRIGGER: {dt_now.strftime('%H:%M:%S')} <<<")
                    self._process_entry_cycle()
                    last_entry_run = time.time()
            
            time.sleep(3)

    # --- Fast Loop Logic (Exits & Cleanup) ---

    def _monitor_exits(self):
        """
        Monitors both Long and Short accounts independently.
        """
        if not self.active_params:
            return

        with self.param_lock:
            current_params = self.active_params.copy()

        # Check Long Account
        self._monitor_account(self.kf_long, "LONG", current_params)
        
        # Check Short Account
        self._monitor_account(self.kf_short, "SHORT", current_params)

    def _monitor_account(self, client: KrakenFuturesApi, acct_name: str, params_map: Dict):
        try:
            raw_pos = client.get_open_positions()
            raw_ord = client.get_open_orders()
            raw_tick = client.get_tickers()
        except Exception as e:
            bot_log(f"[{acct_name}] Monitor Fetch Error: {e}", level="error")
            return

        pos_map = {p["symbol"].upper(): p for p in raw_pos.get("openPositions", [])}
        
        ord_map = {}
        for o in raw_ord.get("openOrders", []):
            s = o["symbol"].upper()
            if s not in ord_map: ord_map[s] = []
            ord_map[s].append(o)
            
        tick_map = {t["symbol"].upper(): float(t["markPrice"]) for t in raw_tick.get("tickers", [])}

        for app_symbol, params in params_map.items():
            kraken_symbol = SYMBOL_MAP.get(app_symbol)
            if not kraken_symbol: continue
            
            sym_upper = kraken_symbol.upper()
            
            # We have a position?
            if sym_upper in pos_map:
                p_data = pos_map[sym_upper]
                size = float(p_data["size"])
                if p_data["side"] == "short": size = -size
                entry = float(p_data["price"])
                
                current_price = tick_map.get(sym_upper, 0.0)
                if current_price == 0: continue

                self._manage_active_position(
                    client,
                    acct_name,
                    kraken_symbol, 
                    size, 
                    entry, 
                    current_price, 
                    ord_map.get(sym_upper, []), 
                    params
                )

    def _manage_active_position(self, client: KrakenFuturesApi, acct_name: str, symbol_str: str, 
                                pos_size: float, entry_price: float, current_price: float, 
                                open_orders: List, params: Dict):
        """
        Strictly enforces Protection (SL/TP) for an active position.
        """
        symbol_lower = symbol_str.lower()
        symbol_upper = symbol_str.upper()
        stop_pct = params["stop_percent"]
        profit_pct = params["profit_percent"]
        specs = self.instrument_specs.get(symbol_upper)
        if not specs: return

        has_sl = False
        has_tp = False

        # Analyze existing orders
        for o in open_orders:
            o_type = o.get("orderType", o.get("type", "")).lower()
            o_reduce = o.get("reduceOnly", False)
            o_trigger = o.get("triggerSignal", None)
            o_stop_px = o.get("stopPrice", None)
            
            # Check for SL (stp OR trigger-based)
            is_sl_order = (o_type == "stp" or o_stop_px is not None or o_trigger is not None)
            if is_sl_order: has_sl = True
            
            # Check for TP (lmt + reduceOnly + NO trigger)
            is_tp_order = ((o_type == "lmt" and o_reduce and o_trigger is None) or o_type == "take_profit")
            if is_tp_order: has_tp = True

        # --- PROTECTION (Ensure SL/TP) ---
        if not has_sl or not has_tp:
            self._place_bracket_orders(
                client, acct_name,
                symbol_lower, pos_size, entry_price, current_price,
                stop_pct, profit_pct, specs["tickSize"],
                has_sl, has_tp
            )

    def _place_bracket_orders(self, client: KrakenFuturesApi, acct_name: str, symbol: str, 
                              position_size: float, entry_price: float, current_price: float,
                              sl_pct: float, tp_pct: float, tick_size: float, has_sl: bool, has_tp: bool):
        is_long = position_size > 0
        side = "sell" if is_long else "buy"
        abs_size = abs(position_size)

        # Handle SL Logic (Check & Place)
        if not has_sl:
            if is_long:
                sl_price = entry_price * (1 - sl_pct)
            else:
                sl_price = entry_price * (1 + sl_pct)
            sl_price = self._round_to_step(sl_price, tick_size)

            # Emergency Check (Only if SL is missing)
            sl_breached = False
            if is_long and current_price <= sl_price: sl_breached = True
            elif not is_long and current_price >= sl_price: sl_breached = True

            if sl_breached:
                bot_log(f"[{acct_name}:{symbol.upper()}] EMERGENCY: Price {current_price} crossed SL {sl_price}. Market Close.")
                try:
                    client.send_order({
                        "orderType": "mkt", "symbol": symbol, "side": side,
                        "size": abs_size, "reduceOnly": True
                    })
                except Exception as e:
                     bot_log(f"[{acct_name}:{symbol.upper()}] Emergency Close Failed: {e}", level="error")
                return # Don't place other orders if we are exiting

            # Place SL
            bot_log(f"[{acct_name}:{symbol.upper()}] SL MISSING. Placing at {sl_price}")
            try:
                client.send_order({
                    "orderType": "stp", "symbol": symbol, "side": side, "size": abs_size, 
                    "stopPrice": sl_price, "triggerSignal": "mark", "reduceOnly": True
                })
            except Exception as e:
                bot_log(f"[{acct_name}:{symbol.upper()}] SL Placement Failed: {e}", level="error")

        # Handle TP Logic (Place Only)
        if not has_tp:
            if is_long:
                tp_price = entry_price * (1 + tp_pct)
            else:
                tp_price = entry_price * (1 - tp_pct)
            tp_price = self._round_to_step(tp_price, tick_size)

            bot_log(f"[{acct_name}:{symbol.upper()}] TP MISSING. Placing at {tp_price}")
            try:
                client.send_order({
                    "orderType": "lmt", "symbol": symbol, "side": side, "size": abs_size, 
                    "limitPrice": tp_price, "reduceOnly": True
                })
            except Exception as e:
                bot_log(f"[{acct_name}:{symbol.upper()}] TP Placement Failed: {e}", level="error")

    # --- Slow Loop Logic (Entries) ---

    def _process_entry_cycle(self):
        new_params = self.fetcher.fetch_all_params()
        if not new_params:
            return
        
        with self.param_lock:
            self.active_params = new_params
        
        try:
            # Check Equity Long
            acc_l = self.kf_long.get_accounts()
            if "flex" in acc_l.get("accounts", {}):
                eq_l = float(acc_l["accounts"]["flex"].get("marginEquity", 0))
            else:
                eq_l = float(list(acc_l.get("accounts", {}).values())[0].get("marginEquity", 0))

            # Check Equity Short
            acc_s = self.kf_short.get_accounts()
            if "flex" in acc_s.get("accounts", {}):
                eq_s = float(acc_s["accounts"]["flex"].get("marginEquity", 0))
            else:
                eq_s = float(list(acc_s.get("accounts", {}).values())[0].get("marginEquity", 0))

            if eq_l <= 0 and eq_s <= 0: return
        except Exception:
            return

        limited_keys = list(new_params.keys())[:TEST_ASSET_LIMIT]
        limited_params = {k: new_params[k] for k in limited_keys}
        active_assets_count = len(limited_params)
        
        if active_assets_count == 0: return
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for app_symbol, params in limited_params.items():
                kraken_symbol = SYMBOL_MAP.get(app_symbol)
                if not kraken_symbol: continue
                
                executor.submit(
                    self._execute_entry_logic, 
                    kraken_symbol, 
                    eq_l,
                    eq_s,
                    params, 
                    active_assets_count
                )

    def _execute_entry_logic(self, symbol_str: str, eq_l: float, eq_s: float, params: Dict, asset_count: int):
        time.sleep(random.uniform(0.1, 1.5))
        
        symbol_upper = symbol_str.upper()
        symbol_lower = symbol_str.lower()
        
        grid_lines = np.sort(np.array(params["line_prices"]))
        specs = self.instrument_specs.get(symbol_upper)
        if not specs: return

        # Get Mark Price (from Long client is fine)
        current_price = self._get_mark_price(self.kf_long, symbol_upper)
        if current_price == 0: return

        # --- STEP 1: CLEANUP EXISTING ENTRIES ---
        # Cancel stale Grid orders on both accounts.
        self._cancel_entry_orders(self.kf_long, symbol_lower)
        self._cancel_entry_orders(self.kf_short, symbol_lower)

        # --- STEP 2: PLACE NEW GRID (ALL LINES) ---
        # Allocation
        safe_equity_l = eq_l * 0.95
        safe_equity_s = eq_s * 0.95
        
        alloc_l = safe_equity_l / max(1, asset_count)
        alloc_s = safe_equity_s / max(1, asset_count)
        
        unit_usd_l = (alloc_l * LEVERAGE) * 0.20
        unit_usd_s = (alloc_s * LEVERAGE) * 0.20
        
        qty_l = self._round_to_step(unit_usd_l / current_price, specs["sizeStep"])
        qty_s = self._round_to_step(unit_usd_s / current_price, specs["sizeStep"])

        # Iterate EVERY line in params
        for line in grid_lines:
            price = self._round_to_step(line, specs["tickSize"])
            
            # LONG GRID (Buy Limits below Price) -> Post to Long Key
            if price < current_price:
                if qty_l >= specs["sizeStep"]:
                    self.kf_long.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "buy",
                        "size": qty_l, "limitPrice": price
                    })
            
            # SHORT GRID (Sell Limits above Price) -> Post to Short Key
            elif price > current_price:
                if qty_s >= specs["sizeStep"]:
                    self.kf_short.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": "sell",
                        "size": qty_s, "limitPrice": price
                    })
            
            time.sleep(0.05) # Rate limit protection

    def _cancel_entry_orders(self, client: KrakenFuturesApi, symbol_lower: str):
        """
        Fetches open orders and cancels only those that are Grid Entries.
        Definition of Entry: Limit Order AND NOT reduceOnly.
        """
        try:
            open_orders = client.get_open_orders().get("openOrders", [])
            for o in open_orders:
                if o["symbol"].lower() != symbol_lower: continue
                
                o_type = o.get("orderType", o.get("type", "")).lower()
                o_reduce = o.get("reduceOnly", False)
                o_trigger = o.get("triggerSignal", None)
                
                # Identify Entry Order
                if o_type == "lmt" and not o_reduce and o_trigger is None:
                    client.cancel_order({"order_id": o["order_id"], "symbol": symbol_lower})
        except Exception:
            pass

    # --- Helpers ---

    def _get_mark_price(self, client: KrakenFuturesApi, kf_symbol_upper: str) -> float:
        try:
            tickers = client.get_tickers()
            for t in tickers.get("tickers", []):
                if t["symbol"].upper() == kf_symbol_upper:
                    return float(t["markPrice"])
            return 0.0
        except Exception:
            return 0.0

if __name__ == "__main__":
    bot = OctopusGridBot()
    bot.initialize()
    bot.run()
