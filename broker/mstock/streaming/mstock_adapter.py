"""
mstock WebSocket adapter implementation (synchronous).

Uses sync websocket-client to avoid asyncio event loop conflicts
with eventlet in gunicorn+eventlet deployments.
"""
import copy
import json
import logging
import os
import sys
import threading
import time
from typing import Any

from broker.mstock.api.data import BrokerData
from broker.mstock.api.mstockwebsocket import MstockWebSocket
from database.auth_db import get_auth_token
from database.token_db import get_token

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))

from websocket_proxy.base_adapter import BaseBrokerWebSocketAdapter
from websocket_proxy.mapping import SymbolMapper

from .mstock_mapping import MstockCapabilityRegistry, MstockExchangeMapper


class MstockWebSocketAdapter(BaseBrokerWebSocketAdapter):
    """mstock-specific implementation of the WebSocket adapter"""

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("mstock_websocket")
        self.ws_client = None
        self.data_client = None
        self.user_id = None
        self.broker_name = "mstock"
        self.running = False
        self.lock = threading.Lock()
        self.auth_token = None
        self.token_modes = {}
        self.token_correlation_ids = {}

        # Subscribe coalescing — mirrors angel/zerodha subscription_queue + batch_timer.
        # Per-symbol subscribe() calls append to the queue and arm a 500ms timer;
        # the timer drains the queue and emits broker-side subscribe messages. The
        # delay also bridges the cold-start race between connect() returning and
        # _on_connect firing.
        self.subscription_queue: list[dict[str, Any]] = []
        self.batch_timer: threading.Timer | None = None
        self.batch_delay = 0.5  # seconds — matches angel/zerodha fleet pattern

    def initialize(
        self, broker_name: str, user_id: str, auth_data: dict[str, str] | None = None
    ) -> None:
        self.user_id = user_id
        self.broker_name = broker_name

        if not auth_data:
            auth_token = get_auth_token(user_id, bypass_cache=True)
            if not auth_token:
                self.logger.error(f"No authentication token found for user {user_id}")
                raise ValueError(f"No authentication token found for user {user_id}")
        else:
            auth_token = auth_data.get("auth_token")
            if not auth_token:
                self.logger.error("Missing required authentication data")
                raise ValueError("Missing required authentication data")

        self.auth_token = auth_token
        self.data_client = BrokerData(auth_token=auth_token)
        # Pass a token_provider so the client re-reads a fresh access token from
        # the database before each reconnect; Indian broker tokens roll over
        # daily (~3 AM IST) and the construction-time token is dead after rollover.
        self.ws_client = MstockWebSocket(
            auth_token=auth_token, token_provider=self._get_fresh_auth_token
        )
        self.ws_client.on_connect = self._on_connect
        self.logger.info(f"mstock adapter initialized for user {user_id}")

    def _get_fresh_auth_token(self) -> str | None:
        """
        Re-read a fresh access token from the database for the current user.

        Used as the token_provider for MstockWebSocket so reconnects after the
        daily token rollover (~3 AM IST) pick up a live token. Returns None on
        failure so the client keeps its existing token.
        """
        if not self.user_id:
            return None
        try:
            return get_auth_token(self.user_id, bypass_cache=True)
        except Exception as e:
            self.logger.warning(f"Failed to re-read fresh mstock auth token: {e}")
            return None

    def connect(self) -> None:
        """Establish persistent connection to mstock WebSocket"""
        if not self.ws_client:
            self.logger.error("WebSocket client not initialized. Call initialize() first.")
            return

        self.logger.info("Connecting to mstock WebSocket in streaming mode...")
        self.running = True

        # Start streaming — connection happens in background thread (Angel/Upstox pattern).
        self.ws_client.connect_stream(self._on_data)

        # Wait for handshake to complete (Zerodha pattern).
        self.logger.info("Waiting for WebSocket connection...")
        if self.ws_client.wait_for_connection(timeout=15.0):
            self.connected = True
            self.logger.info("mstock WebSocket adapter connected")
        elif self.ws_client.running:
            self.logger.warning("Client started but connection timeout")
        else:
            self.logger.error("Failed to establish mstock WebSocket connection")

    def _on_connect(self) -> None:
        """Callback when the broker session is ready — replay adapter subscriptions."""
        self.logger.info("mstock WebSocket session ready")
        self.connected = True

        with self.lock:
            if self.batch_timer is not None:
                self.batch_timer.cancel()
                self.batch_timer = None
            self.subscription_queue.clear()
            tokens_to_replay = dict(self.token_modes)
            ws_subs = (
                list(self.ws_client.subscriptions.values())
                if self.ws_client
                else []
            )

        for token, mode in tokens_to_replay.items():
            exchange_type = None
            symbol = token
            with self.lock:
                for sub in self.subscriptions.values():
                    if sub["token"] == token:
                        exchange_type = sub["exchange_type"]
                        symbol = sub["symbol"]
                        break

            if exchange_type is None:
                continue

            current_mstock_mode = max(
                (
                    ws_sub.get("mode", 0)
                    for ws_sub in ws_subs
                    if ws_sub.get("token") == token
                ),
                default=0,
            )
            self._send_ws_subscription(
                symbol, token, exchange_type, mode, current_mstock_mode
            )

    def _start_batch_timer(self) -> None:
        """Arm the coalescing timer that drains subscription_queue."""
        with self.lock:
            if self.batch_timer is not None:
                self.batch_timer.cancel()
            self.batch_timer = threading.Timer(
                self.batch_delay, self._process_batch_subscriptions
            )
            self.batch_timer.daemon = True
            self.batch_timer.start()

    def _process_batch_subscriptions(self) -> None:
        """Drain the queue and send broker subscribe messages."""
        with self.lock:
            if not self.subscription_queue:
                self.batch_timer = None
                return
            pending = list(self.subscription_queue)
            self.subscription_queue.clear()
            self.batch_timer = None

        if not self.connected or not self.ws_client:
            self.logger.warning(
                f"Dropping batch of {len(pending)} subscriptions — not connected; "
                f"_on_connect will replay from self.subscriptions"
            )
            return

        # Collapse to one subscribe per token using the highest queued mode.
        by_token: dict[str, dict[str, Any]] = {}
        for sub in pending:
            token = sub["token"]
            existing = by_token.get(token)
            if existing is None or sub["subscribe_mode"] > existing["subscribe_mode"]:
                by_token[token] = sub

        for sub in by_token.values():
            self._send_ws_subscription(
                sub["symbol"],
                sub["token"],
                sub["exchange_type"],
                sub["subscribe_mode"],
                sub["current_mstock_mode"],
            )

    def _send_ws_subscription(
        self, symbol: str, token: str, exchange_type: int, subscribe_mode: int, current_mstock_mode: int
    ) -> None:
        """Subscribe or upgrade a token on the broker WebSocket."""
        try:
            if current_mstock_mode > 0 and token in self.token_correlation_ids:
                old_correlation_id = self.token_correlation_ids[token]
                if old_correlation_id in self.ws_client.subscriptions:
                    self.ws_client.unsubscribe_stream(old_correlation_id)
                    time.sleep(0.2)

            mstock_correlation_id = f"mstock_{token}_{subscribe_mode}"
            result = self.ws_client.subscribe_stream(
                mstock_correlation_id, token, exchange_type, subscribe_mode
            )

            if result:
                self.token_correlation_ids[token] = mstock_correlation_id
                self.logger.info(
                    f"Subscribed to {symbol} (token: {token}) with mode {subscribe_mode}"
                )
            else:
                self.logger.warning(f"Failed to subscribe to {symbol}")
        except Exception as e:
            self.logger.error(f"Error subscribing: {str(e)}")

    def _on_data(self, quote_data: dict) -> None:
        """Callback function called when data is received from WebSocket"""
        try:
            token = quote_data.get("token")
            if not token:
                self.logger.warning("Received data without token")
                return

            matching_subscriptions = []
            with self.lock:
                for correlation_id, sub in self.subscriptions.items():
                    if sub["token"] == token:
                        matching_subscriptions.append(sub)

            if not matching_subscriptions:
                self.logger.warning(f"Received data for unsubscribed token: '{token}'")
                return

            packet_mode = quote_data.get("subscription_mode", 1)
            market_data_base = self._normalize_market_data(quote_data, packet_mode)

            for subscription in matching_subscriptions:
                symbol = subscription["symbol"]
                exchange = subscription["exchange"]
                mode = subscription["mode"]
                mode_str = {1: "LTP", 2: "QUOTE", 3: "DEPTH"}[mode]
                topic = f"{exchange}_{symbol}_{mode_str}"

                market_data = copy.deepcopy(market_data_base)
                market_data.update({
                    "symbol": symbol,
                    "exchange": exchange,
                    "mode": mode,
                    "timestamp": int(time.time() * 1000),
                })

                self.publish_market_data(topic, market_data)
                self.logger.debug(f"Published data for {symbol} on {exchange} mode {mode}")

        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}", exc_info=True)

    def disconnect(self) -> None:
        """Disconnect from mstock WebSocket"""
        with self.lock:
            self.running = False
            if self.batch_timer is not None:
                self.batch_timer.cancel()
                self.batch_timer = None
            self.subscription_queue.clear()

        if self.ws_client:
            self.ws_client.disconnect_stream()

        self.connected = False
        self.logger.info("mstock WebSocket adapter disconnected")
        self.cleanup_zmq()

    def subscribe(
        self, symbol: str, exchange: str, mode: int = 2, depth_level: int = 5
    ) -> dict[str, Any]:
        if mode not in [1, 2, 3]:
            return self._create_error_response(
                "INVALID_MODE", f"Invalid mode {mode}. Must be 1 (LTP), 2 (Quote), or 3 (Depth)"
            )

        if mode == 3 and depth_level not in [5]:
            return self._create_error_response(
                "INVALID_DEPTH", f"Invalid depth level {depth_level}. mstock only supports 5 levels"
            )

        if not self.running:
            return self._create_error_response(
                "NOT_CONNECTED", "WebSocket not connected. Call connect() first."
            )

        if not self.ws_client:
            return self._create_error_response(
                "NOT_INITIALIZED", "WebSocket client not initialized"
            )

        token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
        if not token_info:
            return self._create_error_response(
                "SYMBOL_NOT_FOUND", f"Symbol {symbol} not found for exchange {exchange}"
            )

        token = token_info["token"]
        brexchange = token_info["brexchange"]
        exchange_type = MstockExchangeMapper.get_exchange_type(brexchange)
        correlation_id = f"{symbol}_{exchange}_{mode}"

        needs_ws_subscribe = False
        subscribe_mode = mode

        with self.lock:
            self.subscriptions[correlation_id] = {
                "symbol": symbol,
                "exchange": exchange,
                "brexchange": brexchange,
                "token": token,
                "mode": mode,
                "depth_level": depth_level,
                "exchange_type": exchange_type,
            }

            max_mode_for_token = mode
            for sub in self.subscriptions.values():
                if sub["token"] == token:
                    max_mode_for_token = max(max_mode_for_token, sub["mode"])

            current_mstock_mode = self.token_modes.get(token, 0)
            if max_mode_for_token > current_mstock_mode:
                needs_ws_subscribe = True
                subscribe_mode = max_mode_for_token
                self.token_modes[token] = max_mode_for_token

        if needs_ws_subscribe and self.ws_client and self.running:
            if not self.ws_client.is_connected():
                self.logger.warning("WebSocket not connected, waiting for connection...")
                if not self.ws_client.wait_for_connection(timeout=10.0):
                    self.logger.warning(
                        f"Subscription for {symbol} stored locally; will be sent from _on_connect"
                    )
                else:
                    self.connected = True

            if self.connected and self.ws_client:
                try:
                    with self.lock:
                        self.subscription_queue.append({
                            "symbol": symbol,
                            "token": token,
                            "exchange_type": exchange_type,
                            "subscribe_mode": subscribe_mode,
                            "current_mstock_mode": current_mstock_mode,
                        })
                        if len(self.subscription_queue) == 1:
                            self._start_batch_timer()
                except Exception as e:
                    self.logger.error(f"Error queuing subscription for {symbol}.{exchange}: {e}")
                    return self._create_error_response("SUBSCRIPTION_ERROR", str(e))

        return {
            "status": "success",
            "message": f"Subscribed to {symbol} on {exchange} in mode {mode}",
            "correlation_id": correlation_id,
        }

    def _normalize_market_data(self, quote_data: dict, mode: int) -> dict[str, Any]:
        try:
            normalized = {"ltp": float(quote_data.get("ltp", 0))}

            if mode >= 2:
                normalized.update({
                    "open": float(quote_data.get("open", 0)),
                    "high": float(quote_data.get("high", 0)),
                    "low": float(quote_data.get("low", 0)),
                    "close": float(quote_data.get("close", 0)),
                    "prev_close": float(quote_data.get("close", 0)),
                    "volume": int(quote_data.get("volume", 0)),
                    "oi": int(quote_data.get("oi", 0)),
                    "last_trade_quantity": int(quote_data.get("last_traded_qty", 0)),
                    "average_price": float(quote_data.get("avg_price", 0)),
                    "total_buy_quantity": int(quote_data.get("total_buy_qty", 0)),
                    "total_sell_quantity": int(quote_data.get("total_sell_qty", 0)),
                })

            if mode == 3:
                bids = quote_data.get("bids", [])[:5]
                asks = quote_data.get("asks", [])[:5]

                formatted_bids = []
                for bid in bids:
                    if isinstance(bid, dict):
                        formatted_bids.append({
                            "price": float(bid.get("price", 0)),
                            "quantity": int(bid.get("quantity", 0)),
                            "orders": int(bid.get("orders", 0)),
                        })
                    elif isinstance(bid, (list, tuple)) and len(bid) >= 2:
                        formatted_bids.append({
                            "price": float(bid[0]),
                            "quantity": int(bid[1]),
                            "orders": int(bid[2]) if len(bid) > 2 else 0,
                        })

                formatted_asks = []
                for ask in asks:
                    if isinstance(ask, dict):
                        formatted_asks.append({
                            "price": float(ask.get("price", 0)),
                            "quantity": int(ask.get("quantity", 0)),
                            "orders": int(ask.get("orders", 0)),
                        })
                    elif isinstance(ask, (list, tuple)) and len(ask) >= 2:
                        formatted_asks.append({
                            "price": float(ask[0]),
                            "quantity": int(ask[1]),
                            "orders": int(ask[2]) if len(ask) > 2 else 0,
                        })

                normalized["depth"] = {"buy": formatted_bids, "sell": formatted_asks}
                normalized.update({
                    "total_buy_quantity": int(quote_data.get("total_buy_qty", 0)),
                    "total_sell_quantity": int(quote_data.get("total_sell_qty", 0)),
                    "upper_circuit": float(quote_data.get("upper_circuit", 0)),
                    "lower_circuit": float(quote_data.get("lower_circuit", 0)),
                })

            return normalized

        except Exception as e:
            self.logger.error(f"Error normalizing market data: {str(e)}")
            return {"ltp": 0}

    def unsubscribe(self, symbol: str, exchange: str, mode: int = 2) -> dict[str, Any]:
        correlation_id = f"{symbol}_{exchange}_{mode}"

        needs_ws_update = False
        new_mode = 0
        token = None
        exchange_type = None

        with self.lock:
            if correlation_id not in self.subscriptions:
                return self._create_error_response(
                    "NOT_SUBSCRIBED", f"{symbol} on {exchange} mode {mode} is not subscribed"
                )

            subscription = self.subscriptions[correlation_id]
            token = subscription["token"]
            exchange_type = subscription["exchange_type"]

            del self.subscriptions[correlation_id]

            max_mode_for_token = 0
            for sub in self.subscriptions.values():
                if sub["token"] == token:
                    max_mode_for_token = max(max_mode_for_token, sub["mode"])

            current_mstock_mode = self.token_modes.get(token, 0)
            if max_mode_for_token < current_mstock_mode:
                needs_ws_update = True
                new_mode = max_mode_for_token
                if new_mode > 0:
                    self.token_modes[token] = new_mode
                else:
                    self.token_modes.pop(token, None)
                    self.token_correlation_ids.pop(token, None)

        if needs_ws_update and self.ws_client and self.running:
            try:
                current_correlation_id = self.token_correlation_ids.get(token)

                if new_mode == 0:
                    if current_correlation_id:
                        self.ws_client.unsubscribe_stream(current_correlation_id)
                        self.logger.info(f"Unsubscribed token {token} from mstock")
                else:
                    if current_correlation_id and current_correlation_id in self.ws_client.subscriptions:
                        self.ws_client.unsubscribe_stream(current_correlation_id)
                        time.sleep(0.2)

                    new_correlation_id = f"mstock_{token}_{new_mode}"
                    self.ws_client.subscribe_stream(
                        new_correlation_id, token, exchange_type, new_mode
                    )
                    self.token_correlation_ids[token] = new_correlation_id
                    self.logger.debug(f"Downgraded subscription for token {token} to mode {new_mode}")

            except Exception as e:
                self.logger.error(f"Error updating WebSocket subscription: {str(e)}")

        return {
            "status": "success",
            "message": f"Unsubscribed from {symbol} on {exchange} mode {mode}",
        }

    def _create_error_response(self, error_code: str, message: str) -> dict[str, Any]:
        return {"status": "error", "error_code": error_code, "message": message}

    def get_subscriptions(self) -> list[dict[str, Any]]:
        with self.lock:
            return [
                {
                    "symbol": sub["symbol"],
                    "exchange": sub["exchange"],
                    "mode": sub["mode"],
                    "depth_level": sub.get("depth_level", 5),
                }
                for sub in self.subscriptions.values()
            ]
