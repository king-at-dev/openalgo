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
        # Official SDK pattern: subscribe only after LOGIN via on_connect.
        self.ws_client.on_connect = self._on_session_ready
        self.running = True
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

    def connect(self) -> dict[str, Any]:
        """
        Establish persistent connection to mstock WebSocket.

        Starts the broker socket in a background thread and returns quickly.
        Session readiness (LOGIN sent) is signaled via ``_on_session_ready``,
        matching the official MTicker on_connect lifecycle:

            send_login_after_connect()
            ws.subscribe(...)

        Early subscribe() calls are queued locally and flushed from that callback.
        """
        if not self.ws_client:
            self.logger.error("WebSocket client not initialized. Call initialize() first.")
            return {"status": "error", "message": "WebSocket client not initialized"}

        try:
            self.logger.info("Connecting to mstock WebSocket in streaming mode...")
            self.running = True
            self.connected = False

            # Ensure callback is wired (safe if initialize already set it).
            self.ws_client.on_connect = self._on_session_ready
            self.ws_client.connect_stream(self._on_data)

            # Do not block here — ConnectionPool.connect() holds its lock while
            # this runs. LOGIN + subscription flush happen asynchronously in
            # _on_session_ready (official on_connect pattern).
            self.logger.info(
                "mstock WebSocket connection started; subscriptions will flush after LOGIN"
            )
            return {
                "status": "success",
                "message": "Client started, connection in progress",
            }
        except Exception as e:
            self.logger.error(f"Error connecting mstock WebSocket: {e}")
            return {"status": "error", "message": str(e)}

    def _highest_mode_by_token(self) -> dict[str, dict[str, Any]]:
        """Collapse local subscriptions to the highest mode per broker token."""
        by_token: dict[str, dict[str, Any]] = {}
        with self.lock:
            for sub in self.subscriptions.values():
                token = sub["token"]
                existing = by_token.get(token)
                if existing is None or sub["mode"] > existing["mode"]:
                    by_token[token] = sub
        return by_token

    def _drop_stale_token_subscription(self, token: str, mode: int) -> None:
        """Unsubscribe a prior mode for ``token`` when upgrading/replaying."""
        if not self.ws_client:
            return
        old_correlation_id = self.token_correlation_ids.get(token)
        if not old_correlation_id or old_correlation_id not in self.ws_client.subscriptions:
            return
        old_mode = self.ws_client.subscriptions[old_correlation_id].get("mode", 0)
        if old_mode != mode:
            self.ws_client.unsubscribe_stream(old_correlation_id)

    def _flush_token_subscription(self, token: str, sub: dict[str, Any]) -> None:
        """Send one queued subscription to the broker after LOGIN."""
        mode = sub["mode"]
        exchange_type = sub["exchange_type"]
        symbol = sub["symbol"]
        try:
            self._drop_stale_token_subscription(token, mode)
            mstock_correlation_id = f"mstock_{token}_{mode}"
            if self.ws_client and self.ws_client.subscribe_stream(
                mstock_correlation_id, token, exchange_type, mode
            ):
                self.token_correlation_ids[token] = mstock_correlation_id
                self.token_modes[token] = mode
                self.logger.info(f"Flushed subscription {symbol} (token: {token}) mode {mode}")
            else:
                self.logger.warning(f"Failed to flush subscription for {symbol} (token: {token})")
        except Exception as e:
            self.logger.error(f"Error flushing subscription for {symbol}: {e}")

    def _on_session_ready(self) -> None:
        """
        Callback after LOGIN — mirrors official sample on_connect:

            send_login_after_connect()
            ws.subscribe(...)

        Flush every locally tracked subscription to the broker here so early
        subscribe() calls that arrived before LOGIN are not lost.
        """
        self.connected = True
        self.logger.info("mstock WebSocket session ready — flushing subscriptions")
        for token, sub in self._highest_mode_by_token().items():
            self._flush_token_subscription(token, sub)

    def _send_broker_subscribe(
        self,
        symbol: str,
        token: str,
        exchange_type: int,
        subscribe_mode: int,
        current_mstock_mode: int,
    ) -> bool:
        """Send (or upgrade) a broker-side subscription when the session is ready."""
        if not self.ws_client or not self.ws_client.is_connected():
            return False

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
            return bool(result)
        except Exception as e:
            self.logger.error(f"Error subscribing: {str(e)}")
            return False

    def _on_data(self, quote_data: dict) -> None:
        """Callback function called when data is received from WebSocket"""
        try:
            token = quote_data.get("token")
            if not token:
                self.logger.warning("Received data without token")
                return

            matching_subscriptions = []
            with self.lock:
                for _correlation_id, sub in self.subscriptions.items():
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
                market_data.update(
                    {
                        "symbol": symbol,
                        "exchange": exchange,
                        "mode": mode,
                        "timestamp": int(time.time() * 1000),
                    }
                )

                self.publish_market_data(topic, market_data)
                self.logger.debug(f"Published data for {symbol} on {exchange} mode {mode}")

        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}", exc_info=True)

    def disconnect(self) -> None:
        """Disconnect from mstock WebSocket"""
        self.running = False

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
            # Official SDK only subscribes after LOGIN (inside on_connect).
            # If the session is not ready yet, keep the local subscription and
            # let _on_session_ready flush it — do not treat this as failure.
            if not self.ws_client.is_connected():
                self.logger.info(f"Session not ready — queued {symbol} for subscribe after LOGIN")
            else:
                self._send_broker_subscribe(
                    symbol, token, exchange_type, subscribe_mode, current_mstock_mode
                )

        return {
            "status": "success",
            "message": f"Subscribed to {symbol} on {exchange} in mode {mode}",
            "correlation_id": correlation_id,
        }

    @staticmethod
    def _format_depth_level(level: Any) -> dict[str, Any] | None:
        """Normalize one bid/ask level from dict or list/tuple form."""
        if isinstance(level, dict):
            return {
                "price": float(level.get("price", 0)),
                "quantity": int(level.get("quantity", 0)),
                "orders": int(level.get("orders", 0)),
            }
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return {
                "price": float(level[0]),
                "quantity": int(level[1]),
                "orders": int(level[2]) if len(level) > 2 else 0,
            }
        return None

    @classmethod
    def _format_depth_side(cls, levels: list[Any]) -> list[dict[str, Any]]:
        """Format up to 5 depth levels for one book side."""
        formatted = []
        for level in levels[:5]:
            parsed = cls._format_depth_level(level)
            if parsed is not None:
                formatted.append(parsed)
        return formatted

    def _quote_fields(self, quote_data: dict) -> dict[str, Any]:
        """Map quote-mode fields from an mstock packet."""
        return {
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
        }

    def _depth_fields(self, quote_data: dict) -> dict[str, Any]:
        """Map depth-mode fields from an mstock packet."""
        return {
            "depth": {
                "buy": self._format_depth_side(quote_data.get("bids", [])),
                "sell": self._format_depth_side(quote_data.get("asks", [])),
            },
            "total_buy_quantity": int(quote_data.get("total_buy_qty", 0)),
            "total_sell_quantity": int(quote_data.get("total_sell_qty", 0)),
            "upper_circuit": float(quote_data.get("upper_circuit", 0)),
            "lower_circuit": float(quote_data.get("lower_circuit", 0)),
        }

    def _normalize_market_data(self, quote_data: dict, mode: int) -> dict[str, Any]:
        try:
            normalized = {"ltp": float(quote_data.get("ltp", 0))}
            if mode >= 2:
                normalized.update(self._quote_fields(quote_data))
            if mode == 3:
                normalized.update(self._depth_fields(quote_data))
            return normalized
        except Exception as e:
            self.logger.error(f"Error normalizing market data: {str(e)}")
            return {"ltp": 0}

    def _max_mode_for_token(self, token: str) -> int:
        """Highest remaining local subscription mode for a broker token."""
        max_mode = 0
        for sub in self.subscriptions.values():
            if sub["token"] == token:
                max_mode = max(max_mode, sub["mode"])
        return max_mode

    def _remove_local_subscription(
        self, correlation_id: str, symbol: str, exchange: str, mode: int
    ) -> tuple[str, int, bool, int] | dict[str, Any]:
        """
        Drop a local subscription and decide whether the broker feed must change.

        Returns either an error response dict, or
        ``(token, exchange_type, needs_ws_update, new_mode)``.
        """
        with self.lock:
            if correlation_id not in self.subscriptions:
                return self._create_error_response(
                    "NOT_SUBSCRIBED", f"{symbol} on {exchange} mode {mode} is not subscribed"
                )

            subscription = self.subscriptions[correlation_id]
            token = subscription["token"]
            exchange_type = subscription["exchange_type"]
            del self.subscriptions[correlation_id]

            max_mode_for_token = self._max_mode_for_token(token)
            current_mstock_mode = self.token_modes.get(token, 0)
            needs_ws_update = False
            new_mode = 0
            if max_mode_for_token < current_mstock_mode:
                needs_ws_update = True
                new_mode = max_mode_for_token
                if new_mode > 0:
                    self.token_modes[token] = new_mode
                else:
                    self.token_modes.pop(token, None)
                    self.token_correlation_ids.pop(token, None)

            return token, exchange_type, needs_ws_update, new_mode

    def _apply_broker_unsubscribe(self, token: str, exchange_type: int, new_mode: int) -> None:
        """Fully remove or downgrade the broker-side subscription for ``token``."""
        if not self.ws_client:
            return

        current_correlation_id = self.token_correlation_ids.get(token)
        if new_mode == 0:
            if current_correlation_id:
                self.ws_client.unsubscribe_stream(current_correlation_id)
                self.logger.info(f"Unsubscribed token {token} from mstock")
            return

        if current_correlation_id and current_correlation_id in self.ws_client.subscriptions:
            self.ws_client.unsubscribe_stream(current_correlation_id)
            time.sleep(0.2)

        new_correlation_id = f"mstock_{token}_{new_mode}"
        self.ws_client.subscribe_stream(new_correlation_id, token, exchange_type, new_mode)
        self.token_correlation_ids[token] = new_correlation_id
        self.logger.debug(f"Downgraded subscription for token {token} to mode {new_mode}")

    def unsubscribe(self, symbol: str, exchange: str, mode: int = 2) -> dict[str, Any]:
        correlation_id = f"{symbol}_{exchange}_{mode}"
        result = self._remove_local_subscription(correlation_id, symbol, exchange, mode)
        if isinstance(result, dict):
            return result

        token, exchange_type, needs_ws_update, new_mode = result
        if needs_ws_update and self.ws_client and self.running:
            try:
                self._apply_broker_unsubscribe(token, exchange_type, new_mode)
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
