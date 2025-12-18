"""Arbitrage bot between Extended (maker) and Lighter (taker)."""
import asyncio
import json
import logging
import os
import signal
import sys
import time
from decimal import Decimal
from typing import Optional, Tuple

import requests
import websockets
from lighter.signer_client import SignerClient

from exchanges.extended import ExtendedClient


class Config:
    """Simple config class to wrap dictionary for Extended client."""

    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class ExtendedArb:
    """Arbitrage trading bot using Extended for maker orders and Lighter for taker orders."""

    def __init__(
        self,
        ticker: str,
        order_quantity: Decimal,
        fill_timeout: int = 5,
        long_ex_threshold: Decimal = Decimal("10"),
        short_ex_threshold: Decimal = Decimal("10"),
    ):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.long_ex_threshold = long_ex_threshold
        self.short_ex_threshold = short_ex_threshold

        self.stop_flag = False

        self.extended_client: Optional[ExtendedClient] = None
        self.lighter_client: Optional[SignerClient] = None

        self.extended_contract_id: Optional[str] = None
        self.extended_best_bid: Optional[Decimal] = None
        self.extended_best_ask: Optional[Decimal] = None

        self.lighter_market_index: Optional[int] = None
        self.base_amount_multiplier: Optional[int] = None
        self.price_multiplier: Optional[int] = None
        self.lighter_best_bid: Optional[Decimal] = None
        self.lighter_best_ask: Optional[Decimal] = None

        self._setup_logger()

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    def _setup_logger(self) -> None:
        os.makedirs("logs", exist_ok=True)
        log_filename = f"logs/extended_{self.ticker}_log.txt"

        self.logger = logging.getLogger(f"extended_arb_{self.ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        file_handler = logging.FileHandler(log_filename)
        console_handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

    def initialize_lighter_client(self) -> SignerClient:
        if self.lighter_client is None:
            api_key_private_key = os.getenv("API_KEY_PRIVATE_KEY")
            if not api_key_private_key:
                raise ValueError("API_KEY_PRIVATE_KEY environment variable not set")

            account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
            api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))

            self.lighter_client = SignerClient(
                url="https://mainnet.zklighter.elliot.ai",
                private_key=api_key_private_key,
                account_index=account_index,
                api_key_index=api_key_index,
            )

            err = self.lighter_client.check_client()
            if err is not None:
                raise Exception(f"CheckClient error: {err}")

            self.logger.info("✅ Lighter client initialized successfully")
        return self.lighter_client

    def initialize_extended_client(self) -> ExtendedClient:
        config_dict = {
            "ticker": self.ticker,
            "contract_id": "",
            "quantity": self.order_quantity,
            "tick_size": Decimal("0.01"),
            "close_order_side": "sell",
            "vault": os.getenv("EXTENDED_VAULT", ""),
            "stark_private_key": os.getenv("EXTENDED_STARK_KEY_PRIVATE", ""),
            "api_key": os.getenv("EXTENDED_API_KEY", ""),
        }
        config = Config(config_dict)
        self.extended_client = ExtendedClient(config)
        self.logger.info("✅ Extended client initialized successfully")
        return self.extended_client

    async def get_extended_contract_info(self) -> Tuple[str, Decimal]:
        """Fetch contract attributes from Extended client."""
        if not self.extended_client:
            raise RuntimeError("Extended client not initialized")

        attributes = await self.extended_client.get_contract_attributes(self.ticker)
        contract_id = attributes.get("contract_id") or attributes.get("contractId")
        tick_size = Decimal(attributes.get("tick_size", "0.01"))
        if not contract_id:
            raise ValueError("Failed to load Extended contract_id")
        return contract_id, tick_size

    def setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    # ------------------------------------------------------------------
    # WebSocket handling for Extended order book
    # ------------------------------------------------------------------
    async def setup_extended_depth_websocket(self) -> None:
        """Setup separate WebSocket connection for Extended depth updates."""
        market_name = f"{self.ticker}-USD"
        url = (
            "wss://api.starknet.extended.exchange/stream.extended.exchange/"
            f"v1/orderbooks/{market_name}?depth=1"
        )

        async def handle_depth_websocket():
            while not self.stop_flag:
                try:
                    async with websockets.connect(url) as ws:
                        self.logger.info("✅ Connected to Extended order book stream")
                        async for message in ws:
                            if self.stop_flag:
                                break
                            data = json.loads(message)
                            if data.get("type") in ["SNAPSHOT", "DELTA"]:
                                self.handle_extended_order_book_update(data)
                except Exception as exc:
                    self.logger.error(f"WS Error: {exc}")
                    await asyncio.sleep(2)

        asyncio.create_task(handle_depth_websocket())

    def handle_extended_order_book_update(self, message: dict) -> None:
        data = message.get("data", {})
        if not data:
            return

        bids = data.get("b", [])
        asks = data.get("a", [])

        if bids:
            bid = bids[0]
            if isinstance(bid, dict):
                self.extended_best_bid = Decimal(bid.get("p", "0"))
            else:
                self.extended_best_bid = Decimal(bid[0])

        if asks:
            ask = asks[0]
            if isinstance(ask, dict):
                self.extended_best_ask = Decimal(ask.get("p", "0"))
            else:
                self.extended_best_ask = Decimal(ask[0])

    # ------------------------------------------------------------------
    # Market data helpers
    # ------------------------------------------------------------------
    def get_extended_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        return self.extended_best_bid, self.extended_best_ask

    def get_lighter_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        url = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
        response = requests.get(url, headers={"accept": "application/json"}, timeout=10)
        response.raise_for_status()
        data = response.json()
        for market in data.get("order_books", []):
            if market.get("symbol") == self.ticker:
                bids = market.get("orderbook_bids", [])
                asks = market.get("orderbook_asks", [])
                if bids:
                    self.lighter_best_bid = Decimal(bids[0][0])
                if asks:
                    self.lighter_best_ask = Decimal(asks[0][0])
                return self.lighter_best_bid, self.lighter_best_ask
        return None, None

    def get_lighter_market_config(self) -> Tuple[int, int, int]:
        url = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
        response = requests.get(url, headers={"accept": "application/json"}, timeout=10)
        response.raise_for_status()
        data = response.json()
        for market in data.get("order_books", []):
            if market.get("symbol") == self.ticker:
                market_id = market.get("market_id")
                base_multiplier = pow(10, market.get("supported_size_decimals", 0))
                price_multiplier = pow(10, market.get("supported_price_decimals", 0))
                return market_id, base_multiplier, price_multiplier
        raise ValueError(f"Ticker {self.ticker} not found on Lighter")

    # ------------------------------------------------------------------
    # Trading actions
    # ------------------------------------------------------------------
    async def place_extended_maker_order(self, side: str, quantity: Decimal) -> bool:
        best_bid = self.extended_best_bid
        best_ask = self.extended_best_ask
        price = best_bid if side == "buy" else best_ask

        order_result = await self.extended_client.place_open_order(
            contract_id=self.extended_contract_id,
            quantity=quantity,
            direction=side.lower(),
        )

        if getattr(order_result, "success", False):
            self.logger.info(
                f"[Extended] Post-only {side} order placed @ {price} for {quantity}"
            )
            # Wait for fill (simplified placeholder)
            await asyncio.sleep(self.fill_timeout)
            return True
        self.logger.error("Failed to place Extended maker order")
        return False

    async def place_lighter_taker_order(self, side: str, quantity: Decimal) -> Optional[str]:
        if not self.lighter_client:
            raise RuntimeError("Lighter client not initialized")

        best_bid, best_ask = self.lighter_best_bid, self.lighter_best_ask
        if not best_bid or not best_ask:
            best_bid, best_ask = self.get_lighter_bbo()

        is_ask = side.lower() == "sell"
        price = best_bid if is_ask else best_ask
        price = price * (Decimal("0.998") if is_ask else Decimal("1.002"))

        client_order_index = int(time.time() * 1000)
        tx_info, error = self.lighter_client.sign_create_order(
            market_index=self.lighter_market_index,
            client_order_index=client_order_index,
            base_amount=int(quantity * self.base_amount_multiplier),
            price=int(price * self.price_multiplier),
            is_ask=is_ask,
            order_type=self.lighter_client.ORDER_TYPE_LIMIT,
            time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            reduce_only=False,
            trigger_price=0,
        )
        if error is not None:
            self.logger.error(f"❌ Lighter sign error: {error}")
            return None

        tx_hash = await self.lighter_client.send_tx(
            tx_type=self.lighter_client.TX_TYPE_CREATE_ORDER, tx_info=tx_info
        )
        self.logger.info(
            f"[Lighter] Taker {side} order placed @ {price} for {quantity} (tx: {tx_hash})"
        )
        return tx_hash

    # ------------------------------------------------------------------
    # Main trading loop
    # ------------------------------------------------------------------
    async def trading_loop(self) -> None:
        self.logger.info(f"🚀 Starting Extended/Lighter arbitrage for {self.ticker}")

        try:
            self.initialize_lighter_client()
            self.initialize_extended_client()
            self.extended_contract_id, tick_size = await self.get_extended_contract_info()
            self.logger.info(
                f"Contract info loaded - Extended: {self.extended_contract_id}, tick {tick_size}"
            )
            (
                self.lighter_market_index,
                self.base_amount_multiplier,
                self.price_multiplier,
            ) = self.get_lighter_market_config()
        except Exception as exc:
            self.logger.error(f"❌ Initialization failed: {exc}")
            return

        await self.setup_extended_depth_websocket()
        await asyncio.sleep(2)

        while not self.stop_flag:
            try:
                ex_best_bid, ex_best_ask = self.get_extended_bbo()
                lighter_bid, lighter_ask = self.get_lighter_bbo()

                long_opportunity = (
                    lighter_bid
                    and ex_best_bid
                    and lighter_bid - ex_best_bid > self.long_ex_threshold
                )
                short_opportunity = (
                    ex_best_ask
                    and lighter_ask
                    and ex_best_ask - lighter_ask > self.short_ex_threshold
                )

                if long_opportunity:
                    self.logger.info("⚡ Long arbitrage opportunity detected")
                    filled = await self.place_extended_maker_order("buy", self.order_quantity)
                    if filled and not self.stop_flag:
                        await self.place_lighter_taker_order("sell", self.order_quantity)
                elif short_opportunity:
                    self.logger.info("⚡ Short arbitrage opportunity detected")
                    filled = await self.place_extended_maker_order("sell", self.order_quantity)
                    if filled and not self.stop_flag:
                        await self.place_lighter_taker_order("buy", self.order_quantity)

                await asyncio.sleep(0.5)
            except Exception as exc:
                if self.stop_flag:
                    break
                self.logger.error(f"⚠️ Error in trading loop: {exc}")
                await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def shutdown(self, signum=None, frame=None) -> None:
        if self.stop_flag:
            return
        self.stop_flag = True
        if signum is not None:
            self.logger.info("\n🛑 Stopping...")
        else:
            self.logger.info("🛑 Stopping...")


async def main():
    ticker = os.getenv("TICKER", "ETH")
    quantity = Decimal(os.getenv("ORDER_QTY", "0.01"))
    long_threshold = Decimal(os.getenv("LONG_EXT_THRESHOLD", "10"))
    short_threshold = Decimal(os.getenv("SHORT_EXT_THRESHOLD", "10"))

    bot = ExtendedArb(
        ticker=ticker,
        order_quantity=quantity,
        long_ex_threshold=long_threshold,
        short_ex_threshold=short_threshold,
    )
    bot.setup_signal_handlers()
    await bot.trading_loop()


if __name__ == "__main__":
    asyncio.run(main())
