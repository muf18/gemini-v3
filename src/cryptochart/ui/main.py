import asyncio
import sys
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from loguru import logger
from PySide6.QtCore import Slot
from PySide6.QtGui import QAction, QCloseEvent
from PySide6.QtWidgets import (
    QApplication,
    QInputDialog,
    QMainWindow,
    QMessageBox,
    QStatusBar,
)

from cryptochart.adapters import (
    binance,
    bitget,
    bitstamp,
    bitvavo,
    coinbase,
    digifinex,
    kraken,
    okx,
)
from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.aggregator import Aggregator
from cryptochart.config import settings
from cryptochart.logging_config import setup_logging
from cryptochart.normalizer import Normalizer
from cryptochart.persistence import Persistence
from cryptochart.publisher import Publisher
from cryptochart.types import models_pb2
from cryptochart.ui.qt_asyncio_integration import run_with_asyncio
from cryptochart.ui.views.chart_view import ChartView
from cryptochart.ui.views.settings_panel import SettingsPanel

# --- Constants ---
DEFAULT_TIMEFRAME = "1m"
DEFAULT_TIMEFRAME_SECONDS = 60
HISTORICAL_DATA_RANGE_DAYS = 90


class MainWindow(QMainWindow):
    """The main application window."""

    def __init__(self) -> None:
        super().__init__()
        self._active_adapters: list[ExchangeAdapter] = []
        self._active_subscription_id: int | None = None
        self._ui_update_task: asyncio.Task[None] | None = None
        self._settings_panel: SettingsPanel | None = None
        self._pair_selection_task: asyncio.Task[None] | None = None

        # --- Initialize Core Components ---
        self._http_client = httpx.AsyncClient(
            http2=True, timeout=20.0, follow_redirects=True
        )
        # Data pipeline queues
        self._adapter_q: asyncio.Queue[models_pb2.PriceUpdate] = asyncio.Queue()
        self._aggregator_q: asyncio.Queue[models_pb2.PriceUpdate] = asyncio.Queue()
        self._publisher_q: asyncio.Queue[models_pb2.PriceUpdate] = asyncio.Queue()
        self._persistence_q: asyncio.Queue[models_pb2.PriceUpdate] = asyncio.Queue()

        # Core services
        self._publisher = Publisher(self._publisher_q)
        self._persistence = Persistence(
            input_queue=self._persistence_q,
            output_directory=Path(settings.persistence.output_directory),
        )
        self._normalizer = Normalizer(self._adapter_q, self._aggregator_q)
        self._aggregator = Aggregator(self._aggregator_q, self._publisher_q)

        # Instantiate all available adapters
        self._all_adapters = self._instantiate_all_adapters()

        self._setup_ui()

    def _instantiate_all_adapters(self) -> list[ExchangeAdapter]:
        """Creates instances of all enabled exchange adapters."""
        adapter_classes = [
            coinbase.CoinbaseAdapter,
            kraken.KrakenAdapter,
            bitstamp.BitstampAdapter,
            binance.BinanceAdapter,
            okx.OKXAdapter,
            bitget.BitgetAdapter,
            digifinex.DigifinexAdapter,
            bitvavo.BitvavoAdapter,
        ]
        instances = []
        for adapter_cls in adapter_classes:
            instance = adapter_cls([], self._adapter_q, self._http_client)
            if getattr(settings.api, f"{instance.venue_name}_enabled", False):
                instances.append(instance)
                logger.info(f"Adapter enabled: {instance.venue_name}")
        return instances

    def _setup_ui(self) -> None:
        """Sets up the window, menus, and central widget."""
        self.setWindowTitle("CryptoChart")
        self.resize(1280, 720)

        self._chart_view = ChartView(self)
        self.setCentralWidget(self._chart_view)

        self.setStatusBar(QStatusBar(self))
        self.statusBar().showMessage("Ready. Please select a trading pair to begin.")

        menu_bar = self.menuBar()
        file_menu = menu_bar.addMenu("&File")

        pair_action = QAction("Select &Pair...", self)
        pair_action.triggered.connect(self._show_pair_selector)
        file_menu.addAction(pair_action)

        settings_action = QAction("&Settings...", self)
        settings_action.triggered.connect(self._show_settings)
        file_menu.addAction(settings_action)

        file_menu.addSeparator()

        exit_action = QAction("E&xit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

    async def start_services(self) -> None:
        """Starts all the core background services."""
        logger.info("Starting core application services...")
        self._publisher.start()
        await self._publisher.subscribe("*", "*", self._persistence_q)
        self._persistence.start()
        if settings.persistence.enabled_by_default:
            await self._persistence.set_enabled(True)
        self._normalizer.start()
        self._aggregator.start()
        logger.success("All core services started.")

    @Slot()
    def _show_pair_selector(self) -> None:
        """Slot to show the pair selection dialog."""
        self._pair_selection_task = asyncio.create_task(self._run_pair_selection_flow())

    async def _run_pair_selection_flow(self) -> None:
        """Orchestrates the process of selecting and loading a new pair."""
        self.statusBar().showMessage("Fetching available pairs...")
        all_pairs = {
            "BTC/USD",
            "BTC/EUR",
            "BTC/USDT",
            "ETH/USD",
            "ETH/EUR",
            "ETH/USDT",
        }

        selected_pair, ok = QInputDialog.getItem(
            self, "Select Pair", "Pair:", sorted(all_pairs), 0, False
        )

        if ok and selected_pair:
            self.statusBar().showMessage(f"Loading pair: {selected_pair}...")
            await self._reconfigure_for_new_pair(selected_pair)
            self.statusBar().showMessage(f"Live data for {selected_pair}", 5000)
        else:
            self.statusBar().showMessage("Pair selection cancelled.", 5000)

    async def _reconfigure_for_new_pair(self, pair: str) -> None:
        """Stops old services, starts new ones, and loads data for the new pair."""
        if self._ui_update_task:
            self._ui_update_task.cancel()
        if self._active_subscription_id:
            await self._publisher.unsubscribe(self._active_subscription_id)
        for adapter in self._active_adapters:
            await adapter.stop()
        self._active_adapters.clear()

        relevant_adapters = [
            adapter
            for adapter in self._all_adapters
            if pair.split("/") in adapter.venue_name.upper() or "USD" in pair
        ]
        if not relevant_adapters:
            relevant_adapters = [
                a
                for a in self._all_adapters
                if a.venue_name in ("coinbase", "kraken", "binance")
            ]

        for adapter in relevant_adapters:
            adapter.symbols = [pair]
        self._active_adapters = relevant_adapters

        self._chart_view.set_symbol(pair, DEFAULT_TIMEFRAME_SECONDS)
        await self._fetch_and_load_historical(pair, self._active_adapters)

        for adapter in self._active_adapters:
            adapter.start()
        await self._subscribe_to_realtime_data(pair, DEFAULT_TIMEFRAME)

    async def _fetch_and_load_historical(
        self, symbol: str, adapters: Sequence[ExchangeAdapter]
    ) -> None:
        """Fetches historical data from the first available adapter."""
        if not adapters:
            QMessageBox.warning(self, "No Adapters", f"No adapters found for {symbol}.")
            return

        adapter = adapters[0]
        self.statusBar().showMessage(
            f"Fetching historical data for {symbol} from {adapter.venue_name}..."
        )
        try:
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(days=HISTORICAL_DATA_RANGE_DAYS)
            candles = await adapter.get_historical_candles(
                symbol, DEFAULT_TIMEFRAME, start_dt, end_dt
            )
            self._chart_view.set_historical_data(candles)
        except Exception as e:
            logger.error(f"Failed to fetch historical data: {e}")
            QMessageBox.critical(self, "Error", f"Could not load historical data: {e}")

    async def _subscribe_to_realtime_data(self, symbol: str, timeframe: str) -> None:
        """Subscribes the UI to the publisher for real-time updates."""
        ui_queue: asyncio.Queue[models_pb2.PriceUpdate] = asyncio.Queue(maxsize=100)
        self._active_subscription_id = await self._publisher.subscribe(
            symbol, timeframe, ui_queue
        )
        self._ui_update_task = asyncio.create_task(self._ui_update_loop(ui_queue))

    async def _ui_update_loop(
        self, queue: "asyncio.Queue[models_pb2.PriceUpdate]"
    ) -> None:
        """The loop that feeds real-time data from a queue to the chart."""
        try:
            while True:
                update = await queue.get()
                self._chart_view.update_realtime_data(update)
                queue.task_done()
        except asyncio.CancelledError:
            logger.info("UI update loop cancelled.")

    @Slot()
    def _show_settings(self) -> None:
        """Shows the settings panel dialog."""
        if self._settings_panel is None:
            self._settings_panel = SettingsPanel(self._persistence, settings, self)
        self._settings_panel.show()
        self._settings_panel.raise_()
        self._settings_panel.activateWindow()

    async def _shutdown(self) -> None:
        """Gracefully shuts down all application components."""
        logger.info("Initiating graceful shutdown...")
        self.statusBar().showMessage("Shutting down...")
        for adapter in self._active_adapters:
            await adapter.stop()
        await self._aggregator.stop()
        await self._normalizer.stop()
        await self._persistence.stop()
        await self._publisher.stop()
        await self._http_client.aclose()
        logger.success("Shutdown complete.")

    def closeEvent(self, event: QCloseEvent) -> None:  # noqa: N802
        """Overrides QMainWindow.closeEvent to trigger async shutdown."""
        logger.info("Close event triggered.")
        event.accept()
        asyncio.create_task(self._shutdown()).add_done_callback(
            lambda _: QApplication.instance().quit()
        )


async def main_async() -> int:
    """The main async entry point for the application."""
    log_dir = (
        Path(settings.general.log_directory)
        if settings.general.log_directory
        else None
    )
    setup_logging(
        console_level=settings.general.log_level_console,
        file_level=settings.general.log_level_file,
        log_dir=log_dir,
    )

    main_window = MainWindow()
    main_window.show()
    await main_window.start_services()
    return 0


def main() -> None:
    """The synchronous entry point for the application."""
    try:
        exit_code = run_with_asyncio(main_async())
        sys.exit(exit_code)
    except Exception:
        logger.exception("An unhandled exception reached the top-level entry point.")
        sys.exit(1)


if __name__ == "__main__":
    main()