import time
from collections import deque
from datetime import datetime, timezone

import numpy as np
import pyqtgraph as pg
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QGraphicsView, QGridLayout, QWidget

from cryptochart.types import models_pb2

# Set pyqtgraph configuration options for better appearance and performance
pg.setConfigOptions(antialias=True, useOpenGL=False)
pg.setConfigOption("background", "#161A25")
pg.setConfigOption("foreground", "#D8D9DD")


class DateAxis(pg.AxisItem):
    """A custom axis item to display dates and times."""

    def tickStrings(  # noqa: N802
        self, values: list[float], _scale: float, _spacing: float
    ) -> list[str]:
        """Formats the tick values from Unix timestamps to readable strings."""
        # Determine format based on the time range visible
        if not values:
            return []

        range_sec = values[-1] - values[0]
        if range_sec < 60 * 60 * 24 * 2:  # Less than 2 days
            string_format = "%H:%M:%S"
        elif range_sec < 60 * 60 * 24 * 30:  # Less than 30 days
            string_format = "%b %d"
        else:
            string_format = "%Y-%m-%d"

        return [
            datetime.fromtimestamp(v, tz=timezone.utc).strftime(string_format)
            for v in values
        ]


class ChartView(QWidget):
    """A high-performance charting widget for financial data.

    Uses PyQtGraph for displaying candlesticks, volume, and VWAP overlays.
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._symbol: str | None = None
        self._timeframe_seconds: int | None = None

        # Data storage using NumPy arrays for performance
        self._candle_data: np.ndarray | None = None
        self._volume_data: np.ndarray | None = None
        self._vwap_data: deque[tuple[float, float]] = deque(maxlen=5000)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Initializes the plot widgets and layout."""
        layout = QGridLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Create the main plot area for price and candlesticks
        self._price_plot = pg.PlotWidget(
            axisItems={"bottom": DateAxis(orientation="bottom")}
        )
        self._price_plot.setLabel("left", "Price")
        self._price_plot.showGrid(x=True, y=True, alpha=0.3)
        self._price_plot.setLogMode(x=False, y=False)
        self._price_plot.getPlotItem().setDownsampling(mode="peak")
        self._price_plot.getPlotItem().setClipToView(True)

        # Create the volume plot area
        self._volume_plot = pg.PlotWidget(
            axisItems={"bottom": DateAxis(orientation="bottom")}
        )
        self._volume_plot.setMaximumHeight(150)
        self._volume_plot.setLabel("left", "Volume")
        self._volume_plot.showGrid(x=True, y=True, alpha=0.3)

        # Link the X-axes of the two plots
        self._volume_plot.setXLink(self._price_plot)

        # Add plot items to the widgets
        self._candlestick_item = pg.CandlestickItem()
        self._price_plot.addItem(self._candlestick_item)

        self._vwap_item = pg.PlotDataItem(pen=pg.mkPen("#FFD700", width=2))
        self._price_plot.addItem(self._vwap_item)

        self._live_price_line = pg.InfiniteLine(
            angle=0, movable=False, pen=pg.mkPen("#FFA500", style=Qt.PenStyle.DashLine)
        )
        self._price_plot.addItem(self._live_price_line)

        self._volume_item = pg.BarGraphItem(x=[], height=[], width=0.8, brush="#4A5568")
        self._volume_plot.addItem(self._volume_item)

        # Add widgets to layout
        layout.addWidget(self._price_plot, 0, 0)
        layout.addWidget(self._volume_plot, 1, 0)

        # Improve zooming/panning behavior
        self._price_plot.setMouseEnabled(x=True, y=True)
        self._price_plot.setViewportUpdateMode(
            QGraphicsView.ViewportUpdateMode.FullViewportUpdate
        )

    def set_symbol(self, symbol: str, timeframe_seconds: int) -> None:
        """Sets the current symbol and timeframe, updating titles."""
        self.clear_chart()
        self._symbol = symbol
        self._timeframe_seconds = timeframe_seconds
        self._price_plot.setTitle(f"{symbol} - {timeframe_seconds // 60}m")

    def clear_chart(self) -> None:
        """Clears all data from the chart."""
        self._candle_data = None
        self._volume_data = None
        self._vwap_data.clear()

        self._candlestick_item.setData([])
        self._volume_item.setOpts(x=[], height=[])
        self._vwap_item.setData([])
        self._live_price_line.hide()

    def set_historical_data(self, candles: list[models_pb2.Candle]) -> None:
        """Populates the chart with a batch of historical candle data."""
        if not candles:
            self.clear_chart()
            return

        # Convert protobuf messages to NumPy arrays
        # Candle format: (time, open, high, low, close)
        # Volume format: (time, volume)
        num_candles = len(candles)
        self._candle_data = np.zeros((num_candles, 5))
        self._volume_data = np.zeros((num_candles, 2))

        for i, c in enumerate(candles):
            ts = datetime.fromisoformat(
                c.open_time.replace("Z", "+00:00")
            ).timestamp()
            self._candle_data[i] = (
                ts,
                float(c.open),
                float(c.high),
                float(c.low),
                float(c.close),
            )
            self._volume_data[i] = (ts, float(c.volume))

        self._candlestick_item.setData(self._candle_data)
        bar_width = (self._candle_data[1, 0] - self._candle_data[0, 0]) * 0.8
        self._volume_item.setOpts(
            x=self._volume_data[:, 0],
            height=self._volume_data[:, 1],
            width=bar_width,
        )
        self._price_plot.autoRange()
        self._volume_plot.autoRange()

    def update_realtime_data(self, update: models_pb2.PriceUpdate) -> None:
        """Updates the chart with a new real-time trade update."""
        if (
            self._candle_data is None
            or self._volume_data is None
            or self._timeframe_seconds is None
        ):
            return

        try:
            price = float(update.price)
            size = float(update.size)
            vwap = float(update.vwap)
            ts = time.time()
        except (ValueError, TypeError):
            return  # Ignore updates with invalid data

        # Update live price line
        self._live_price_line.setValue(price)
        if not self._live_price_line.isVisible():
            self._live_price_line.show()

        # Update VWAP line
        self._vwap_data.append((ts, vwap))
        vwap_np = np.array(list(self._vwap_data))
        self._vwap_item.setData(x=vwap_np[:, 0], y=vwap_np[:, 1])

        # Check if a new candle needs to be created
        last_candle_ts = self._candle_data[-1, 0]
        if ts >= last_candle_ts + self._timeframe_seconds:
            # Create a new candle
            new_candle = np.array([[ts, price, price, price, price]])
            new_volume = np.array([[ts, size]])
            self._candle_data = np.vstack([self._candle_data, new_candle])
            self._volume_data = np.vstack([self._volume_data, new_volume])
        else:
            # Update the current (last) candle
            self._candle_data[-1, 2] = max(self._candle_data[-1, 2], price)  # High
            self._candle_data[-1, 3] = min(self._candle_data[-1, 3], price)  # Low
            self._candle_data[-1, 4] = price  # Close
            self._volume_data[-1, 1] += size  # Add to volume

        # Redraw the updated items
        self._candlestick_item.setData(self._candle_data)
        self._volume_item.setOpts(
            x=self._volume_data[:, 0], height=self._volume_data[:, 1]
        )