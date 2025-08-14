import asyncio
from collections.abc import Sequence

from loguru import logger
from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QLabel,
    QLineEdit,
    QListWidget,
    QMessageBox,
    QVBoxLayout,
    QWidget,
)

# This is a forward reference; the actual adapters will implement this.
from cryptochart.adapters.base import ExchangeAdapter


class PairSelectorDialog(QDialog):
    """A dialog that fetches and displays tradable pairs from multiple exchanges.

    This allows the user to search and select a pair.
    """

    def __init__(
        self, adapters: Sequence[ExchangeAdapter], parent: QWidget | None = None
    ) -> None:
        """Initializes the PairSelectorDialog.

        Args:
            adapters: A sequence of initialized exchange adapter instances.
            parent: The parent widget.
        """
        super().__init__(parent)
        self._adapters = adapters
        self._all_pairs: list[str] = []
        self._selected_pair: str | None = None

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Creates and arranges the widgets for the dialog."""
        self.setWindowTitle("Select Trading Pair")
        self.setMinimumSize(400, 500)

        layout = QVBoxLayout(self)

        # Search input
        search_label = QLabel("Search for a trading pair (e.g., BTC/USD):")
        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Filter pairs...")
        self._search_input.textChanged.connect(self._filter_list)

        # List of pairs
        self._pair_list_widget = QListWidget()
        self._pair_list_widget.setSortingEnabled(False)
        self._pair_list_widget.itemDoubleClicked.connect(self.accept)

        # Status label for loading feedback
        self._status_label = QLabel("Fetching pairs from exchanges...")
        self._status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)

        # Add widgets to layout
        layout.addWidget(search_label)
        layout.addWidget(self._search_input)
        layout.addWidget(self._status_label)
        layout.addWidget(self._pair_list_widget, stretch=1)
        layout.addWidget(button_box)

        # Initially hide the list and show status
        self._pair_list_widget.hide()

    async def populate_pairs(self) -> None:
        """Asynchronously fetches pairs from all adapters and populates the list."""
        logger.info("Starting to fetch tradable pairs from all adapters.")
        tasks = []
        for adapter in self._adapters:
            # Each adapter needs to implement get_all_tradable_pairs
            # We assume this method exists for now.
            if hasattr(adapter, "get_all_tradable_pairs"):
                tasks.append(adapter.get_all_tradable_pairs())
            else:
                logger.warning(
                    f"Adapter '{adapter.venue_name}' does not have "
                    "'get_all_tradable_pairs' method."
                )

        # Run all fetch tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        unique_pairs = set()
        for i, result in enumerate(results):
            adapter_name = self._adapters[i].venue_name
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch pairs from '{adapter_name}': {result}")
            elif isinstance(result, list):
                logger.info(f"Fetched {len(result)} pairs from '{adapter_name}'.")
                unique_pairs.update(result)
            else:
                logger.warning(
                    f"Unexpected result type from '{adapter_name}': {type(result)}"
                )

        self._all_pairs = sorted(unique_pairs)
        logger.success(f"Found a total of {len(self._all_pairs)} unique pairs.")

        # Update UI
        self._status_label.hide()
        self._pair_list_widget.show()

        if not self._all_pairs:
            QMessageBox.warning(
                self,
                "No Pairs Found",
                "Could not fetch any tradable pairs from the configured exchanges.",
            )
            self.reject()
            return

        self._filter_list("")  # Populate with all pairs initially

    @Slot(str)
    def _filter_list(self, text: str) -> None:
        """Filters the list widget based on the search input text."""
        self._pair_list_widget.clear()
        search_term = text.strip().upper()
        if not search_term:
            # If search is empty, add all pairs
            self._pair_list_widget.addItems(self._all_pairs)
        else:
            # Otherwise, add only matching pairs
            filtered_pairs = [
                pair for pair in self._all_pairs if search_term in pair.upper()
            ]
            self._pair_list_widget.addItems(filtered_pairs)

    def accept(self) -> None:
        """Overrides QDialog.accept to store the selected pair before closing."""
        selected_items = self._pair_list_widget.selectedItems()
        if not selected_items:
            QMessageBox.warning(
                self, "No Selection", "Please select a pair to continue."
            )
            return

        self._selected_pair = selected_items[0].text()
        logger.info(f"User selected pair: {self._selected_pair}")
        super().accept()

    def selected_pair(self) -> str | None:
        """Returns the pair that was selected by the user.

        Returns:
            The selected pair as a string (e.g., "BTC/USD"), or None if
            the dialog was cancelled or no pair was selected.
        """
        return self._selected_pair

    @staticmethod
    async def get_pair(
        adapters: Sequence[ExchangeAdapter], parent: QWidget | None = None
    ) -> str | None:
        """A static method to create, populate, and show the dialog.

        This encapsulates the entire process of getting a pair selection from the user.

        Args:
            adapters: A sequence of exchange adapters.
            parent: The parent widget for the dialog.

        Returns:
            The selected pair string, or None if cancelled.
        """
        dialog = PairSelectorDialog(adapters, parent)
        await dialog.populate_pairs()

        # The dialog is shown modally. The event loop continues to run.
        if dialog.exec() == QDialog.DialogCode.Accepted:
            return dialog.selected_pair()
        return None