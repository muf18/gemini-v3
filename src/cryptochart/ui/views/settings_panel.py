import asyncio
from typing import TYPE_CHECKING

from loguru import logger
from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QLabel,
    QVBoxLayout,
    QWidget,
)

# Use a forward reference for type hinting to avoid circular imports
if TYPE_CHECKING:
    from cryptochart.config import Settings
    from cryptochart.persistence import Persistence


class SettingsPanel(QDialog):
    """A dialog window for configuring the application's settings at runtime.

    This panel provides controls for features like enabling/disabling CSV
    persistence and viewing the status of API configurations.
    """

    def __init__(
        self,
        persistence_manager: "Persistence",
        app_settings: "Settings",
        parent: QWidget | None = None,
    ) -> None:
        """Initializes the SettingsPanel.

        Args:
            persistence_manager: The application's persistence engine instance.
            app_settings: The application's configuration object.
            parent: The parent widget.
        """
        super().__init__(parent)
        self._persistence = persistence_manager
        self._settings = app_settings
        self._csv_toggle_task: asyncio.Task[None] | None = None

        self.setWindowTitle("Application Settings")
        self.setMinimumWidth(450)
        self._setup_ui()

    def _setup_ui(self) -> None:
        """Creates and arranges the widgets for the settings panel."""
        main_layout = QVBoxLayout(self)

        # --- Persistence Settings ---
        persistence_group = self._create_persistence_group()
        main_layout.addWidget(persistence_group)

        # --- API Settings ---
        api_group = self._create_api_status_group()
        main_layout.addWidget(api_group)

        main_layout.addStretch()

        # --- Dialog Buttons ---
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        button_box.rejected.connect(self.reject)
        # Since we have a close button, we connect 'accepted' to 'reject'
        button_box.accepted.connect(self.reject)
        button_box.button(QDialogButtonBox.StandardButton.Close).setDefault(True)

        main_layout.addWidget(button_box)

    def _create_persistence_group(self) -> QGroupBox:
        """Creates the GroupBox for data persistence settings."""
        group = QGroupBox("Data Persistence")
        layout = QFormLayout()

        # CSV Toggle Checkbox
        self._csv_toggle_checkbox = QCheckBox("Enable real-time CSV logging")
        self._csv_toggle_checkbox.setToolTip(
            "When enabled, all aggregated trade data will be saved to CSV files."
        )
        # Set the initial state from the persistence manager
        self._csv_toggle_checkbox.setChecked(self._persistence.is_enabled)
        # Connect the stateChanged signal to its handler
        self._csv_toggle_checkbox.stateChanged.connect(self._on_csv_toggle_changed)

        # CSV Directory Info
        csv_dir_label = QLabel("Output Directory:")
        csv_dir_value = QLabel(f"<code>{self._persistence.output_directory}</code>")
        csv_dir_value.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse
        )

        layout.addRow(self._csv_toggle_checkbox)
        layout.addRow(csv_dir_label, csv_dir_value)
        group.setLayout(layout)
        return group

    def _create_api_status_group(self) -> QGroupBox:
        """Creates a read-only GroupBox showing the status of API configurations."""
        group = QGroupBox("API Status")
        layout = QFormLayout()
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)

        api_settings = self._settings.api
        for exchange_name, is_enabled in api_settings.__dict__.items():
            if not exchange_name.endswith("_enabled"):
                continue

            clean_name = exchange_name.replace("_enabled", "").capitalize()
            status_label = QLabel(
                "<font color='green'>Enabled</font>"
                if is_enabled
                else "<font color='red'>Disabled</font>"
            )
            layout.addRow(f"{clean_name}:", status_label)

        info_label = QLabel(
            "<small><i>API settings are managed in the configuration file. "
            "API keys are stored securely in the system keyring.</i></small>"
        )
        info_label.setWordWrap(True)
        layout.addRow(info_label)

        group.setLayout(layout)
        return group

    @Slot(int)
    def _on_csv_toggle_changed(self, state: int) -> None:
        """Slot that handles changes to the CSV toggle checkbox.

        This method runs the asynchronous `set_enabled` method of the
        persistence manager in a background task.

        Args:
            state: The new state of the checkbox (Qt.CheckState enum value).
        """
        is_enabled = state == Qt.CheckState.Checked.value
        logger.info(f"Settings panel toggling CSV persistence to: {is_enabled}")

        # Since the target method is async, we must not block the GUI thread.
        # We create a task to run the coroutine on the active asyncio event loop.
        self._csv_toggle_task = asyncio.create_task(
            self._persistence.set_enabled(is_enabled)
        )