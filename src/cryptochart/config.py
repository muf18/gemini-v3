import sys
import tomllib
from dataclasses import dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, ClassVar, TypeVar

import keyring
from loguru import logger

# --- Constants ---
APP_NAME = "cryptochart"
# Use a platform-agnostic user config directory
if sys.platform == "win32":
    CONFIG_DIR = Path.home() / "AppData" / "Roaming" / APP_NAME
else:
    CONFIG_DIR = Path.home() / ".config" / APP_NAME

CONFIG_FILE = CONFIG_DIR / "config.toml"

# --- Keyring Service Name ---
KEYRING_SERVICE_NAME = f"{APP_NAME.lower()}-api-keys"

# --- Dataclass Models for Settings ---
T = TypeVar("T")


@dataclass
class GeneralSettings:
    """General application settings."""

    log_level_console: str = "INFO"
    log_level_file: str = "DEBUG"
    log_directory: str = str(CONFIG_DIR / "logs")


@dataclass
class APISettings:
    """Settings for exchange APIs."""

    # Note: Actual keys are stored in the system keyring, not here.
    coinbase_enabled: bool = True
    bitstamp_enabled: bool = True
    kraken_enabled: bool = True
    bitvavo_enabled: bool = True
    binance_enabled: bool = True
    okx_enabled: bool = True
    bitget_enabled: bool = True
    digifinex_enabled: bool = True


@dataclass
class PersistenceSettings:
    """Settings for CSV data persistence."""

    enabled_by_default: bool = False
    output_directory: str = str(CONFIG_DIR / "csv_data")


@dataclass
class Settings:
    """Root container for all application settings."""

    general: GeneralSettings = field(default_factory=GeneralSettings)
    api: APISettings = field(default_factory=APISettings)
    persistence: PersistenceSettings = field(default_factory=PersistenceSettings)

    _instance: ClassVar["Settings | None"] = None

    @classmethod
    def get_instance(cls) -> "Settings":
        """Returns the singleton instance of the Settings object."""
        if cls._instance is None:
            cls._instance = load_config()
        return cls._instance


def _merge_dicts(base: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    """Recursively merges two dictionaries."""
    for key, value in new.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            base[key] = _merge_dicts(base[key], value)
        else:
            base[key] = value
    return base


def _update_dataclass(dc_instance: T, data: dict[str, Any]) -> T:
    """Recursively updates a dataclass instance from a dictionary."""
    for f in field_names(dc_instance):
        if f in data:
            field_value = getattr(dc_instance, f)
            if is_dataclass(field_value):
                _update_dataclass(field_value, data[f])
            else:
                setattr(dc_instance, f, data[f])
    return dc_instance


def field_names(dc_instance: Any) -> list[str]:
    """Helper to get field names from a dataclass instance."""
    return [f.name for f in dc_instance.__dataclass_fields__.values()]


def load_config(path: Path = CONFIG_FILE) -> Settings:
    """Loads settings from a TOML file, merging them with defaults.

    If the config file does not exist, it creates one with default values.

    Args:
        path: The path to the configuration file.

    Returns:
        A populated Settings object.
    """
    settings_obj = Settings()
    logger.info(f"Loading configuration from '{path}'...")

    if not path.exists():
        logger.warning(f"Configuration file not found. Creating default at '{path}'.")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # This is a placeholder for creating a default config file.
            # In a real app, you'd write the default settings to a TOML file here.
            with path.open("w", encoding="utf-8") as f:
                f.write("# CryptoChart Configuration File\n")
                f.write("# Add your settings overrides here.\n")
        except OSError as e:
            logger.error(f"Failed to create default config file: {e}")
        return settings_obj

    try:
        with path.open("rb") as f:
            user_config = tomllib.load(f)
        _update_dataclass(settings_obj, user_config)
        logger.success("Successfully loaded user configuration.")
    except tomllib.TOMLDecodeError as e:
        logger.error(f"Error decoding TOML from '{path}': {e}")
        logger.warning("Using default settings due to configuration error.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading config: {e}")
        logger.warning("Using default settings due to configuration error.")

    return settings_obj


# --- Keyring Management ---


def get_api_credentials(exchange_name: str) -> tuple[str | None, str | None]:
    """Retrieves API key and secret for a given exchange from the system keyring.

    Args:
        exchange_name: The lower-case name of the exchange (e.g., 'coinbase').

    Returns:
        A tuple containing (api_key, api_secret). Returns (None, None) if not found.
    """
    exchange_name = exchange_name.lower()
    try:
        api_key = keyring.get_password(KEYRING_SERVICE_NAME, f"{exchange_name}_key")
        api_secret = keyring.get_password(
            KEYRING_SERVICE_NAME, f"{exchange_name}_secret"
        )
        if api_key or api_secret:
            logger.debug(f"Retrieved credentials for '{exchange_name}' from keyring.")
        return api_key, api_secret
    except Exception as e:
        logger.error(f"Could not retrieve credentials from keyring: {e}")
        return None, None


def set_api_credentials(exchange_name: str, api_key: str, api_secret: str) -> None:
    """Stores API key and secret for an exchange in the system keyring.

    Args:
        exchange_name: The lower-case name of the exchange.
        api_key: The API key to store.
        api_secret: The API secret to store.
    """
    exchange_name = exchange_name.lower()
    try:
        keyring.set_password(KEYRING_SERVICE_NAME, f"{exchange_name}_key", api_key)
        keyring.set_password(
            KEYRING_SERVICE_NAME, f"{exchange_name}_secret", api_secret
        )
        logger.info(
            f"Successfully stored credentials for '{exchange_name}' in keyring."
        )
    except Exception as e:
        logger.error(f"Could not store credentials in keyring: {e}")


# --- Global Singleton Instance ---
# Other modules can simply `from cryptochart.config import settings`
settings = Settings.get_instance()