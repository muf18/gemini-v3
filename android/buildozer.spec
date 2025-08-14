# buildozer.spec
#
# This file is the configuration for Buildozer, a tool that packages Python
# applications for Android and iOS. This spec is configured for Android.
#
# For a full list of options, see the Buildozer documentation:
# https://buildozer.readthedocs.io/en/latest/specifications.html

[app]
# --- Basic Information ---
# The name of your application.
title = CryptoChart

# The internal package name, used in Java package structure.
package.name = cryptochart

# The domain of your package, used for uniqueness.
package.domain = org.example.cryptochart

# The directory where your main.py and other source files are.
# Since we run buildozer from the repo root, '.' is correct.
source.dir = .

# The entry point of your application.
# Buildozer expects a main.py in the source.dir. Since our entry point is
# in src/cryptochart/ui/main.py, we will create a simple main.py at the
# root level to import and run it when packaging.
# For now, we assume a main.py exists at the root.
# A better approach is to use `p4a.source_dir` but this is simpler.

# File extensions to include in the package.
source.include_exts = py, toml, ini, yml, proto, png, jpg, svg, json

# --- Versioning ---
# The version of your application.
version = 1.0.0

# --- Requirements ---
# This is the most critical section. List all Python dependencies here.
# Buildozer's backend (python-for-android) will try to find or build a
# "recipe" for each of these. Pure Python packages usually work out of the box.
# Packages with C extensions (like numpy, PySide6) need specific recipes.
requirements =
    # Python and bootstrap
    python3,
    hostpython3==3.11,
    # Core Networking & Async
    websockets,
    httpx,
    brotli,
    h2,
    aiodns,
    async_timeout,
    # Data Processing & Serialization
    protobuf,
    numpy,
    # GUI & Charting - These require specific recipes in python-for-android
    pyside6,
    pyqtgraph,
    qt-asyncio,
    # Utilities
    aiofiles,
    keyring,
    loguru

# --- Android Specifics ---
# The application orientation. Can be 'portrait', 'landscape', or 'all'.
orientation = portrait

# The minimum Android API level your app supports.
# API 24 = Android 7.0 (Nougat) - a good baseline for modern apps.
android.minapi = 24

# The target Android API level. Should be a recent version.
# API 33 = Android 13.
android.api = 33

# Architectures to build for. arm64-v8a is standard for modern devices.
android.archs = arm64-v8a, armeabi-v7a

# Permissions your application needs.
# INTERNET is required for all network calls.
# ACCESS_NETWORK_STATE allows checking for network connectivity.
android.permissions = INTERNET, ACCESS_NETWORK_STATE

# The entry point for a PySide6 application. The pyside6 recipe for
# python-for-android should handle setting the correct activity.
# We leave this commented out to use the recipe's default.
# android.entrypoint = org.qtproject.qt.android.bindings.QtActivity

# Since our code is in a `src` directory, we need to tell python-for-android
# to add it to the Python path inside the APK.
p4a.source_dirs = src

# --- Graphics and Icons ---
# (Optional) Path to a presplash image.
# android.presplash.filename = %(source.dir)s/assets/presplash.png

# (Optional) Path to the application's icon.
# android.icon.filename = %(source.dir)s/assets/icons/icon.png

# --- Build Configuration ---
# Set to 1 to disable the warning when running buildozer as root.
# warn_on_root = 1

[buildozer]
# Log level (0 = error, 1 = warning, 2 = info/debug).
log_level = 2

# Directory to store build artifacts.
build_dir = ./.buildozer

# Directory to store the final APK/AAB files.
bin_dir = ./bin