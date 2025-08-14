#!/usr/bin/env python
#
# This script compiles Protocol Buffer (.proto) files into Python modules.
# It uses grpcio-tools to invoke the protoc compiler.
#
# The script is designed to be run from the root of the repository.
# It finds all .proto files in the 'proto/' directory and generates
# the corresponding Python files in 'src/cryptochart/types/'.

import subprocess
import sys
from pathlib import Path

# --- Configuration ---
# Define the project's root directory by finding the script's location.
# This makes the script runnable from any directory.
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
    ROOT_DIR = SCRIPT_DIR.parent
except NameError:
    # Fallback for interactive environments
    ROOT_DIR = Path.cwd()

PROTO_SOURCE_DIR = ROOT_DIR / "proto"
PYTHON_OUTPUT_DIR = ROOT_DIR / "src" / "cryptochart" / "types"


def find_proto_files() -> list[Path]:
    """Finds all .proto files in the source directory."""
    if not PROTO_SOURCE_DIR.is_dir():
        print(f"Error: Source directory not found at '{PROTO_SOURCE_DIR}'")
        return []
    return list(PROTO_SOURCE_DIR.glob("*.proto"))


def compile_proto_file(proto_file: Path) -> bool:
    """Compiles a single .proto file using protoc."""
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTO_SOURCE_DIR}",
        f"--python_out={ROOT_DIR / 'src'}",
        str(proto_file.name),  # Pass only the filename relative to proto_path
    ]

    print(f"  Compiling {proto_file.name}...")
    try:
        process = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if process.stdout:
            print(f"  [protoc stdout]: {process.stdout}")
        if process.stderr:
            print(f"  [protoc stderr]: {process.stderr}")
        return True
    except FileNotFoundError:
        print("Error: 'protoc' command not found.")
        print("Please ensure 'grpcio-tools' is installed in your environment:")
        print("  pip install grpcio-tools")
        return False
    except subprocess.CalledProcessError as e:
        print(f"Error compiling {proto_file.name}:")
        print(e.stderr)
        return False


def main() -> None:
    """Main function to orchestrate the compilation process."""
    print("--- Starting Protobuf Compilation ---")

    PYTHON_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    proto_files = find_proto_files()
    if not proto_files:
        print("No .proto files found to compile. Exiting.")
        sys.exit(0)

    print(f"Found {len(proto_files)} .proto file(s) in '{PROTO_SOURCE_DIR}'.")

    success_count = sum(1 for pf in proto_files if compile_proto_file(pf))

    init_py_path = PYTHON_OUTPUT_DIR / "__init__.py"
    if not init_py_path.exists():
        print(f"Creating package marker: '{init_py_path}'")
        init_py_path.touch()

    print("-" * 35)
    if success_count == len(proto_files):
        print(f"SUCCESS: Compiled all {success_count} protobuf definition(s).")
        sys.exit(0)
    else:
        print(f"FAILURE: Only {success_count} of {len(proto_files)} files succeeded.")
        sys.exit(1)


if __name__ == "__main__":
    main()