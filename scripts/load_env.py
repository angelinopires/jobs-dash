#!/usr/bin/env python3
"""
Environment Variable Loader

Quick utility to load environment variables from .env.local for testing.
This helps set rate limiting profiles without needing to restart your shell.
"""

import os
from pathlib import Path


def load_env_file(filepath: str = ".env.local") -> None:
    """
    Load environment variables from a file.

    Args:
        filepath: Path to the environment file
    """
    env_path = Path(filepath)

    if not env_path.exists():
        print(f"⚠️  Environment file not found: {filepath}")
        return

    print(f"📂 Loading environment variables from {filepath}")

    with open(env_path, "r") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            # Skip export statements
            if line.startswith("export "):
                line = line[7:]  # Remove 'export '

            # Parse KEY=VALUE
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")  # Remove quotes

                os.environ[key] = value
                print(f"✅ Set {key}={value}")
            else:
                print(f"⚠️  Skipping invalid line {line_num}: {line}")

    print("🎉 Environment variables loaded successfully!")


if __name__ == "__main__":
    load_env_file()
