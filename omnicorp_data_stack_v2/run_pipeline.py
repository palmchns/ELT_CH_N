"""Stable command-line entry point; implementation lives in utils/runtime.py."""
from utils.runtime import *

if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        event("command_failed", error_type=type(error).__name__)
        sys.exit(1)
