"""Load file-based credentials without putting secrets in Compose configuration."""
import os
from pathlib import Path
import sys


def environment():
    env = os.environ.copy()
    secret = Path(env.get("PREFECT_AUTH_FILE", "/run/secrets/prefect_auth"))
    if env.get("REQUIRE_API_AUTH") == "true" or secret.exists():
        value = secret.read_text().strip()
        if ":" not in value or not all(value.split(":", 1)) or "\n" in value or "\r" in value:
            raise ValueError("Prefect auth secret must contain username:password")
        env["PREFECT_SERVER_API_AUTH_STRING"] = value
        env["PREFECT_API_AUTH_STRING"] = value
        env.pop("PREFECT_API_KEY", None)
    return env


if __name__ == "__main__":
    env = environment()
    os.execvpe(sys.argv[1], sys.argv[1:], env)
