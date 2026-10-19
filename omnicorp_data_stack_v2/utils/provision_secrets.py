"""Create local deployment secrets without overwriting existing credentials."""
from pathlib import Path
import base64
import secrets


def provision(directory=Path(".secrets")):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    for name in ("prefect_auth", "bi_postgres_admin", "bi_writer", "bi_reader", "metabase_app", "metabase_encryption", "metabase_session"):
        path = directory / name
        if path.exists():
            continue
        value = secrets.token_urlsafe(48)
        if name in {"metabase_encryption", "metabase_session"}:
            value = base64.b64encode(secrets.token_bytes(32)).decode("ascii")
        if name == "prefect_auth":
            value = "operator:" + value
        with path.open("x") as stream:
            stream.write(value + "\n")
        # The private parent directory protects host access; non-root containers read the file mount.
        path.chmod(0o444)


if __name__ == "__main__":
    provision()
