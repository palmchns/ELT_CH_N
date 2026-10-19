"""Probe the API using the same file-based credentials as the server."""
import base64
from pathlib import Path
import urllib.request

request = urllib.request.Request("http://127.0.0.1:4200/api/health")
secret = Path("/run/secrets/prefect_auth")
if secret.exists():
    token = base64.b64encode(secret.read_bytes().strip()).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
with urllib.request.urlopen(request, timeout=3) as response:
    if response.status != 200:
        raise SystemExit(1)
