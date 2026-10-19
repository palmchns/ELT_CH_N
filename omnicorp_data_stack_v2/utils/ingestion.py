"""Bounded and validated source database downloads."""
import hashlib
import os
import tempfile
import requests

def download_source(source, url):
    maximum = int(os.getenv("MAX_SOURCE_BYTES", str(256 * 1024 * 1024)))
    if maximum <= 0:
        raise ValueError("MAX_SOURCE_BYTES must be positive")
    digest = hashlib.sha256()
    size = 0
    tmp = tempfile.NamedTemporaryFile(suffix=f"_{source}.db", delete=False)
    try:
        with requests.get(url, timeout=(10, 60), stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                size += len(chunk)
                if size > maximum:
                    raise ValueError(f"Source {source} exceeds MAX_SOURCE_BYTES")
                digest.update(chunk)
                tmp.write(chunk)
        expected = os.getenv(f"{source.upper()}_SHA256")
        if expected and digest.hexdigest() != expected.lower():
            raise ValueError(f"Source {source} checksum mismatch")
        tmp.flush()
        with open(tmp.name, "rb") as downloaded:
            if downloaded.read(16) != b"SQLite format 3\x00":
                raise ValueError(f"Source {source} is not a SQLite database")
        return tmp.name
    except BaseException:
        os.unlink(tmp.name)
        raise
    finally:
        tmp.close()
