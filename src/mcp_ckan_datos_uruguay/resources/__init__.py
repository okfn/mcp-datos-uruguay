"""Top-level entry point + shared helpers for the plugin's MCP resources.

Sub-areas (`ben/`, future `delitos/`, `compras/`) register their own
resources via a `register_<area>_resources(mcp)` function. Wire each new
area into `register_mcp_resources()` below.

Anything that has to be shared across sub-areas (the on-disk cache, the
lazy URL fetcher) lives here.
"""
import hashlib
import urllib.request
from pathlib import Path


# On-disk cache for large resources fetched from external URLs. PDFs etc.
# are not bundled in the wheel (would bloat installs); they live as GitHub
# Release assets and are downloaded lazily on first read. Keyed by URL hash
# so bumping the release tag automatically forces a re-download.
_CACHE_DIR = Path.home() / ".cache" / "mcp_ckan_datos_uruguay"


def fetch_cached_url(url: str) -> bytes:
    """Download ``url`` once, then serve subsequent reads from disk cache.

    The cache file name is a hash of the URL, so changing the URL (e.g.
    bumping the GitHub Release tag) transparently triggers a fresh download.
    Delete ``~/.cache/mcp_ckan_datos_uruguay/`` to force a refresh.
    """
    cache_key = hashlib.sha256(url.encode()).hexdigest()[:16]
    cache_path = _CACHE_DIR / cache_key
    if cache_path.exists():
        return cache_path.read_bytes()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "mcp-ckan-datos-uruguay"})
    with urllib.request.urlopen(req) as resp:  # noqa: S310 - fixed https URLs we control
        data = resp.read()
    cache_path.write_bytes(data)
    return data


# Sub-area imports go last so `fetch_cached_url` is already bound on this
# module when ben/__init__.py imports it back.
from mcp_ckan_datos_uruguay.resources.ben import register_ben_resources  # noqa: E402


def register_mcp_resources(mcp):
    register_ben_resources(mcp)
