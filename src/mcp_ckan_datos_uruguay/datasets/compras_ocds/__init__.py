"""Resolución del path de la base de datos SQLite de compras OCDS.

La base pesa ~1GB y se genera con `load_db.py` a partir de los zips anuales.
No se empaqueta: vive en un directorio de datos del usuario para
que el mismo path sea válido en desarrollo y tras `pip install`.

Orden de resolución (solo stdlib, sin dependencias externas):
1. Variable de entorno `MCP_CKAN_DATOS_URUGUAY_DB_PATH` (ruta completa al .db).
2. `$XDG_DATA_HOME/mcp-ckan-datos-uruguay/compras_ocds.db` si está definida.
3. `~/.local/share/mcp-ckan-datos-uruguay/compras_ocds.db` (default XDG).

En Windows, exportar `MCP_CKAN_DATOS_URUGUAY_DB_PATH` a una ruta bajo
`%LOCALAPPDATA%` si se desea seguir la convención del sistema.
"""

import os
from pathlib import Path

_ENV_VAR = "MCP_CKAN_DATOS_URUGUAY_DB_PATH"
_APP_DIR = "mcp-ckan-datos-uruguay"
_DB_FILENAME = "compras_ocds.db"


def get_db_path() -> Path:
    override = os.environ.get(_ENV_VAR)
    if override:
        return Path(override).expanduser()

    xdg = os.environ.get("XDG_DATA_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".local" / "share"
    return base / _APP_DIR / _DB_FILENAME


DB_PATH = get_db_path()
