"""
Script idempotente para cargar archivos JSON OCDS en una base de datos SQLite.

Uso:
    python src/mcp_ckan_datos_uruguay/datasets/compras_ocds/load_db.py

Acepta uno o más directorios con archivos JSON OCDS (a-*.json y l-*.json).
Por defecto carga ocds-2024 y ocds-2025/2025.
"""

import json
import logging
import sqlite3
import sys
from pathlib import Path

from mcp_ckan_datos_uruguay.datasets.compras_ocds import DB_PATH

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR.parent / "compras-ocds"

DEFAULT_DATA_DIRS = [
    DATA_DIR / "ocds-2022",
    DATA_DIR / "ocds-2023",
    DATA_DIR / "ocds-2024",
    DATA_DIR / "ocds-2025",
]

SCHEMA = """
-- Publicaciones (releases): cada evento dentro de un proceso de contratación pública.
-- Un proceso (ocid) puede tener múltiples releases (planificación, licitación, adjudicación, etc.).
-- El campo "tag" indica la etapa del proceso (e.g. "planning", "tender", "award").
-- buyer_id y buyer_name identifican al organismo comprador.
CREATE TABLE IF NOT EXISTS releases (
    ocid TEXT,
    release_id TEXT,
    date TEXT,
    tag TEXT,
    buyer_id TEXT,
    buyer_name TEXT,
    source_file TEXT
);

-- Partes involucradas en cada proceso de contratación.
-- Incluye compradores, proveedores y cualquier otro actor.
-- Cada fila es una combinación parte-rol (una parte puede tener múltiples roles).
CREATE TABLE IF NOT EXISTS parties (
    ocid TEXT,
    party_id TEXT,
    name TEXT,
    role TEXT
);

-- Adjudicaciones: decisiones de otorgar un contrato dentro de un proceso.
-- Contiene el monto adjudicado (value_amount) y la moneda (value_currency).
-- Un proceso puede tener múltiples adjudicaciones (e.g. por lote).
CREATE TABLE IF NOT EXISTS awards (
    ocid TEXT,
    award_id TEXT,
    date TEXT,
    status TEXT,
    value_amount REAL,
    value_currency TEXT
);

-- Proveedores ganadores de cada adjudicación.
-- Una adjudicación puede tener múltiples proveedores (consorcio/unión temporal).
CREATE TABLE IF NOT EXISTS award_suppliers (
    ocid TEXT,
    award_id TEXT,
    supplier_id TEXT,
    supplier_name TEXT
);

-- Ítems (bienes/servicios) incluidos en cada adjudicación.
-- Detalla qué se compró, en qué cantidad, a qué precio unitario y con qué clasificación.
CREATE TABLE IF NOT EXISTS award_items (
    ocid TEXT,
    award_id TEXT,
    item_id TEXT,
    description TEXT,
    quantity REAL,
    classification_id TEXT,
    classification_description TEXT,
    unit_name TEXT,
    unit_value_amount REAL,
    unit_value_currency TEXT
);

-- Licitaciones/llamados: la convocatoria para recibir ofertas.
-- procurement_method indica el tipo de compra (open, limited, direct, etc.).
-- start_date y end_date definen el período de la licitación.
CREATE TABLE IF NOT EXISTS tenders (
    ocid TEXT,
    tender_id TEXT,
    title TEXT,
    description TEXT,
    procurement_method TEXT,
    procurement_method_details TEXT,
    status TEXT,
    start_date TEXT,
    end_date TEXT
);

-- Ítems (bienes/servicios) solicitados en cada licitación.
-- Describe qué se pide comprar, la cantidad y su clasificación.
CREATE TABLE IF NOT EXISTS tender_items (
    ocid TEXT,
    tender_id TEXT,
    item_id TEXT,
    description TEXT,
    quantity REAL,
    classification_id TEXT,
    classification_description TEXT,
    unit_name TEXT
);

CREATE INDEX IF NOT EXISTS idx_releases_ocid ON releases(ocid);
CREATE INDEX IF NOT EXISTS idx_releases_buyer_name ON releases(buyer_name);
CREATE INDEX IF NOT EXISTS idx_parties_name ON parties(name);
CREATE INDEX IF NOT EXISTS idx_parties_role ON parties(role);
CREATE INDEX IF NOT EXISTS idx_awards_ocid ON awards(ocid);
CREATE INDEX IF NOT EXISTS idx_award_suppliers_ocid ON award_suppliers(ocid);
CREATE INDEX IF NOT EXISTS idx_award_suppliers_name ON award_suppliers(supplier_name);
CREATE INDEX IF NOT EXISTS idx_award_items_ocid ON award_items(ocid);
CREATE INDEX IF NOT EXISTS idx_award_items_desc ON award_items(classification_description);
CREATE INDEX IF NOT EXISTS idx_tenders_ocid ON tenders(ocid);
CREATE INDEX IF NOT EXISTS idx_tender_items_ocid ON tender_items(ocid);
CREATE INDEX IF NOT EXISTS idx_tender_items_desc ON tender_items(description);
"""


def drop_all_tables(conn):
    tables = [
        "releases", "parties", "awards", "award_suppliers",
        "award_items", "tenders", "tender_items",
    ]
    for t in tables:
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    conn.commit()


def create_schema(conn):
    conn.executescript(SCHEMA)
    conn.commit()


def load_json_file(conn, filepath):
    """Carga un archivo JSON OCDS en la base de datos."""
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    releases = data.get("releases", [])
    fname = filepath.name

    release_rows = []
    party_rows = []
    award_rows = []
    supplier_rows = []
    award_item_rows = []
    tender_rows = []
    tender_item_rows = []

    for r in releases:
        ocid = r.get("ocid", "")
        release_id = r.get("id", "")
        date = r.get("date", "")
        tags = r.get("tag", [])
        tag = tags[0] if tags else ""
        buyer = r.get("buyer", {})

        release_rows.append((
            ocid, release_id, date, tag,
            buyer.get("id", ""), buyer.get("name", ""),
            fname,
        ))

        for p in r.get("parties", []):
            for role in p.get("roles", []):
                party_rows.append((ocid, p.get("id", ""), p.get("name", ""), role))

        for a in r.get("awards", []):
            aid = a.get("id", "")
            value = a.get("value", {})
            award_rows.append((
                ocid, aid, a.get("date", ""), a.get("status", ""),
                value.get("amount"), value.get("currency"),
            ))
            for s in a.get("suppliers", []):
                supplier_rows.append((ocid, aid, s.get("id", ""), s.get("name", "")))
            for item in a.get("items", []):
                cls = item.get("classification", {})
                unit = item.get("unit", {})
                uval = unit.get("value", {})
                award_item_rows.append((
                    ocid, aid, str(item.get("id", "")),
                    cls.get("description", ""),
                    item.get("quantity"),
                    cls.get("id", ""), cls.get("description", ""),
                    unit.get("name", ""),
                    uval.get("amount"), uval.get("currency"),
                ))

        tender = r.get("tender")
        if tender:
            tid = tender.get("id", "")
            period = tender.get("tenderPeriod", {})
            tender_rows.append((
                ocid, tid,
                tender.get("title", ""), tender.get("description", ""),
                tender.get("procurementMethod", ""),
                tender.get("procurementMethodDetails", ""),
                tender.get("status", ""),
                period.get("startDate", ""), period.get("endDate", ""),
            ))
            for item in tender.get("items", []):
                cls = item.get("classification", {})
                unit = item.get("unit", {})
                tender_item_rows.append((
                    ocid, tid, str(item.get("id", "")),
                    item.get("description", ""),
                    item.get("quantity"),
                    cls.get("id", ""), cls.get("description", ""),
                    unit.get("name", ""),
                ))

    conn.executemany("INSERT INTO releases VALUES (?,?,?,?,?,?,?)", release_rows)
    conn.executemany("INSERT INTO parties VALUES (?,?,?,?)", party_rows)
    conn.executemany("INSERT INTO awards VALUES (?,?,?,?,?,?)", award_rows)
    conn.executemany("INSERT INTO award_suppliers VALUES (?,?,?,?)", supplier_rows)
    conn.executemany("INSERT INTO award_items VALUES (?,?,?,?,?,?,?,?,?,?)", award_item_rows)
    conn.executemany("INSERT INTO tenders VALUES (?,?,?,?,?,?,?,?,?)", tender_rows)
    conn.executemany("INSERT INTO tender_items VALUES (?,?,?,?,?,?,?,?)", tender_item_rows)


def load_directories(data_dirs=None, db_path=None):
    """Carga todos los JSON de los directorios dados en SQLite. Idempotente: recrea las tablas."""
    if data_dirs is None:
        data_dirs = DEFAULT_DATA_DIRS
    if db_path is None:
        db_path = DB_PATH

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    drop_all_tables(conn)
    create_schema(conn)

    total_files = 0
    for d in data_dirs:
        d = Path(d)
        if not d.is_dir():
            log.warning(f"Directorio no encontrado: {d}")
            continue
        json_files = sorted(d.glob("*.json"))
        for jf in json_files:
            log.info(f"Cargando {jf.name} ...")
            load_json_file(conn, jf)
            total_files += 1

    conn.commit()

    # Resumen
    cur = conn.cursor()
    counts = {}
    for table in ["releases", "parties", "awards", "award_suppliers", "award_items", "tenders", "tender_items"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        counts[table] = cur.fetchone()[0]

    conn.close()
    log.info(f"Carga completa: {total_files} archivos procesados")
    for t, c in counts.items():
        log.info(f"  {t}: {c:,} registros")
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    dirs = [Path(p) for p in sys.argv[1:]] if len(sys.argv) > 1 else None
    load_directories(dirs)
