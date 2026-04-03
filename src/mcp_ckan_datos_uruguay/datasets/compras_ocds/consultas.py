"""
Funciones de consulta sobre la base de datos OCDS de compras públicas de Uruguay.

Provee búsqueda por similaridad de texto para empresas y productos,
y consultas detalladas de licitaciones/adjudicaciones.
"""

import difflib
import json
import logging
import sqlite3
import unicodedata
import urllib.request
from pathlib import Path

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "compras-ocds"
DB_PATH = DATA_DIR / "compras_ocds.db"

_EMPRESAS_CACHE = None
_PRODUCTOS_CACHE = None


def normalize(text):
    """Minúsculas, sin acentos, sin espacios extra."""
    if not text:
        return ""
    text = text.lower().strip()
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _get_conn():
    return sqlite3.connect(DB_PATH)


def _load_empresas():
    """Carga la lista de empresas proveedoras únicas (nombre -> nombre normalizado)."""
    global _EMPRESAS_CACHE
    if _EMPRESAS_CACHE is not None:
        return _EMPRESAS_CACHE
    conn = _get_conn()
    cur = conn.execute(
        "SELECT DISTINCT supplier_name FROM award_suppliers "
        "WHERE supplier_name IS NOT NULL AND supplier_name != ''"
    )
    empresas = {}
    for (name,) in cur:
        key = normalize(name)
        if key:
            empresas[key] = name.strip()
    conn.close()
    _EMPRESAS_CACHE = empresas
    log.info(f"Cargadas {len(empresas)} empresas únicas")
    return empresas


def _load_productos():
    """Carga la lista de productos únicos de tender_items y award_items."""
    global _PRODUCTOS_CACHE
    if _PRODUCTOS_CACHE is not None:
        return _PRODUCTOS_CACHE
    conn = _get_conn()
    productos = {}

    for table, col in [("tender_items", "description"), ("award_items", "classification_description")]:
        cur = conn.execute(
            f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL AND {col} != ''"
        )
        for (desc,) in cur:
            key = normalize(desc)
            if key and len(key) > 2:
                productos[key] = desc.strip()

    conn.close()
    _PRODUCTOS_CACHE = productos
    log.info(f"Cargados {len(productos)} productos únicos")
    return productos


def buscar_empresa(nombre, limit=10):
    """Busca empresas proveedoras por nombre aproximado.

    Args:
        nombre: Nombre o parte del nombre de la empresa.
        limit: Máximo de resultados.

    Returns:
        str: Lista de empresas similares encontradas.
    """
    empresas = _load_empresas()
    key = normalize(nombre)

    # Primero intentar LIKE (funciona bien con palabras parciales como "copernico")
    conn = _get_conn()
    pattern = f"%{key}%"
    cur = conn.execute(
        "SELECT DISTINCT supplier_name FROM award_suppliers "
        "WHERE supplier_name IS NOT NULL AND LOWER(supplier_name) LIKE ? LIMIT ?",
        (pattern, limit)
    )
    like_results = [r[0].strip() for r in cur if r[0] and r[0].strip()]
    conn.close()

    if like_results:
        unique = like_results
    else:
        # Fallback a difflib para nombres con errores ortográficos
        matches = difflib.get_close_matches(key, empresas.keys(), n=limit, cutoff=0.5)
        if not matches:
            return f"No se encontraron empresas similares a '{nombre}'."
        unique = list(dict.fromkeys(empresas[m] for m in matches))

    table_rows = [["Empresa"]] + [[e] for e in unique]
    table = f"<table>{json.dumps(table_rows, ensure_ascii=False)}</table>"
    return f"Empresas similares a '{nombre}':\n" + "\n".join(f"  - {e}" for e in unique) + table


def buscar_producto(texto, limit=10):
    """Busca productos o rubros por descripción aproximada.

    Args:
        texto: Descripción del producto a buscar.
        limit: Máximo de resultados.

    Returns:
        str: Lista de productos similares encontrados.
    """
    conn = _get_conn()
    pattern = f"%{normalize(texto)}%"

    cur = conn.execute(
        "SELECT DISTINCT description FROM tender_items "
        "WHERE description IS NOT NULL AND description != '' "
        "AND LOWER(description) LIKE ? LIMIT ?",
        (pattern, limit * 3)
    )
    tender_matches = [r[0].strip() for r in cur if r[0] and r[0].strip()]

    cur = conn.execute(
        "SELECT DISTINCT classification_description FROM award_items "
        "WHERE classification_description IS NOT NULL AND classification_description != '' "
        "AND LOWER(classification_description) LIKE ? LIMIT ?",
        (pattern, limit * 3)
    )
    award_matches = [r[0].strip() for r in cur if r[0] and r[0].strip()]
    conn.close()

    all_matches = list(dict.fromkeys(tender_matches + award_matches))

    if not all_matches:
        productos = _load_productos()
        key = normalize(texto)
        fuzzy = difflib.get_close_matches(key, productos.keys(), n=limit, cutoff=0.45)
        if fuzzy:
            all_matches = [productos[m] for m in fuzzy]
        else:
            return f"No se encontraron productos similares a '{texto}'."

    all_matches = all_matches[:limit]
    table_rows = [["Producto/Rubro"]] + [[p] for p in all_matches]
    table = f"<table>{json.dumps(table_rows, ensure_ascii=False)}</table>"
    return f"Productos similares a '{texto}':\n" + "\n".join(f"  - {p}" for p in all_matches) + table


def licitaciones_empresa(nombre_empresa, year=None, comprador=None,
                         metodo=None, limit=20):
    """Busca licitaciones y adjudicaciones en las que participó una empresa.

    Args:
        nombre_empresa: Nombre exacto o aproximado de la empresa.
            Use buscar_empresa() primero si no conoce el nombre exacto.
        year: Año para filtrar (2024 o 2025). None para todos.
        comprador: Filtrar por nombre de organismo comprador (parcial). None para todos.
        metodo: Filtrar por método de contratación (parcial). None para todos.
        limit: Máximo de resultados.

    Returns:
        str: Detalle de licitaciones/adjudicaciones de la empresa.
    """
    nombre_real = _resolve_empresa(nombre_empresa)
    if not nombre_real:
        return (
            f"Empresa '{nombre_empresa}' no encontrada. "
            f"Use buscar_empresa('{nombre_empresa}') para encontrar el nombre correcto."
        )

    conn = _get_conn()

    extra_where = ""
    params = [nombre_real, nombre_real]
    if year:
        extra_where += f" AND r.date LIKE '{int(year)}%'"
    if comprador:
        extra_where += " AND LOWER(r.buyer_name) LIKE ?"
        params.append(f"%{normalize(comprador)}%")
    if metodo:
        extra_where += " AND LOWER(COALESCE(t.procurement_method_details, '')) LIKE ?"
        params.append(f"%{normalize(metodo)}%")

    query = f"""
        SELECT DISTINCT
            r.ocid,
            r.date,
            r.tag,
            r.buyer_name,
            COALESCE(t.title, '') as tender_title,
            COALESCE(t.procurement_method_details, '') as metodo,
            COALESCE(t.description, '') as tender_desc
        FROM releases r
        LEFT JOIN tenders t ON r.ocid = t.ocid
        WHERE r.ocid IN (
            SELECT DISTINCT ocid FROM award_suppliers WHERE supplier_name = ?
            UNION
            SELECT DISTINCT ocid FROM parties WHERE name = ? AND role = 'supplier'
        )
        {extra_where}
        ORDER BY r.date DESC
        LIMIT ?
    """
    params.append(limit)
    cur = conn.execute(query, params)
    rows = cur.fetchall()

    filtros = _filter_description(year, comprador, None, metodo)
    if not rows:
        conn.close()
        msg = f"No se encontraron licitaciones para '{nombre_real}'"
        return f"{msg} ({filtros})." if filtros else f"{msg}."

    header = f"Licitaciones/adjudicaciones de '{nombre_real}'"
    header += f" ({len(rows)} resultados):"
    lines = [header]
    if filtros:
        lines.append(f"Filtros: {filtros}")
    table_rows = [["Fecha", "Tipo", "Comprador", "Título/Descripción", "Método", "Link"]]

    for ocid, date, tag, buyer, title, metodo_val, desc in rows:
        fecha = date[:10] if date else "?"
        tipo = "Adjudicación" if tag == "award" else "Licitación" if tag == "tender" else tag
        titulo = title if title else (desc[:80] if desc else "")
        url = _ocid_to_url(ocid)
        lines.append(f"  - [{fecha}] {tipo} | {buyer} | {titulo} (OCID: {ocid})")
        table_rows.append([fecha, tipo, buyer or "", titulo[:80], metodo_val or "", url])

    conn.close()
    table = f"<table>{json.dumps(table_rows, ensure_ascii=False)}</table>"
    return "\n".join(lines) + table


def _resolve_empresa(nombre_empresa):
    """Resuelve un nombre de empresa al nombre real en la base de datos."""
    empresas = _load_empresas()
    key = normalize(nombre_empresa)
    nombre_real = empresas.get(key)
    if not nombre_real:
        matches = difflib.get_close_matches(key, empresas.keys(), n=1, cutoff=0.5)
        if matches:
            nombre_real = empresas[matches[0]]
    return nombre_real


def _build_filters(year=None, comprador=None, producto=None, metodo=None):
    """Construye cláusulas WHERE y params para filtros opcionales sobre awards."""
    clauses = []
    params = []
    if year:
        clauses.append(f"r.date LIKE '{int(year)}%'")
    if comprador:
        clauses.append("LOWER(r.buyer_name) LIKE ?")
        params.append(f"%{normalize(comprador)}%")
    if producto:
        clauses.append("LOWER(ai.classification_description) LIKE ?")
        params.append(f"%{normalize(producto)}%")
    if metodo:
        clauses.append(
            "r.ocid IN (SELECT ocid FROM tenders WHERE LOWER(procurement_method_details) LIKE ?)"
        )
        params.append(f"%{normalize(metodo)}%")
    return clauses, params


def _filter_description(year=None, comprador=None, producto=None, metodo=None):
    """Genera texto describiendo los filtros activos."""
    parts = []
    if year:
        parts.append(f"en {year}")
    if comprador:
        parts.append(f"comprador '{comprador}'")
    if producto:
        parts.append(f"producto '{producto}'")
    if metodo:
        parts.append(f"método '{metodo}'")
    return " | ".join(parts) if parts else ""


def _build_stacked_output(raw_rows, group_col_label, max_groups=8):
    """Construye datasets stacked y totales a partir de filas (mes, grupo, monto).

    Returns:
        (all_months, datasets, group_totals, monthly_totals)
    """
    all_months = sorted(set(r[0] for r in raw_rows))
    group_totals = {}
    monthly_totals = {}
    for mes, group, monto in raw_rows:
        group_totals[group] = group_totals.get(group, 0) + monto
        monthly_totals[mes] = monthly_totals.get(mes, 0) + monto

    sorted_groups = sorted(group_totals.items(), key=lambda x: x[1], reverse=True)
    chart_groups = [g for g, _ in sorted_groups[:max_groups]]
    has_otros = len(sorted_groups) > max_groups

    group_month = {}
    for mes, group, monto in raw_rows:
        key = group if group in chart_groups else "Otros"
        group_month.setdefault(key, {})
        group_month[key][mes] = group_month[key].get(mes, 0) + monto

    datasets = []
    for group in chart_groups + (["Otros"] if has_otros else []):
        data = [round(group_month.get(group, {}).get(mes, 0), 0) for mes in all_months]
        datasets.append({"label": group, "data": data})

    return all_months, datasets, sorted_groups, monthly_totals


def _query_by_currency(conn, base_query, base_params, where_extra, extra_params):
    """Ejecuta la query agrupada por moneda. Retorna dict {moneda: [(mes, grupo, monto)]}."""
    full_query = f"""
        SELECT
            COALESCE(ai.unit_value_currency, 'UYU') as moneda,
            {base_query}
        {where_extra}
        GROUP BY moneda, mes, grp
        ORDER BY moneda, mes
    """
    cur = conn.execute(full_query, base_params + extra_params)
    by_currency = {}
    for moneda, mes, grp, monto in cur.fetchall():
        by_currency.setdefault(moneda, []).append((mes, grp, monto))
    return by_currency


def resumen_empresa(nombre_empresa, year=None, comprador=None, producto=None,
                    metodo=None, max_compradores_chart=8):
    """Resumen de compras adjudicadas a una empresa, agrupadas por mes y comprador.

    Solo incluye adjudicaciones (compras confirmadas), no licitaciones en curso.
    Los montos se calculan a partir de precio unitario x cantidad de cada item.
    Se genera un gráfico por cada moneda con montos significativos.

    Args:
        nombre_empresa: Nombre exacto o aproximado de la empresa.
            Use buscar_empresa() primero si no conoce el nombre exacto.
        year: Año para filtrar (2024 o 2025). None para todos.
        comprador: Filtrar por nombre de organismo comprador (parcial). None para todos.
        producto: Filtrar por descripción de producto/rubro (parcial). None para todos.
        metodo: Filtrar por método de contratación (parcial). Valores comunes:
            "Compra Directa", "Concurso de Precios", "Licitación Abreviada",
            "Licitación Pública", "Compra por Excepción". None para todos.
        max_compradores_chart: Máximo de compradores individuales en el gráfico. Default 8.

    Returns:
        str: Resumen con tabla y gráficos de barras stacked por comprador y mes (uno por moneda).
    """
    nombre_real = _resolve_empresa(nombre_empresa)
    if not nombre_real:
        return (
            f"Empresa '{nombre_empresa}' no encontrada. "
            f"Use buscar_empresa('{nombre_empresa}') para encontrar el nombre correcto."
        )

    extra_clauses, extra_params = _build_filters(year, comprador, producto, metodo)
    where_extra = ("AND " + " AND ".join(extra_clauses)) if extra_clauses else ""

    conn = _get_conn()

    base_query = """
            SUBSTR(r.date, 1, 7) as mes,
            CASE WHEN r.buyer_name = '' OR r.buyer_name IS NULL
                 THEN 'Sin identificar' ELSE r.buyer_name END as grp,
            SUM(ai.unit_value_amount * ai.quantity) as monto
        FROM award_items ai
        JOIN award_suppliers s ON ai.ocid = s.ocid AND ai.award_id = s.award_id
        JOIN releases r ON ai.ocid = r.ocid AND r.tag = 'award'
        WHERE s.supplier_name = ?
        AND ai.unit_value_amount IS NOT NULL AND ai.unit_value_amount > 0
    """
    by_currency = _query_by_currency(
        conn, base_query, [nombre_real], where_extra, extra_params
    )

    # Top productos adjudicados
    cur = conn.execute(f"""
        SELECT
            ai.classification_description,
            COUNT(*) as veces
        FROM award_items ai
        JOIN award_suppliers s ON ai.ocid = s.ocid AND ai.award_id = s.award_id
        JOIN releases r ON ai.ocid = r.ocid
        WHERE s.supplier_name = ? AND ai.classification_description != ''
        {where_extra}
        GROUP BY ai.classification_description
        ORDER BY veces DESC
        LIMIT 10
    """, [nombre_real] + extra_params)
    top_products = cur.fetchall()
    conn.close()

    filtros = _filter_description(year, comprador, producto, metodo)
    if not by_currency:
        msg = f"No se encontraron adjudicaciones para '{nombre_real}'"
        return f"{msg} ({filtros})." if filtros else f"{msg}."

    # Calcular totales globales
    grand_total_parts = []
    all_buyers = set()
    first_month = None
    last_month = None
    for curr, rows in by_currency.items():
        total = sum(r[2] for r in rows)
        grand_total_parts.append(f"${total:,.0f} {curr}")
        all_buyers.update(r[1] for r in rows)
        months = sorted(set(r[0] for r in rows))
        if months:
            if first_month is None or months[0] < first_month:
                first_month = months[0]
            if last_month is None or months[-1] > last_month:
                last_month = months[-1]

    periodo = f" en {year}" if year else f" ({first_month} a {last_month})"
    header = f"Compras adjudicadas a '{nombre_real}'{periodo}: "
    header += " + ".join(grand_total_parts)
    header += f" de {len(all_buyers)} organismos compradores."
    lines = [header]
    if filtros:
        lines.append(f"Filtros: {filtros}")

    table_rows = []
    charts = ""

    for curr in sorted(by_currency.keys()):
        rows = by_currency[curr]
        all_months, datasets, sorted_buyers, monthly_totals = _build_stacked_output(
            rows, "comprador", max_compradores_chart
        )

        lines.append(f"\nMontos por mes ({curr}):")
        table_rows.append(["Mes", f"Monto {curr}"])
        for mes in all_months:
            lines.append(f"  - {mes}: ${monthly_totals[mes]:,.0f}")
            table_rows.append([mes, f"${monthly_totals[mes]:,.0f}"])

        lines.append(f"\nPrincipales compradores ({curr}):")
        table_rows.append(["", ""])
        table_rows.append(["Comprador", f"Monto {curr}"])
        for buyer, monto in sorted_buyers[:10]:
            lines.append(f"  - {buyer}: ${monto:,.0f}")
            table_rows.append([buyer or "", f"${monto:,.0f}"])
        table_rows.append(["", ""])

        chart_title = f"Compras a {nombre_real} por comprador ({curr})"
        chart_data = json.dumps({
            "type": "bar",
            "stacked": True,
            "title": chart_title,
            "labels": all_months,
            "datasets": datasets,
        }, ensure_ascii=False)
        charts += f"<chart>{chart_data}</chart>"

    if top_products:
        lines.append("\nPrincipales rubros adjudicados:")
        for prod, count in top_products:
            lines.append(f"  - {prod[:70]}: {count} items")

    table = f"<table>{json.dumps(table_rows, ensure_ascii=False)}</table>"

    return "\n".join(lines) + table + charts


def resumen_producto(producto, year=None, proveedor=None, comprador=None,
                     metodo=None, agrupar_por="proveedor", max_grupos_chart=8):
    """Resumen de compras de un producto agrupadas por mes y proveedor o comprador.

    Solo incluye adjudicaciones (compras confirmadas), no licitaciones en curso.
    Los montos se calculan a partir de precio unitario x cantidad de cada item.
    Se genera un gráfico por cada moneda con montos significativos.

    Args:
        producto: Descripción del producto (puede ser parcial).
            Use buscar_producto() primero si no conoce la descripción exacta.
        year: Año para filtrar (2024 o 2025). None para todos.
        proveedor: Filtrar por nombre de empresa proveedora (parcial). None para todos.
        comprador: Filtrar por nombre de organismo comprador (parcial). None para todos.
        metodo: Filtrar por método de contratación (parcial). Valores comunes:
            "Compra Directa", "Concurso de Precios", "Licitación Abreviada",
            "Licitación Pública", "Compra por Excepción". None para todos.
        agrupar_por: "proveedor" o "comprador". Define cómo se desglosan las barras
            del gráfico stacked. Default "proveedor".
        max_grupos_chart: Máximo de grupos individuales en el gráfico. Default 8.

    Returns:
        str: Resumen con tabla y gráficos de barras stacked por mes (uno por moneda).
    """
    por_comprador = agrupar_por == "comprador"
    grp_col = "r.buyer_name" if por_comprador else "s.supplier_name"
    grp_label = "comprador" if por_comprador else "proveedor"

    conn = _get_conn()
    pattern = f"%{normalize(producto)}%"

    extra_clauses, extra_params = _build_filters(year, comprador, None, metodo)
    if proveedor:
        extra_clauses.append("LOWER(s.supplier_name) LIKE ?")
        extra_params.append(f"%{normalize(proveedor)}%")
    where_extra = ("AND " + " AND ".join(extra_clauses)) if extra_clauses else ""

    base_query = f"""
            SUBSTR(r.date, 1, 7) as mes,
            CASE WHEN {grp_col} = '' OR {grp_col} IS NULL
                 THEN 'Sin identificar' ELSE {grp_col} END as grp,
            SUM(ai.unit_value_amount * ai.quantity) as monto
        FROM award_items ai
        JOIN award_suppliers s ON ai.ocid = s.ocid AND ai.award_id = s.award_id
        JOIN releases r ON ai.ocid = r.ocid AND r.tag = 'award'
        WHERE LOWER(ai.classification_description) LIKE ?
        AND ai.unit_value_amount IS NOT NULL AND ai.unit_value_amount > 0
    """
    by_currency = _query_by_currency(
        conn, base_query, [pattern], where_extra, extra_params
    )

    filtros = _filter_description(year, comprador, None, metodo)
    if proveedor:
        filtros = f"proveedor '{proveedor}'" + (f" | {filtros}" if filtros else "")

    if not by_currency:
        conn.close()
        msg = f"No se encontraron compras adjudicadas de '{producto}'"
        if filtros:
            msg += f" ({filtros})"
        return msg + f". Use buscar_producto('{producto}') para verificar el nombre."

    # Top del eje secundario (el que NO es el agrupador del gráfico)
    other_col = "s.supplier_name" if por_comprador else "r.buyer_name"
    other_label = "proveedor" if por_comprador else "comprador"
    cur = conn.execute(f"""
        SELECT
            {other_col},
            COALESCE(ai.unit_value_currency, 'UYU') as moneda,
            SUM(ai.unit_value_amount * ai.quantity) as monto
        FROM award_items ai
        JOIN award_suppliers s ON ai.ocid = s.ocid AND ai.award_id = s.award_id
        JOIN releases r ON ai.ocid = r.ocid AND r.tag = 'award'
        WHERE LOWER(ai.classification_description) LIKE ?
        AND ai.unit_value_amount IS NOT NULL AND ai.unit_value_amount > 0
        {where_extra}
        GROUP BY {other_col}, moneda
        ORDER BY monto DESC
        LIMIT 10
    """, [pattern] + extra_params)
    top_others = cur.fetchall()
    conn.close()

    # Header con totales por moneda
    grand_total_parts = []
    all_groups = set()
    first_month = None
    last_month = None
    for curr, rows in by_currency.items():
        total = sum(r[2] for r in rows)
        grand_total_parts.append(f"${total:,.0f} {curr}")
        all_groups.update(r[1] for r in rows)
        months = sorted(set(r[0] for r in rows))
        if months:
            if first_month is None or months[0] < first_month:
                first_month = months[0]
            if last_month is None or months[-1] > last_month:
                last_month = months[-1]

    periodo = f" en {year}" if year else f" ({first_month} a {last_month})"
    header = f"Compras adjudicadas de '{producto}'{periodo}: "
    header += " + ".join(grand_total_parts)
    header += f" de {len(all_groups)} {grp_label}es."
    lines = [header]
    if filtros:
        lines.append(f"Filtros: {filtros}")

    table_rows = []
    charts = ""

    for curr in sorted(by_currency.keys()):
        rows = by_currency[curr]
        all_months, datasets, sorted_groups, monthly_totals = _build_stacked_output(
            rows, grp_label, max_grupos_chart
        )

        lines.append(f"\nMontos por mes ({curr}):")
        table_rows.append(["Mes", f"Monto {curr}"])
        for mes in all_months:
            lines.append(f"  - {mes}: ${monthly_totals[mes]:,.0f}")
            table_rows.append([mes, f"${monthly_totals[mes]:,.0f}"])

        lines.append(f"\nPrincipales {grp_label}es ({curr}):")
        table_rows.append(["", ""])
        table_rows.append([grp_label.capitalize(), f"Monto {curr}"])
        for grp, monto in sorted_groups[:10]:
            lines.append(f"  - {grp}: ${monto:,.0f}")
            table_rows.append([grp or "", f"${monto:,.0f}"])
        table_rows.append(["", ""])

        chart_title = f"Compras de '{producto}' por {grp_label} ({curr})"
        chart_data = json.dumps({
            "type": "bar",
            "stacked": True,
            "title": chart_title,
            "labels": all_months,
            "datasets": datasets,
        }, ensure_ascii=False)
        charts += f"<chart>{chart_data}</chart>"

    if top_others:
        lines.append(f"\nPrincipales {other_label}es:")
        for name, curr, monto in top_others:
            lines.append(f"  - {name}: ${monto:,.0f} {curr}")

    table = f"<table>{json.dumps(table_rows, ensure_ascii=False)}</table>"

    return "\n".join(lines) + table + charts


def compras_producto(producto, year=None, limit=20):
    """Busca qué empresas proveen un producto o rubro al gobierno.

    Args:
        producto: Descripción del producto (puede ser parcial).
            Use buscar_producto() primero si no conoce la descripción exacta.
        year: Año para filtrar (2024 o 2025). None para todos.
        limit: Máximo de resultados.

    Returns:
        str: Lista de empresas que proveen el producto, con detalles.
    """
    conn = _get_conn()
    pattern = f"%{normalize(producto)}%"

    year_filter = ""
    if year:
        year_filter = f"AND r.date LIKE '{int(year)}%'"

    query = f"""
        SELECT
            s.supplier_name,
            r.buyer_name,
            ai.classification_description,
            r.date,
            r.ocid,
            ai.quantity,
            ai.unit_name
        FROM award_items ai
        JOIN awards a ON ai.ocid = a.ocid AND ai.award_id = a.award_id
        JOIN award_suppliers s ON ai.ocid = s.ocid AND ai.award_id = s.award_id
        JOIN releases r ON ai.ocid = r.ocid AND r.tag = 'award'
        WHERE LOWER(ai.classification_description) LIKE ?
        {year_filter}
        ORDER BY r.date DESC
        LIMIT ?
    """
    cur = conn.execute(query, (pattern, limit))
    rows = cur.fetchall()

    if not rows:
        cur2 = conn.execute(f"""
            SELECT
                r.buyer_name,
                ti.description,
                r.date,
                r.ocid,
                ti.quantity,
                ti.unit_name
            FROM tender_items ti
            JOIN tenders t ON ti.ocid = t.ocid AND ti.tender_id = t.tender_id
            JOIN releases r ON ti.ocid = r.ocid AND r.tag = 'tender'
            WHERE LOWER(ti.description) LIKE ?
            {year_filter}
            ORDER BY r.date DESC
            LIMIT ?
        """, (pattern, limit))
        tender_rows = cur2.fetchall()
        conn.close()

        if not tender_rows:
            return (
                f"No se encontraron compras de '{producto}'" + (f" en {year}" if year else "") + ". "
                f"Use buscar_producto('{producto}') para verificar el nombre del producto."
            )

        lines = [f"Licitaciones de '{producto}'" + (f" en {year}" if year else "") + f" ({len(tender_rows)} resultados):"]
        table_rows = [["Comprador", "Producto", "Fecha", "Cantidad", "Link"]]
        for buyer, desc, date, ocid, qty, unit in tender_rows:
            fecha = date[:10] if date else "?"
            cant = f"{qty} {unit}" if qty else ""
            url = _ocid_to_url(ocid)
            lines.append(f"  - [{fecha}] {buyer} | {desc[:60]} | {cant} (OCID: {ocid})")
            table_rows.append([buyer or "", desc[:60] if desc else "", fecha, cant, url])

        table = f"<table>{json.dumps(table_rows, ensure_ascii=False)}</table>"
        return "\n".join(lines) + table

    lines = [
        f"Empresas que proveen '{producto}'" + (f" en {year}" if year else "") + f" ({len(rows)} resultados):"
    ]
    table_rows = [["Proveedor", "Comprador", "Producto", "Fecha", "Cantidad", "Link"]]
    for supplier, buyer, desc, date, ocid, qty, unit in rows:
        fecha = date[:10] if date else "?"
        cant = f"{qty} {unit}" if qty else ""
        url = _ocid_to_url(ocid)
        lines.append(f"  - {supplier} -> {buyer} | {desc[:50]} | [{fecha}] {cant} (OCID: {ocid})")
        table_rows.append([
            supplier or "", buyer or "", desc[:50] if desc else "", fecha, cant, url
        ])

    conn.close()
    table = f"<table>{json.dumps(table_rows, ensure_ascii=False)}</table>"
    return "\n".join(lines) + table


COMPRAS_BASE_URL = "https://www.comprasestatales.gub.uy/consultas/detalle/id/"


def _check_url(url, timeout=5):
    """Verifica si una URL responde con HTTP 200."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status == 200
    except Exception:
        return False


def _ocid_to_url(ocid):
    """Convierte un OCID a la URL del portal de compras estatales."""
    numero = ocid.replace("ocds-yfs5dr-", "")
    return f"{COMPRAS_BASE_URL}{numero}"


def _build_award_detail(conn, ocid, lines):
    """Agrega detalle de adjudicaciones al listado de líneas. Retorna tabla o None."""
    cur = conn.execute(
        "SELECT award_id, date, status, value_amount, value_currency "
        "FROM awards WHERE ocid = ?",
        (ocid,)
    )
    awards = cur.fetchall()
    if not awards:
        return None

    lines.append(f"\nAdjudicaciones ({len(awards)}):")
    table = [["Proveedor", "Item", "Cantidad", "Precio unit.", "Total", "Moneda"]]

    for aid, adate, astatus, aval, acurr in awards:
        cur = conn.execute(
            "SELECT supplier_name FROM award_suppliers WHERE ocid = ? AND award_id = ?",
            (ocid, aid)
        )
        suppliers = [r[0] for r in cur.fetchall() if r[0]]
        supplier_str = ", ".join(suppliers) if suppliers else "Sin identificar"

        fecha = adate[:10] if adate else "?"
        lines.append(f"\n  Proveedor: {supplier_str} [{fecha}] ({astatus or ''})")
        if aval:
            lines.append(f"  Monto adjudicación: ${aval:,.0f} {acurr or ''}")

        cur = conn.execute(
            "SELECT description, classification_description, quantity, unit_name, "
            "unit_value_amount, unit_value_currency "
            "FROM award_items WHERE ocid = ? AND award_id = ?",
            (ocid, aid)
        )
        for desc, cls_desc, qty, unit, precio, curr in cur.fetchall():
            nombre = desc or cls_desc or ""
            cant = f"{qty:g}" if qty else ""
            total = precio * qty if precio and qty else None
            precio_str = f"${precio:,.2f}" if precio else ""
            total_str = f"${total:,.0f}" if total else ""
            curr_str = curr or "UYU"
            lines.append(
                f"    - {nombre[:60]} | {cant} {unit or ''}"
                f" x {precio_str} = {total_str} {curr_str}"
            )
            table.append([supplier_str, nombre[:60], cant, precio_str, total_str, curr_str])

    return table


def detalle_proceso(ocid):
    """Muestra todos los detalles de un proceso de compra por su identificador OCID.

    Args:
        ocid: Identificador OCID del proceso. Ej: "ocds-yfs5dr-1307121".

    Returns:
        str: Detalle completo del proceso con comprador, licitación, adjudicaciones,
             proveedores e items con montos.
    """
    conn = _get_conn()

    cur = conn.execute(
        "SELECT ocid, release_id, date, tag, buyer_id, buyer_name "
        "FROM releases WHERE ocid = ? ORDER BY date",
        (ocid,)
    )
    releases = cur.fetchall()
    if not releases:
        conn.close()
        return f"No se encontró el proceso con OCID '{ocid}'."

    buyer = next((r[5] for r in releases if r[5]), "Sin identificar")
    lines = [f"Proceso {ocid}:", f"Comprador: {buyer}"]

    lines.append(f"\nEventos ({len(releases)}):")
    for _, rid, date, tag, _, _ in releases:
        fecha = date[:10] if date else "?"
        lines.append(f"  - [{fecha}] {tag}: {rid}")

    # Tender
    cur = conn.execute(
        "SELECT tender_id, title, description, procurement_method_details, "
        "status, start_date, end_date FROM tenders WHERE ocid = ?",
        (ocid,)
    )
    tenders = cur.fetchall()
    tender_table = None
    if tenders:
        t = tenders[0]
        lines.append(f"\nLicitación: {t[1] or t[0]}")
        if t[2]:
            lines.append(f"Descripción: {t[2][:200]}")
        if t[3]:
            lines.append(f"Método: {t[3]}")
        if t[4]:
            lines.append(f"Estado: {t[4]}")
        if t[5]:
            lines.append(f"Período: {t[5][:10]} a {(t[6] or '?')[:10]}")

        cur = conn.execute(
            "SELECT item_id, description, quantity, classification_description, unit_name "
            "FROM tender_items WHERE ocid = ?",
            (ocid,)
        )
        tender_items = cur.fetchall()
        if tender_items:
            lines.append(f"\nItems licitados ({len(tender_items)}):")
            tender_table = [["Item", "Descripción", "Cantidad", "Unidad"]]
            for iid, desc, qty, cls_desc, unit in tender_items:
                nombre = desc or cls_desc or ""
                cant = f"{qty:g}" if qty else ""
                lines.append(f"  - {nombre[:70]} | {cant} {unit or ''}")
                tender_table.append([iid, nombre[:70], cant, unit or ""])

    # Awards
    award_table = _build_award_detail(conn, ocid, lines)

    conn.close()

    result = "\n".join(lines)
    if award_table:
        result += f"<table>{json.dumps(award_table, ensure_ascii=False)}</table>"
    elif tender_table:
        result += f"<table>{json.dumps(tender_table, ensure_ascii=False)}</table>"

    url = _ocid_to_url(ocid)
    if _check_url(url):
        result += (
            f"<force>Aquí puedes ver el link oficial con detalles de esta compra: {url}</force>"
        )
    else:
        result += (
            f"\nLa URL {url} debería contener detalles pero no está funcionando ahora."
        )

    return result
