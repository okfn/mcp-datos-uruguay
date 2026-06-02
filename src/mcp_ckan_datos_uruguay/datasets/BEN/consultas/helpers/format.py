"""
Formateo de números, builders de `CallToolResult`, tablas y mensajes fijos.

`fmt_num`, `text_result`/`empty_result` (arman el `CallToolResult` con su
`structuredContent`), `mix_breakdown_lines` y `build_table` para el texto y la
tabla de cada respuesta, más las notas fijas `ALREADY_TABLE`/`ALREADY_CHART` y
el `SOURCE_FOOTER`.
"""

import pandas as pd
from mcp.types import CallToolResult, TextContent


# Notas para la IA: la tabla y el gráfico ya se renderizaron en la UI vía
# structuredContent, así que no debe re-emitirlos.
ALREADY_TABLE = (
    "Hemos impreso una tabla con los datos al usuario, no es necesario que "
    "agregues datos crudos, solo haz tu analisis de los datos"
)
ALREADY_CHART = (
    "Acabamos de generar y mostrarle un gráfico al usuario. No es necesario "
    "que intentes generar otro."
)

SOURCE_FOOTER = (
    "Fuente: MIEM - Balance Energético Nacional, Uruguay "
    "(catalogodatos.gub.uy)."
)

# Guardrails para la IA: se appendea automáticamente a toda respuesta de BEN
# desde `text_result`. Evita que el modelo invente nombres de empresas y que
# atribuya las variaciones a causas (clima, lluvias) que no están en los datos.
SIN_ESPECULAR = (
    "Responde únicamente con los datos presentes en esta respuesta y en los "
    "datasets del BEN (producción y consumo energético). NO menciones nombres "
    "de empresas, compañías ni operadores que no aparezcan explícitamente en "
    "los datos. NO atribuyas las variaciones (por ejemplo, suba o baja de la "
    "generación hidráulica) a causas como clima, lluvias, sequía o nivel de "
    "embalses: el BEN de Uruguay no contiene datos meteorológicos ni "
    "hidrológicos. Limítate a describir qué muestran los números."
)


def fmt_num(v, dec=0):
    """Formato '1,234' (locale C) o '-' si NaN/None."""
    if v is None:
        return "-"
    try:
        if pd.isna(v):
            return "-"
    except (TypeError, ValueError):
        pass
    return f"{float(v):,.{dec}f}"


def text_result(text, sources, table=None, charts=None):
    sc = {"sources": sources}
    if table:
        sc["table"] = table
    if charts:
        sc["charts"] = charts
    text = f"{text}\n\n{SIN_ESPECULAR}"
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structuredContent=sc,
    )


def empty_result(label, sources):
    return CallToolResult(
        content=[TextContent(
            type="text",
            text=f"No hay datos disponibles {label}.",
        )],
        structuredContent={"sources": sources},
    )


def mix_breakdown_lines(row, fuentes, total_col="TOTAL", incluye_pct_renov=None):
    """Genera las líneas '  - Hidráulica:    7,331 GWh  (42.6%)' para
    el último año / año único. Devuelve (lines, valores) donde valores
    es una list[(etiqueta, valor, pct)] útil para % renovables.

    `incluye_pct_renov`: si se pasa una list de etiquetas consideradas
    renovables, agrega una línea con el % renovable.
    """
    total = float(row[total_col]) if pd.notna(row[total_col]) else 0.0
    valores = []
    lines = []
    for col, etiqueta in fuentes:
        v = float(row[col]) if pd.notna(row[col]) else 0.0
        pct = (v / total * 100) if total else 0.0
        valores.append((etiqueta, v, pct))
        lines.append(f"  - {etiqueta:<14}: {fmt_num(v):>10}  ({pct:>5.1f}%)")
    if incluye_pct_renov is not None:
        renov = sum(v for et, v, _ in valores if et in incluye_pct_renov)
        pct_renov = (renov / total * 100) if total else 0.0
        lines.append(f"  → Renovables: {pct_renov:.1f}%")
    return lines, valores


def build_table(df, fuentes, total_col="TOTAL", extra_cols=None):
    """Tabla genérica: Año + 1 col por fuente + Total + cols extra.

    `extra_cols`: list[(header, fn(row) -> str)] para columnas calculadas
    como '% Renov.' o tasas. Si None, sólo se muestra Total.
    """
    headers = ["Año"] + [et for _, et in fuentes] + [total_col.title()]
    if extra_cols:
        headers += [h for h, _ in extra_cols]
    rows = [headers]
    for _, row in df.iterrows():
        line = [str(int(row["anio"]))]
        for col, _et in fuentes:
            line.append(fmt_num(row[col]))
        line.append(fmt_num(row[total_col]))
        if extra_cols:
            for _h, fn in extra_cols:
                line.append(fn(row))
        rows.append(line)
    return rows
