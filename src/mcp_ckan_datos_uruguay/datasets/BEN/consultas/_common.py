"""
Helpers compartidos para las consultas BEN.

Centraliza:
  - URLs de los 10 datasets (página + CSV directo).
  - Loader cacheado (`load_dataset`) que normaliza la columna AÑO -> anio.
  - Constructores de gráficos Chart.js (`pie_chart`, `stacked_bar_chart`,
    `line_chart`, `grouped_bar_chart`) y un selector automático
    (`chart_for_mix`) que elige pie cuando hay 1 año y stacked-bar cuando
    hay varios.
  - Bloque de explicación de unidades (`unit_blurb`) — siempre adjuntar
    al texto de respuesta para que el lector no técnico entienda ktep,
    GWh, MW, Gg CO2, t CO2/GWh.
  - Builders de `CallToolResult` (`text_result`, `empty_result`).

No definir tools aquí; las tools van en sus módulos temáticos
(electricidad, consumo, abastecimiento, importaciones, emisiones).
"""

import pandas as pd
from mcp.types import CallToolResult, TextContent


PORTAL = "https://catalogodatos.gub.uy/dataset/"

# Página del dataset (slug) → URL al portal CKAN
DATASET_PAGES = {
    "gas_natural":            PORTAL + "ben-importacion-de-gas-natural",
    "petroleo":               PORTAL + "ben-importacion-de-petroleo-y-carga-de-refineria",
    "impo_expo_electricidad": PORTAL + "ben-importacion-y-exportacion-de-electricidad",
    "abastecimiento":         PORTAL + "miem-abastecimiento-de-energia-por-fuente",
    "factor_emision_sin":     PORTAL + "miem-ben-factor-de-emision-de-co2-del-sin",
    "consumo_fuente":         PORTAL + "miem-consumo-final-energetico-por-fuente",
    "consumo_sector":         PORTAL + "miem-consumo-final-energetico-por-sector",
    "emisiones_sector":       PORTAL + "miem-emisiones-de-co2-por-sector",
    "generacion":             PORTAL + "miem-generacion-de-electricidad-por-fuente",
    "potencia":               PORTAL + "miem-potencia-instalada-por-fuente",
}

# Cada entrada es (dataset_uuid, resource_uuid, filename) — verificada
# contra los snapshots `_meta_*.json` de `package_show`. El CSV directo
# se compone debajo en `DATASET_URLS`.
_DATASET_PARTS = {
    "gas_natural": (
        "61631f9e-03ac-4080-a225-a4ddad8da40f",
        "9690c070-d697-40ff-a848-7f9dc98454ab",
        "impo_gas_natural.csv",
    ),
    "petroleo": (
        "db244448-da5e-4dff-8bca-8163ed9c8d6d",
        "7efe24ee-b7ee-45fb-8b97-5518b02930fe",
        "impo_y_carga_refineria_petroleo.csv",
    ),
    "impo_expo_electricidad": (
        "b0ab3d1f-4d00-490b-86fc-727238a405cc",
        "dbec6e8f-0202-46dc-8bf5-64ef4018cab9",
        "impo_y_expo_electricidad.csv",
    ),
    "abastecimiento": (
        "fc5e7e9c-73cc-4c10-a3b7-6c27848bc095",
        "eb3fadb3-a0b5-4324-8742-5355632f9b24",
        "abastecimiento-de-energia-por-fuente.csv",
    ),
    "factor_emision_sin": (
        "10967adb-2b87-4d25-b8e3-7accbcc917f5",
        "79af11a5-a11d-49e8-8e56-b54b5539af0c",
        "factor_emision-co2-sin.csv",
    ),
    "consumo_fuente": (
        "1737fc66-58b7-4286-a246-f1864533975d",
        "6e6af1ef-15ee-434a-ba11-b091641b292d",
        "consumo-final-energetico-por-fuente.csv",
    ),
    "consumo_sector": (
        "9beed8c2-b881-4e9b-a3d8-9412e48b9554",
        "11142183-96cb-4309-8f90-2b368f8eb99c",
        "consumo-final-energetico-por-sector.csv",
    ),
    "emisiones_sector": (
        "fded3eeb-2904-43a5-a348-0b31434ef085",
        "0276c0ec-a444-4221-8354-b4909b5bd2d6",
        "emisiones-de-co2-por-sector.csv",
    ),
    "generacion": (
        "91118d5f-11be-4c84-82f0-79370ab7b089",
        "101c92a0-2906-4232-ad8d-34234a5eca17",
        "generacion-electricidad-por-fuente.csv",
    ),
    "potencia": (
        "a3254965-7f67-4c91-b52d-7c50d2192f40",
        "1306f14e-fdf8-4e26-82c6-7e12319ee0ec",
        "potencia-instalada-por-fuente.csv",
    ),
}

DATASET_URLS = {
    key: f"{PORTAL}{ds}/resource/{rs}/download/{fn}"
    for key, (ds, rs, fn) in _DATASET_PARTS.items()
}


_caches = {}


def load_dataset(key):
    """Lee el CSV remoto del MIEM (encoding latin-1, sep ';'). Cachea por key.

    Renombra `AÑO` → `anio` y la castea a int para que las tools puedan
    filtrar con confianza.
    """
    if key in _caches:
        return _caches[key]
    url = DATASET_URLS[key]
    df = pd.read_csv(url, sep=";", encoding="latin-1")
    if "AÑO" in df.columns:
        df = df.rename(columns={"AÑO": "anio"})
    df["anio"] = df["anio"].astype(int)
    _caches[key] = df
    return df


def filter_years(df, anio_desde=None, anio_hasta=None):
    """Filtra por rango cerrado [anio_desde, anio_hasta] y ordena."""
    if anio_desde is not None:
        df = df[df["anio"] >= int(anio_desde)]
    if anio_hasta is not None:
        df = df[df["anio"] <= int(anio_hasta)]
    return df.sort_values("anio").reset_index(drop=True)


# ─── Unidades ──────────────────────────────────────────────────────────────

UNIT_BLURB = {
    "ktep": (
        "ktep = kilotonelada equivalente de petróleo. Unidad estándar de "
        "balances energéticos: permite sumar fuentes muy distintas "
        "(electricidad, gas, leña, etc.). 1 ktep ≈ 11.63 GWh."
    ),
    "GWh": (
        "GWh = gigavatio-hora = 1 millón de kWh. Energía eléctrica "
        "efectivamente entregada en un período."
    ),
    "MW": (
        "MW = megavatio. Unidad de **potencia** (capacidad), no de energía. "
        "Una central de 100 MW al 100% durante 1 año generaría 876 GWh."
    ),
    "Gg CO2": (
        "Gg = gigagramo = 1.000 toneladas (= 1 kt). Convención IPCC para "
        "inventarios nacionales de gases de efecto invernadero."
    ),
    "t CO2/GWh": (
        "Intensidad de carbono: toneladas de CO2 emitidas por GWh de "
        "electricidad generada. Cuanto más bajo, más limpia la matriz."
    ),
}


def unit_blurb(*units):
    """Bloque '💡 Unidades' para concatenar al texto de respuesta."""
    lines = ["", "💡 Unidades:"]
    for u in units:
        if u in UNIT_BLURB:
            lines.append(f"  - {UNIT_BLURB[u]}")
    return "\n".join(lines)


# ─── Charts ────────────────────────────────────────────────────────────────

# Paleta categórica por defecto (Tableau-10) cuando no se provee paleta
PALETTE_DEFAULT = [
    "#1f77b4", "#2ca02c", "#ff7f0e", "#8c564b", "#7f7f7f",
    "#9467bd", "#d62728", "#17becf", "#bcbd22", "#e377c2",
]


def _color(label, palette, idx):
    if palette and label in palette:
        return palette[label]
    return PALETTE_DEFAULT[idx % len(PALETTE_DEFAULT)]


def pie_chart(title, slices, palette=None):
    """Pie chart Chart.js. `slices`: list[(label, value)] en el orden deseado.

    Las celdas con NaN se renderizan como 0 (Chart.js no soporta huecos).
    """
    labels = [s[0] for s in slices]
    values = [
        float(s[1]) if s[1] is not None and not (
            isinstance(s[1], float) and pd.isna(s[1])
        ) else 0.0
        for s in slices
    ]
    colors = [_color(lbl, palette, i) for i, lbl in enumerate(labels)]
    return {
        "type": "pie",
        "title": title,
        "labels": labels,
        "datasets": [{"data": values, "backgroundColor": colors}],
    }


def _values_safe(values):
    """Convierte una lista a floats, NaN → 0."""
    return [
        float(v) if v is not None and not (
            isinstance(v, float) and pd.isna(v)
        ) else 0.0
        for v in values
    ]


def stacked_bar_chart(title, anios, series, palette=None):
    """Barras apiladas. `series`: list[(label, [valores por año])]."""
    datasets = []
    for i, (label, values) in enumerate(series):
        datasets.append({
            "label": label,
            "data": _values_safe(values),
            "backgroundColor": _color(label, palette, i),
        })
    return {
        "type": "bar",
        "stacked": True,
        "title": title,
        "labels": [str(int(a)) for a in anios],
        "datasets": datasets,
    }


def grouped_bar_chart(title, anios, series, palette=None):
    """Barras agrupadas (no stacked). Mismo input que `stacked_bar_chart`."""
    datasets = []
    for i, (label, values) in enumerate(series):
        datasets.append({
            "label": label,
            "data": _values_safe(values),
            "backgroundColor": _color(label, palette, i),
        })
    return {
        "type": "bar",
        "title": title,
        "labels": [str(int(a)) for a in anios],
        "datasets": datasets,
    }


def line_chart(title, anios, series, palette=None):
    """Líneas (no apiladas). `series`: list[(label, [valores por año])]."""
    datasets = []
    for i, (label, values) in enumerate(series):
        datasets.append({
            "label": label,
            "data": _values_safe(values),
            "borderColor": _color(label, palette, i),
        })
    return {
        "type": "line",
        "title": title,
        "labels": [str(int(a)) for a in anios],
        "datasets": datasets,
    }


def chart_for_mix(df, fuentes, title, palette=None):
    """Selector automático: pie si hay 1 año, stacked bar si hay varios.

    `fuentes` es list[(columna_csv, etiqueta_legible)]. Lee del DataFrame
    ya filtrado por años.
    """
    if len(df) == 1:
        row = df.iloc[0]
        slices = [(label, row[col]) for col, label in fuentes]
        return pie_chart(f"{title} — {int(row['anio'])}", slices, palette)
    return stacked_bar_chart(
        title,
        df["anio"].tolist(),
        [(label, df[col].tolist()) for col, label in fuentes],
        palette,
    )


# ─── Resultados / formato ──────────────────────────────────────────────────

def fmt_num(v, dec=0):
    """Formato '1,234' (locale C) o '—' si NaN/None."""
    if v is None:
        return "—"
    try:
        if pd.isna(v):
            return "—"
    except (TypeError, ValueError):
        pass
    return f"{float(v):,.{dec}f}"


def text_result(text, sources, table=None, charts=None):
    sc = {"sources": sources}
    if table:
        sc["table"] = table
    if charts:
        sc["charts"] = charts
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


SOURCE_FOOTER = (
    "Fuente: MIEM — Balance Energético Nacional, Uruguay "
    "(catalogodatos.gub.uy)."
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
