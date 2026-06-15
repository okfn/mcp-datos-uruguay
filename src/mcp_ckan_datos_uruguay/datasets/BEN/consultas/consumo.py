"""
Tools BEN - Consumo final energético (lado demanda).

Cubre dos datasets MIEM:
  - `miem-consumo-final-energetico-por-fuente` (ktep, 1965-2024)
  - `miem-consumo-final-energetico-por-sector`  (ktep, 1965-2024)

El TOTAL de ambos coincide para el mismo año (misma magnitud, dos
desagregaciones). Verificado: 2024 = 6076.4 ktep en ambos.

Preguntas del README cubiertas:
  - 1.1 consumo total último año
  - 1.2 evolución del consumo
  - 1.4 ¿aumenta o reduce demanda?
  - 1.5 fuentes que predominan en demanda
  - 5.1 sector que más consume
  - 5.2 evolución por sector
  - 5.3 sector que más crece
"""

import pandas as pd

from mcp_server import DataToolOutput
from . import helpers as h


# ═══ Consumo final por sector ═════════════════════════════════════════════

CONSUMO_SECTORES = [
    ("I", "Industrial"),
    ("T", "Transporte"),
    ("R", "Residencial"),
    ("C_S_SP", "Comercial/Servicios/Sec.Público"),
    ("AP", "Actividades primarias"),
    ("NI", "No identificado"),
]
def consumo_final_por_sector(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Consumo final energético desagregado por sector, en ktep."""
    df = h.load_dataset("consumo_sector")
    df = h.filter_years(df, anio_desde, anio_hasta)
    src = [h.DATASET_PAGES["consumo_sector"]]
    if df.empty:
        return h.empty_result("de consumo final por sector en ese rango", src)

    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])
    rango = (
        f"{anio_ult}" if len(df) == 1
        else f"{int(df['anio'].min())}-{anio_ult}"
    )

    breakdown_lines, _ = h.mix_breakdown_lines(ult, CONSUMO_SECTORES)

    lines = [
        f"Consumo final energético de Uruguay por sector, {rango} (ktep).",
        "",
        f"Mix sectorial {anio_ult} - TOTAL = {h.fmt_num(ult['TOTAL'], 1)} ktep:",
    ] + breakdown_lines

    if len(df) >= 2:
        prim = df.iloc[0]
        # Tasa de variación anual compuesta por sector
        n = int(df.iloc[-1]["anio"]) - int(df.iloc[0]["anio"])
        if n > 0:
            crec = []
            for col, etiqueta in CONSUMO_SECTORES:
                v0, v1 = prim[col], ult[col]
                if pd.notna(v0) and pd.notna(v1) and v0 > 0:
                    cagr = ((float(v1) / float(v0)) ** (1 / n) - 1) * 100
                    crec.append((etiqueta, cagr))
            crec.sort(key=lambda x: -x[1])
            lines.append("")
            lines.append(
                f"Crecimiento anual promedio (CAGR) {int(prim['anio'])}-{anio_ult}:"
            )
            for et, g in crec:
                lines.append(f"  - {et:<32}: {g:+.2f}% / año")
            sector_top = crec[0]
            lines.append(
                f"  → Sector que más creció: **{sector_top[0]}** "
                f"({sector_top[1]:+.2f}%/año)."
            )

    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(
        "Nota 1965-1979: la columna 'Comercial/Servicios/Sec.Público' "
        "está incluida dentro de 'Residencial'."
    )
    lines.append(h.definiciones_relevantes("ktep", "energia_final", "sector_de_consumo"))
    lines.append("")
    lines.append(h.SOURCE_FOOTER)

    table = h.build_table(df, CONSUMO_SECTORES)

    chart = h.chart_for_mix(
        df, CONSUMO_SECTORES,
        f"Consumo final energético por sector ({rango}), ktep",
        palette=h.COLORES_BEN,
    )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])


# ═══ Consumo final por fuente ═════════════════════════════════════════════

CONSUMO_FUENTES = [
    ("DP", "Derivados del petróleo"),
    ("RB", "Residuos de biomasa"),
    ("EE", "Electricidad"),
    ("L_CV", "Leña / carbón vegetal"),
    ("GN", "Gas natural"),
    ("Bc", "Biocombustibles"),
    ("S", "Solar térmica"),
    ("CM", "Carbón mineral"),
    ("DC", "Derivados del carbón"),
    ("RI", "Residuos industriales"),
]
# Renovables/limpias del lado consumo (Bc, leña, biomasa, solar térmica).
# Electricidad y residuos industriales no se cuentan como renovables aquí
# (electricidad arrastra mix variable; RI es recuperación, no renovable).
RENOVABLES_CONSUMO = {
    "Residuos de biomasa", "Leña / carbón vegetal",
    "Biocombustibles", "Solar térmica",
}


def consumo_final_por_fuente(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Consumo final energético desagregado por fuente, en ktep."""
    df = h.load_dataset("consumo_fuente")
    df = h.filter_years(df, anio_desde, anio_hasta)
    src = [h.DATASET_PAGES["consumo_fuente"]]
    if df.empty:
        return h.empty_result("de consumo final por fuente en ese rango", src)

    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])
    rango = (
        f"{anio_ult}" if len(df) == 1
        else f"{int(df['anio'].min())}-{anio_ult}"
    )

    breakdown_lines, _ = h.mix_breakdown_lines(
        ult, CONSUMO_FUENTES,
        incluye_pct_renov=RENOVABLES_CONSUMO,
    )

    lines = [
        f"Consumo final energético de Uruguay por fuente, {rango} (ktep).",
        "",
        f"Mix por fuente {anio_ult} - TOTAL = {h.fmt_num(ult['TOTAL'], 1)} ktep:",
    ] + breakdown_lines
    lines.append("")
    lines.append(
        "El % renovable considera leña/carbón vegetal, residuos de biomasa, "
        "biocombustibles y solar térmica. La electricidad se contabiliza "
        "aparte: para ver su mix renovable usar `matriz_generacion_electrica`."
    )
    lines.append(h.definiciones_relevantes("ktep", "energia_final", "residuos_de_biomasa"))
    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(h.SOURCE_FOOTER)

    def _pct_renov(row):
        total = float(row["TOTAL"]) if pd.notna(row["TOTAL"]) else 0.0
        renov = sum(float(row[col]) for col, et in CONSUMO_FUENTES
                    if et in RENOVABLES_CONSUMO and pd.notna(row[col]))
        return f"{(renov / total * 100):.1f}%" if total else "-"

    table = h.build_table(
        df, CONSUMO_FUENTES,
        extra_cols=[("% Renov.", _pct_renov)],
    )

    chart = h.chart_for_mix(
        df, CONSUMO_FUENTES,
        f"Consumo final energético por fuente ({rango}), ktep",
        palette=h.COLORES_BEN,
    )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])


# ═══ Tendencia del consumo total (Q1.1, Q1.2, Q1.4) ═══════════════════════

def tendencia_demanda_total(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Evolución del consumo final energético TOTAL del país, ktep/año."""
    df = h.load_dataset("consumo_fuente")
    df = h.filter_years(df, anio_desde, anio_hasta)
    src = [h.DATASET_PAGES["consumo_fuente"]]
    if df.empty:
        return h.empty_result("de consumo total en ese rango", src)

    rango = (
        f"{int(df['anio'].iloc[0])}" if len(df) == 1
        else f"{int(df['anio'].min())}-{int(df['anio'].max())}"
    )
    ult = df.iloc[-1]
    prim = df.iloc[0]
    n = int(ult["anio"]) - int(prim["anio"])

    lines = [
        f"Consumo final energético TOTAL de Uruguay, {rango} (ktep/año).",
        "",
        f"  - {int(ult['anio'])}: {h.fmt_num(ult['TOTAL'], 1)} ktep "
        f"(≈ {float(ult['TOTAL']) * 11.63 / 1000:.1f} TWh equivalentes).",
    ]
    if n > 0 and float(prim["TOTAL"]) > 0:
        cagr = ((float(ult["TOTAL"]) / float(prim["TOTAL"])) ** (1 / n) - 1) * 100
        delta_pct = (float(ult["TOTAL"]) / float(prim["TOTAL"]) - 1) * 100
        lines.append(
            f"  - {int(prim['anio'])}: {h.fmt_num(prim['TOTAL'], 1)} ktep."
        )
        lines.append(
            f"  - Variación {int(prim['anio'])}→{int(ult['anio'])}: "
            f"{delta_pct:+.1f}% total ({cagr:+.2f}% / año compuesto)."
        )
        # ¿Aumenta o reduce demanda? Mirar últimos 5 años o todo el rango.
        ult_n = df.tail(min(6, len(df)))
        if len(ult_n) >= 2:
            x0 = float(ult_n.iloc[0]["TOTAL"])
            x1 = float(ult_n.iloc[-1]["TOTAL"])
            tendencia = "↑ creciendo" if x1 > x0 else "↓ contrayéndose" if x1 < x0 else "↔ estable"
            lines.append(
                f"  - Últimos {len(ult_n)} años: {tendencia} "
                f"({(x1 / x0 - 1) * 100:+.1f}% en el período)."
            )

    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(
        "Recordar: BEN no incluye PIB ni población. Para 'consumo per "
        "cápita' o 'intensidad energética = energía/PIB' hay que cruzar "
        "con datos del INE / BCU."
    )
    lines.append(h.definiciones_relevantes("ktep", "energia_final"))
    lines.append("")
    lines.append(h.SOURCE_FOOTER)

    table = [["Año", "Consumo total (ktep)"]]
    for _, row in df.iterrows():
        table.append([str(int(row["anio"])), h.fmt_num(row["TOTAL"], 1)])

    chart = h.line_chart(
        f"Consumo final energético total Uruguay ({rango}), ktep",
        df["anio"].tolist(),
        [("Consumo total", df["TOTAL"].tolist())],
        palette=h.COLORES_BEN,
    )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])
