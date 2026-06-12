"""
Tools BEN - Emisiones de CO2.

Cubre el dataset MIEM `miem-emisiones-de-co2-por-sector` (Gg CO2,
1965-2024). Para la intensidad del SIN ver `electricidad.factor_emision_electrico`.

Preguntas del README cubiertas:
  - 5.1 sectores grandes emisores
  - 6 'termómetro' de la transición energética (caída de CE_SP con la
    entrada de renovables)
"""

import pandas as pd

from mcp_server import DataToolOutput
from . import helpers as h


# Sectores del inventario nacional. Orden por relevancia esperada.
EMISIONES_SECTORES = [
    ("T", "Transporte"),
    ("I", "Industrial"),
    ("CE_SP", "Centrales eléctricas (servicio público)"),
    ("R", "Residencial"),
    ("AP", "Actividades primarias"),
    ("C_S_SP", "Comercial/Servicios/Sec.Público"),
    ("CP", "Consumo propio sector energético"),
    ("NI", "No identificado"),
]
def emisiones_co2_por_sector(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Emisiones de CO2 por sector (combustión de combustibles fósiles)."""
    df = h.load_dataset("emisiones_sector")
    df = h.filter_years(df, anio_desde, anio_hasta)
    src = [h.DATASET_PAGES["emisiones_sector"]]
    if df.empty:
        return h.empty_result("de emisiones de CO2 por sector en ese rango", src)

    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])
    rango = (
        f"{anio_ult}" if len(df) == 1
        else f"{int(df['anio'].min())}-{anio_ult}"
    )

    breakdown_lines, _ = h.mix_breakdown_lines(ult, EMISIONES_SECTORES)

    lines = [
        f"Emisiones de CO2 por sector en Uruguay, {rango} (Gg CO2 = "
        f"kilotoneladas).",
        "",
        f"Mix sectorial {anio_ult} - TOTAL = {h.fmt_num(ult['TOTAL'], 1)} "
        f"Gg CO2:",
    ] + breakdown_lines

    if len(df) >= 2:
        prim = df.iloc[0]
        delta_total = float(ult["TOTAL"]) - float(prim["TOTAL"])
        delta_pct = (
            (float(ult["TOTAL"]) / float(prim["TOTAL"]) - 1) * 100
            if float(prim["TOTAL"]) else 0.0
        )
        lines.append("")
        lines.append(
            f"Variación TOTAL {int(prim['anio'])}→{anio_ult}: "
            f"{delta_total:+.0f} Gg CO2 ({delta_pct:+.1f}%)."
        )
        # Sector con mayor caída (clave para narrativa de transición)
        cambios = []
        for col, etiqueta in EMISIONES_SECTORES:
            v0, v1 = prim[col], ult[col]
            if pd.notna(v0) and pd.notna(v1):
                cambios.append((etiqueta, float(v1) - float(v0)))
        cambios.sort(key=lambda x: x[1])
        if cambios:
            lines.append(
                f"  → Sector con mayor reducción: {cambios[0][0]} "
                f"({cambios[0][1]:+.0f} Gg)."
            )
            lines.append(
                f"  → Sector con mayor aumento: {cambios[-1][0]} "
                f"({cambios[-1][1]:+.0f} Gg)."
            )

    lines.append("")
    lines.append(
        "Notas IPCC: las partidas Q_B (quema de biomasa) y BI (búnker "
        "internacional) son **informativas**, no se suman al inventario "
        "nacional. Sólo cubre CO2 (no CH4/N2O) y sólo combustión de "
        "combustibles fósiles (no procesos industriales como cemento)."
    )
    lines.append(h.unit_blurb("Gg CO2"))
    lines.append(h.definiciones_relevantes("emisiones_co2", "sector_de_consumo"))
    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(h.SOURCE_FOOTER)

    table = h.build_table(df, EMISIONES_SECTORES, total_col="TOTAL")

    chart = h.chart_for_mix(
        df, EMISIONES_SECTORES,
        f"Emisiones de CO2 por sector ({rango}), Gg CO2",
        palette=h.COLORES_BEN,
    )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])
