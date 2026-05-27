"""
Tools BEN — Electricidad.

Cubre tres datasets MIEM:
  - `miem-generacion-de-electricidad-por-fuente` (GWh, 2002-2024)
  - `miem-potencia-instalada-por-fuente` (MW, datos útiles 2003-2024)
  - `miem-ben-factor-de-emision-de-co2-del-sin` (t CO2/GWh, 1965-2024)

Preguntas del README cubiertas: 1.5 (fuentes predominantes en eléctrico),
2.3 (principal fuente), 2.4 (fuentes que más crecen, vía capacidad), 2.5
(diversificación), 6 (transición — entrada de eólica/solar y caída del
factor de emisión).
"""

import pandas as pd

from mcp_server import DataToolOutput
from . import helpers as h


# ═══ Generación eléctrica por fuente (GWh) ═══════════════════════════════

GENERACION_FUENTES = [
    ("EE_H", "Hidráulica"),
    ("EE_Eo", "Eólica"),
    ("EE_S", "Solar"),
    ("EE_B", "Biomasa"),
    ("EE_F", "Fósil"),
]
GENERACION_PALETA = {
    "Hidráulica": "#1f77b4", "Eólica": "#2ca02c", "Solar": "#ff7f0e",
    "Biomasa": "#8c564b", "Fósil": "#7f7f7f",
}
RENOVABLES_GEN = {"Hidráulica", "Eólica", "Solar", "Biomasa"}


def matriz_generacion_electrica(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Matriz de generación eléctrica anual por fuente, en GWh."""
    df = h.load_dataset("generacion")
    df = h.filter_years(df, anio_desde, anio_hasta)
    src = [h.DATASET_PAGES["generacion"]]
    if df.empty:
        return h.empty_result("de generación eléctrica en ese rango", src)

    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])
    rango = (
        f"{anio_ult}" if len(df) == 1
        else f"{int(df['anio'].min())}-{anio_ult}"
    )
    breakdown_lines, _ = h.mix_breakdown_lines(
        ult, GENERACION_FUENTES,
        incluye_pct_renov=RENOVABLES_GEN,
    )

    lines = [
        f"Generación eléctrica de Uruguay, {rango} (GWh por año).",
        "",
        f"Mix {anio_ult} — TOTAL = {h.fmt_num(ult['TOTAL'])} GWh:",
    ] + breakdown_lines
    lines.append(h.unit_blurb("GWh"))
    lines.append(h.definiciones_relevantes("clasificacion_por_tipo", "energia_solar"))
    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(h.SOURCE_FOOTER)

    def _pct_renov(row):
        total = float(row["TOTAL"])
        renov = sum(float(row[col]) for col, et in GENERACION_FUENTES
                    if et in RENOVABLES_GEN and pd.notna(row[col]))
        return f"{(renov / total * 100):.1f}%" if total else "—"

    table = h.build_table(
        df, GENERACION_FUENTES,
        extra_cols=[("% Renov.", _pct_renov)],
    )

    chart = h.chart_for_mix(
        df, GENERACION_FUENTES,
        f"Matriz de generación eléctrica Uruguay ({rango}), GWh",
        palette=GENERACION_PALETA,
    )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])


# ═══ Potencia instalada por fuente (MW) ═══════════════════════════════════

# La potencia tiene un nivel adicional: las fósil/biomasa tienen subtotales
# por tecnología. Usamos los TOTAL_X para mantenernos al mismo nivel que
# generación.
POTENCIA_FUENTES = [
    ("TOTAL_H", "Hidráulica"),
    ("TOTAL_Eo", "Eólica"),
    ("TOTAL_S", "Solar"),
    ("TOTAL_B", "Biomasa"),
    ("TOTAL_F", "Fósil"),
]
POTENCIA_PALETA = GENERACION_PALETA
RENOVABLES_POT = {"Hidráulica", "Eólica", "Solar", "Biomasa"}


def potencia_instalada_por_fuente(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Capacidad instalada por fuente, en MW (total al cierre de cada año)."""
    df = h.load_dataset("potencia")
    df = h.filter_years(df, anio_desde, anio_hasta)
    # Los años pre-2003 vienen vacíos; descartar filas donde TOTAL es NaN.
    df = df[df["TOTAL"].notna()].reset_index(drop=True)
    src = [h.DATASET_PAGES["potencia"]]
    if df.empty:
        return h.empty_result("de potencia instalada en ese rango", src)

    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])
    rango = (
        f"{anio_ult}" if len(df) == 1
        else f"{int(df['anio'].min())}-{anio_ult}"
    )
    breakdown_lines, _ = h.mix_breakdown_lines(
        ult, POTENCIA_FUENTES,
        incluye_pct_renov=RENOVABLES_POT,
    )

    lines = [
        f"Potencia (capacidad) eléctrica instalada en Uruguay, "
        f"{rango} (MW al cierre de cada año).",
        "",
        f"Mix de capacidad {anio_ult} — TOTAL = {h.fmt_num(ult['TOTAL'])} MW:",
    ] + breakdown_lines
    lines.append("")
    lines.append(
        "Nota: capacidad ≠ generación. Una hidro de 1.500 MW puede generar "
        "poco en un año seco. Para uso efectivo cruzar con generación."
    )
    lines.append(h.unit_blurb("MW"))
    lines.append(h.definiciones_relevantes("clasificacion_por_tipo", "energia_solar"))
    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(h.SOURCE_FOOTER)

    def _pct_renov(row):
        total = float(row["TOTAL"])
        renov = sum(float(row[col]) for col, et in POTENCIA_FUENTES
                    if et in RENOVABLES_POT and pd.notna(row[col]))
        return f"{(renov / total * 100):.1f}%" if total else "—"

    table = h.build_table(
        df, POTENCIA_FUENTES,
        extra_cols=[("% Renov.", _pct_renov)],
    )

    chart = h.chart_for_mix(
        df, POTENCIA_FUENTES,
        f"Capacidad instalada Uruguay ({rango}), MW",
        palette=POTENCIA_PALETA,
    )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])


# ═══ Factor de emisión de CO2 del SIN (t CO2/GWh) ═════════════════════════

def factor_emision_electrico(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Intensidad de carbono del Sistema Interconectado Nacional (SIN)."""
    df = h.load_dataset("factor_emision_sin")
    df = h.filter_years(df, anio_desde, anio_hasta)
    df = df[df["FE_SIN"].notna()].reset_index(drop=True)
    src = [h.DATASET_PAGES["factor_emision_sin"]]
    if df.empty:
        return h.empty_result(
            "del factor de emisión del SIN en ese rango", src,
        )

    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])
    rango = (
        f"{anio_ult}" if len(df) == 1
        else f"{int(df['anio'].min())}-{anio_ult}"
    )
    fe_ult = float(ult["FE_SIN"])
    em_ult = float(ult["Em_CE_SP"])
    ee_ult = float(ult["EE_CE_SP"])

    fe_min = df["FE_SIN"].min()
    fe_max = df["FE_SIN"].max()
    anio_min = int(df.loc[df["FE_SIN"].idxmin(), "anio"])
    anio_max = int(df.loc[df["FE_SIN"].idxmax(), "anio"])

    lines = [
        f"Factor de emisión del SIN (intensidad de carbono del sistema "
        f"eléctrico uruguayo), {rango}.",
        "",
        f"  - {anio_ult}: FE_SIN = {fe_ult:.1f} t CO2/GWh "
        f"(emisiones {h.fmt_num(em_ult, 1)} Gg CO2 sobre "
        f"{h.fmt_num(ee_ult)} GWh entregados).",
        f"  - Mínimo histórico en el rango: {fe_min:.1f} t CO2/GWh ({anio_min}).",
        f"  - Máximo histórico en el rango: {fe_max:.1f} t CO2/GWh ({anio_max}).",
        "",
        "Lectura: cuanto menor el FE_SIN, más limpio el sistema. La "
        "volatilidad anual la marca el régimen hidráulico — años secos "
        "obligan a despachar térmica fósil.",
        "Referencias: térmica a gas ciclo combinado ≈ 350-400; térmica a "
        "carbón ≈ 800-1.000; renovables (en operación) ≈ 0 t CO2/GWh.",
    ]
    lines.append(h.unit_blurb("t CO2/GWh", "Gg CO2", "GWh"))
    lines.append(h.definiciones_relevantes("emisiones_co2"))
    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(h.SOURCE_FOOTER)

    table = [["Año", "FE_SIN (t CO2/GWh)", "Emisiones (Gg CO2)", "Generación SIN (GWh)"]]
    for _, row in df.iterrows():
        table.append([
            str(int(row["anio"])),
            f"{float(row['FE_SIN']):.1f}",
            h.fmt_num(row["Em_CE_SP"], 1),
            h.fmt_num(row["EE_CE_SP"]),
        ])

    if len(df) == 1:
        # Una sola medida: barra simple comparando con referencias visuales.
        chart = h.grouped_bar_chart(
            f"Factor de emisión del SIN — {anio_ult} vs referencias técnicas",
            [anio_ult],
            [
                (f"SIN Uruguay {anio_ult}", [fe_ult]),
                ("Térmica gas ciclo combinado", [375.0]),
                ("Térmica carbón", [900.0]),
            ],
        )
    else:
        chart = h.line_chart(
            f"Factor de emisión del SIN ({rango}), t CO2/GWh",
            df["anio"].tolist(),
            [("FE_SIN", df["FE_SIN"].tolist())],
            palette={"FE_SIN": "#d62728"},
        )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])
