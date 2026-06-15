"""
Tools BEN - Abastecimiento de energía primaria (lado oferta).

Cubre el dataset MIEM `miem-abastecimiento-de-energia-por-fuente` (ktep,
1965-2024) y construye un derivado para 'pérdidas de transformación'
cruzando con `miem-consumo-final-energetico-por-fuente`.

Preguntas del README cubiertas:
  - 1.5 fuentes que predominan en la matriz primaria
  - 2.1 % renovables vs no renovables
  - 2.2 evolución de la participación renovable
  - 2.5 diversificación de la matriz
  - 3.1 importado vs producción local
  - 3.2 nivel de dependencia energética
  - 6 pérdidas de transformación + autoconsumo del sector energético
"""

import pandas as pd

from mcp_server import DataToolOutput
from . import helpers as h


# Columnas del dataset abastecimiento - todas en ktep.
# Renovables locales: EE_H, EE_Eo, S, B
# Importadas (no renovables): EE_i, GN, P_D, C_C
# Otros: RI (residuos industriales - locales pero no renovables)
ABAST_FUENTES = [
    ("P_D", "Petróleo + derivados"),
    ("B", "Biomasa"),
    ("EE_H", "Hidráulica"),
    ("EE_Eo", "Eólica"),
    ("S", "Solar"),
    ("GN", "Gas natural"),
    ("C_C", "Carbón + coque"),
    ("EE_i", "Electricidad importada"),
    ("RI", "Residuos industriales"),
]
RENOVABLES_ABAST = {"Hidráulica", "Eólica", "Solar", "Biomasa"}
IMPORTADAS_ABAST = {"Electricidad importada", "Gas natural",
                    "Petróleo + derivados", "Carbón + coque"}


def matriz_abastecimiento_primario(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Oferta primaria de energía total del país, desagregada por fuente."""
    df = h.load_dataset("abastecimiento")
    df = h.filter_years(df, anio_desde, anio_hasta)
    src = [h.DATASET_PAGES["abastecimiento"]]
    if df.empty:
        return h.empty_result("de oferta primaria en ese rango", src)

    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])
    rango = (
        f"{anio_ult}" if len(df) == 1
        else f"{int(df['anio'].min())}-{anio_ult}"
    )

    breakdown_lines, _ = h.mix_breakdown_lines(
        ult, ABAST_FUENTES,
        incluye_pct_renov=RENOVABLES_ABAST,
    )

    # Diversificación: índice de Herfindahl-Hirschman normalizado (0=100%
    # concentrado en 1 fuente, 1=todas iguales). HHI = Σ(share²); HHI* = (1-HHI)/(1-1/n).
    total = float(ult["TOTAL"])
    shares = [
        (float(ult[col]) / total) if pd.notna(ult[col]) and total else 0
        for col, _et in ABAST_FUENTES
    ]
    hhi = sum(s * s for s in shares)
    n = len(ABAST_FUENTES)
    diversidad = (1 - hhi) / (1 - 1 / n) if n > 1 else 0

    lines = [
        f"Oferta primaria de energía de Uruguay, {rango} (ktep).",
        "",
        f"Mix primario {anio_ult} - TOTAL = {h.fmt_num(ult['TOTAL'], 1)} ktep:",
    ] + breakdown_lines
    lines.append(
        f"  → Diversificación (Herfindahl normalizado): {diversidad:.2f} "
        f"(0 = monofuente, 1 = mix perfectamente repartido)."
    )

    if len(df) >= 2:
        prim = df.iloc[0]
        prim_total = float(prim["TOTAL"])
        if prim_total:
            renov_prim = sum(
                float(prim[col]) for col, et in ABAST_FUENTES
                if et in RENOVABLES_ABAST and pd.notna(prim[col])
            )
            renov_ult = sum(
                float(ult[col]) for col, et in ABAST_FUENTES
                if et in RENOVABLES_ABAST and pd.notna(ult[col])
            )
            pct_prim = renov_prim / prim_total * 100
            pct_ult = renov_ult / float(ult["TOTAL"]) * 100
            lines.append("")
            lines.append(
                f"% Renovables: {int(prim['anio'])} = {pct_prim:.1f}% "
                f"→ {anio_ult} = {pct_ult:.1f}% "
                f"({pct_ult - pct_prim:+.1f} pp)."
            )

    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(h.definiciones_relevantes(
        "ktep", "matriz_primaria", "clasificacion_por_tipo", "energia_primaria",
    ))
    lines.append("")
    lines.append(h.SOURCE_FOOTER)

    def _pct_renov(row):
        total_r = float(row["TOTAL"]) if pd.notna(row["TOTAL"]) else 0.0
        renov = sum(float(row[col]) for col, et in ABAST_FUENTES
                    if et in RENOVABLES_ABAST and pd.notna(row[col]))
        return f"{(renov / total_r * 100):.1f}%" if total_r else "-"

    table = h.build_table(
        df, ABAST_FUENTES,
        extra_cols=[("% Renov.", _pct_renov)],
    )

    chart = h.chart_for_mix(
        df, ABAST_FUENTES,
        f"Oferta primaria de energía Uruguay ({rango}), ktep",
        palette=h.COLORES_BEN,
    )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])


# ═══ Dependencia externa ══════════════════════════════════════════════════

def dependencia_energetica_externa(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """% de energía importada vs producida localmente, año a año."""
    df = h.load_dataset("abastecimiento")
    df = h.filter_years(df, anio_desde, anio_hasta)
    src = [h.DATASET_PAGES["abastecimiento"]]
    if df.empty:
        return h.empty_result("para calcular dependencia externa", src)

    df = df.copy()
    df["importado"] = df[["EE_i", "GN", "P_D", "C_C"]].sum(axis=1, skipna=True)
    df["local"] = df["TOTAL"] - df["importado"]
    df["pct_importado"] = (df["importado"] / df["TOTAL"] * 100).round(2)

    rango = (
        f"{int(df['anio'].iloc[0])}" if len(df) == 1
        else f"{int(df['anio'].min())}-{int(df['anio'].max())}"
    )
    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])

    lines = [
        f"Dependencia energética externa de Uruguay, {rango}.",
        "",
        f"  - {anio_ult}: {ult['pct_importado']:.1f}% importado "
        f"({h.fmt_num(ult['importado'], 1)} ktep) "
        f"vs {(100 - ult['pct_importado']):.1f}% local "
        f"({h.fmt_num(ult['local'], 1)} ktep). "
        f"Total = {h.fmt_num(ult['TOTAL'], 1)} ktep.",
    ]
    if len(df) >= 2:
        prim = df.iloc[0]
        delta = ult["pct_importado"] - prim["pct_importado"]
        tendencia = (
            "↓ menos dependiente" if delta < -1 else
            "↑ más dependiente" if delta > 1 else "↔ estable"
        )
        lines.append(
            f"  - {int(prim['anio'])}: {prim['pct_importado']:.1f}% importado "
            f"→ {anio_ult}: {ult['pct_importado']:.1f}% importado "
            f"({delta:+.1f} pp, {tendencia})."
        )

    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(
        "Definición: 'importado' = electricidad importada (EE_i) + gas "
        "natural (GN) + petróleo y derivados (P_D) + carbón y coque (C_C). "
        "'Local' = el resto (renovables locales + biomasa + residuos "
        "industriales)."
    )
    lines.append(h.definiciones_relevantes("ktep", "clasificacion_por_origen", "importacion"))
    lines.append("")
    lines.append(h.SOURCE_FOOTER)

    table = [["Año", "Importado (ktep)", "Local (ktep)", "Total (ktep)", "% Importado"]]
    for _, row in df.iterrows():
        table.append([
            str(int(row["anio"])),
            h.fmt_num(row["importado"], 1),
            h.fmt_num(row["local"], 1),
            h.fmt_num(row["TOTAL"], 1),
            f"{row['pct_importado']:.1f}%",
        ])

    if len(df) == 1:
        chart = h.pie_chart(
            f"Importado vs producción local - {anio_ult}",
            [
                ("Importado", float(ult["importado"])),
                ("Local", float(ult["local"])),
            ],
            palette=h.COLORES_BEN,
        )
    else:
        chart = h.stacked_bar_chart(
            f"Abastecimiento Uruguay: importado vs local ({rango}), ktep",
            df["anio"].tolist(),
            [
                ("Local", df["local"].tolist()),
                ("Importado", df["importado"].tolist()),
            ],
            palette=h.COLORES_BEN,
        )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])


# ═══ Pérdidas de transformación ═══════════════════════════════════════════

def perdidas_transformacion(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Diferencia entre oferta primaria y consumo final = pérdidas + autoconsumo."""
    abast = h.load_dataset("abastecimiento")
    cons = h.load_dataset("consumo_fuente")
    abast = h.filter_years(abast, anio_desde, anio_hasta)
    cons = h.filter_years(cons, anio_desde, anio_hasta)
    src = [
        h.DATASET_PAGES["abastecimiento"],
        h.DATASET_PAGES["consumo_fuente"],
    ]
    if abast.empty or cons.empty:
        return h.empty_result("para calcular pérdidas en ese rango", src)

    merged = pd.merge(
        abast[["anio", "TOTAL"]].rename(columns={"TOTAL": "oferta"}),
        cons[["anio", "TOTAL"]].rename(columns={"TOTAL": "consumo"}),
        on="anio",
    ).sort_values("anio").reset_index(drop=True)

    if merged.empty:
        return h.empty_result("para calcular pérdidas en ese rango", src)

    merged["perdidas"] = merged["oferta"] - merged["consumo"]
    merged["pct_perdidas"] = (merged["perdidas"] / merged["oferta"] * 100).round(2)

    rango = (
        f"{int(merged['anio'].iloc[0])}" if len(merged) == 1
        else f"{int(merged['anio'].min())}-{int(merged['anio'].max())}"
    )
    ult = merged.iloc[-1]
    anio_ult = int(ult["anio"])

    lines = [
        f"Pérdidas de transformación + autoconsumo del sector energético, {rango}.",
        "",
        f"  - {anio_ult}: oferta primaria {h.fmt_num(ult['oferta'], 1)} ktep, "
        f"consumo final {h.fmt_num(ult['consumo'], 1)} ktep, "
        f"pérdidas {h.fmt_num(ult['perdidas'], 1)} ktep "
        f"({ult['pct_perdidas']:.1f}%).",
        "",
        "Definición: 'pérdidas' incluyen pérdidas físicas en transporte y "
        "distribución, autoconsumo del sector energético (refinería, "
        "centrales, etc.) y pérdidas de transformación (de calor a "
        "electricidad). No es desperdicio puro: una buena parte es "
        "termodinámica.",
    ]
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)

    lines.append(h.definiciones_relevantes(
        "ktep", "perdidas", "centro_de_transformacion", "energia_neta",
    ))
    lines.append("")
    lines.append(h.SOURCE_FOOTER)

    table = [["Año", "Oferta (ktep)", "Consumo (ktep)", "Pérdidas (ktep)", "% Pérdidas"]]
    for _, row in merged.iterrows():
        table.append([
            str(int(row["anio"])),
            h.fmt_num(row["oferta"], 1),
            h.fmt_num(row["consumo"], 1),
            h.fmt_num(row["perdidas"], 1),
            f"{row['pct_perdidas']:.1f}%",
        ])

    if len(merged) == 1:
        chart = h.grouped_bar_chart(
            f"Oferta vs consumo final - {anio_ult} (ktep)",
            [anio_ult],
            [
                ("Oferta primaria", [float(ult["oferta"])]),
                ("Consumo final", [float(ult["consumo"])]),
                ("Pérdidas + autoconsumo", [float(ult["perdidas"])]),
            ],
            palette=h.COLORES_BEN,
        )
    else:
        chart = h.line_chart(
            f"Oferta vs consumo vs pérdidas Uruguay ({rango}), ktep",
            merged["anio"].tolist(),
            [
                ("Oferta primaria", merged["oferta"].tolist()),
                ("Consumo final", merged["consumo"].tolist()),
                ("Pérdidas + autoconsumo", merged["perdidas"].tolist()),
            ],
            palette=h.COLORES_BEN,
        )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])
