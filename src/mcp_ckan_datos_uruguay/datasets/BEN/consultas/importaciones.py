"""
Tools BEN - Importaciones / intercambio externo de energía.

Cubre tres datasets MIEM:
  - `ben-importacion-de-gas-natural` (ktep, 1965-2024)
  - `ben-importacion-de-petroleo-y-carga-de-refineria` (ktep, 1965-2024)
  - `ben-importacion-y-exportacion-de-electricidad` (ktep, 1965-2024)

Preguntas del README cubiertas:
  - 3.3 tipo de energía importada
  - 3.4 evolución de las importaciones
  - 3.5 vulnerabilidad a shocks externos (vía petróleo)
"""

import pandas as pd

from mcp_server import DataToolOutput
from . import helpers as h


# ═══ Importación de petróleo + carga de refinería ═════════════════════════

def importacion_petroleo(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Importación de petróleo crudo + carga de refinería, en ktep."""
    df = h.load_dataset("petroleo")
    df = h.filter_years(df, anio_desde, anio_hasta)
    src = [h.DATASET_PAGES["petroleo"]]
    if df.empty:
        return h.empty_result("de importación de petróleo en ese rango", src)

    rango = (
        f"{int(df['anio'].iloc[0])}" if len(df) == 1
        else f"{int(df['anio'].min())}-{int(df['anio'].max())}"
    )
    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])

    lines = [
        f"Importación de petróleo crudo y carga de refinería, "
        f"{rango} (ktep).",
        "",
        f"  - {anio_ult}: importación = "
        f"{h.fmt_num(ult['impo_petroleo'], 1)} ktep, "
        f"carga refinería = {h.fmt_num(ult['carga_refineria'], 1)} ktep.",
    ]
    if len(df) >= 2:
        prim = df.iloc[0]
        if pd.notna(prim["impo_petroleo"]) and float(prim["impo_petroleo"]) > 0:
            cagr = (
                (float(ult["impo_petroleo"]) / float(prim["impo_petroleo"]))
                ** (1 / (int(ult["anio"]) - int(prim["anio"]))) - 1
            ) * 100
            lines.append(
                f"  - Crecimiento anual {int(prim['anio'])}-{anio_ult}: "
                f"{cagr:+.2f}% / año (importación)."
            )

    lines.append("")
    lines.append(
        "Lectura: la diferencia entre importación y carga refleja "
        "movimientos de stock (acopio o desacopio de crudo)."
    )
    lines.append(h.definiciones_relevantes("ktep", "importacion"))
    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(h.SOURCE_FOOTER)

    table = [["Año", "Importación (ktep)", "Carga refinería (ktep)"]]
    for _, row in df.iterrows():
        table.append([
            str(int(row["anio"])),
            h.fmt_num(row["impo_petroleo"], 1),
            h.fmt_num(row["carga_refineria"], 1),
        ])

    if len(df) == 1:
        chart = h.grouped_bar_chart(
            f"Importación de petróleo y carga refinería - {anio_ult} (ktep)",
            [anio_ult],
            [
                ("Importación", [float(ult["impo_petroleo"])]),
                ("Carga refinería", [float(ult["carga_refineria"])]),
            ],
            palette=h.COLORES_BEN,
        )
    else:
        chart = h.line_chart(
            f"Petróleo: importación vs carga refinería Uruguay ({rango}), ktep",
            df["anio"].tolist(),
            [
                ("Importación", df["impo_petroleo"].tolist()),
                ("Carga refinería", df["carga_refineria"].tolist()),
            ],
            palette=h.COLORES_BEN,
        )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])


# ═══ Importación de gas natural ═══════════════════════════════════════════

def importacion_gas_natural(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Serie anual de importaciones de gas natural, en ktep."""
    df = h.load_dataset("gas_natural")
    df = h.filter_years(df, anio_desde, anio_hasta)
    df = df[df["impo_gas_natural"].notna()].reset_index(drop=True)
    src = [h.DATASET_PAGES["gas_natural"]]
    if df.empty:
        return h.empty_result(
            "de importación de gas natural en ese rango (la serie tiene "
            "datos desde 1998)",
            src,
        )

    rango = (
        f"{int(df['anio'].iloc[0])}" if len(df) == 1
        else f"{int(df['anio'].min())}-{int(df['anio'].max())}"
    )
    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])
    media = df["impo_gas_natural"].mean()
    lines = [
        f"Importación de gas natural a Uruguay, {rango} (ktep).",
        "",
        f"  - {anio_ult}: {h.fmt_num(ult['impo_gas_natural'], 1)} ktep.",
        f"  - Promedio del período: {h.fmt_num(media, 1)} ktep.",
        "",
        "Contexto: volúmenes modestos comparados con petróleo y biomasa; "
        "la serie tiene datos desde 1998.",
    ]
    lines.append(h.definiciones_relevantes("ktep", "gas_natural", "importacion"))
    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(h.SOURCE_FOOTER)

    table = [["Año", "Importación gas natural (ktep)"]]
    for _, row in df.iterrows():
        table.append([
            str(int(row["anio"])),
            h.fmt_num(row["impo_gas_natural"], 1),
        ])

    if len(df) == 1:
        chart = h.grouped_bar_chart(
            f"Importación de gas natural - {anio_ult} (ktep)",
            [anio_ult],
            [("Importación gas natural", [float(ult["impo_gas_natural"])])],
            palette=h.COLORES_BEN,
        )
    else:
        chart = h.line_chart(
            f"Importación de gas natural Uruguay ({rango}), ktep",
            df["anio"].tolist(),
            [("Importación gas natural", df["impo_gas_natural"].tolist())],
            palette=h.COLORES_BEN,
        )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])


# ═══ Importación / exportación de electricidad ═══════════════════════════

def intercambio_electricidad(anio_desde=None, anio_hasta=None) -> DataToolOutput:
    """Importación, exportación y saldo neto de electricidad."""
    df = h.load_dataset("impo_expo_electricidad")
    df = h.filter_years(df, anio_desde, anio_hasta)
    src = [h.DATASET_PAGES["impo_expo_electricidad"]]
    if df.empty:
        return h.empty_result("de intercambio eléctrico en ese rango", src)

    df = df.copy()
    df["impo_electricidad"] = df["impo_electricidad"].fillna(0)
    df["expo_electricidad"] = df["expo_electricidad"].fillna(0)
    df["saldo_neto"] = df["expo_electricidad"] - df["impo_electricidad"]

    rango = (
        f"{int(df['anio'].iloc[0])}" if len(df) == 1
        else f"{int(df['anio'].min())}-{int(df['anio'].max())}"
    )
    ult = df.iloc[-1]
    anio_ult = int(ult["anio"])
    posicion = (
        "exportador neto" if ult["saldo_neto"] > 0
        else "importador neto" if ult["saldo_neto"] < 0
        else "balanceado"
    )

    lines = [
        f"Intercambio eléctrico de Uruguay (importación, exportación y "
        f"saldo neto), {rango} (ktep).",
        "",
        f"  - {anio_ult}: importación = {h.fmt_num(ult['impo_electricidad'], 1)} "
        f"ktep, exportación = {h.fmt_num(ult['expo_electricidad'], 1)} "
        f"ktep, saldo neto = {h.fmt_num(ult['saldo_neto'], 1)} ktep "
        f"({posicion}).",
    ]
    if len(df) >= 2:
        n_export = (df["saldo_neto"] > 0).sum()
        n_import = (df["saldo_neto"] < 0).sum()
        lines.append(
            f"  - En el rango {rango}: {n_export} año(s) exportador neto, "
            f"{n_import} año(s) importador neto."
        )

    lines.append("")
    lines.append(
        "Contexto: con la entrada masiva de eólica desde 2014, Uruguay "
        "pasó de importador estructural a exportador neto en años "
        "hidráulicamente buenos. Las dos columnas son magnitudes "
        "**positivas** (no usar signo): el saldo se calcula como "
        "exportación - importación."
    )
    lines.append(h.definiciones_relevantes("ktep", "exportacion", "importacion"))
    lines.append("")
    lines.append(h.ALREADY_TABLE)
    lines.append(h.ALREADY_CHART)
    lines.append(h.SOURCE_FOOTER)

    table = [["Año", "Importación (ktep)", "Exportación (ktep)", "Saldo neto (ktep)"]]
    for _, row in df.iterrows():
        table.append([
            str(int(row["anio"])),
            h.fmt_num(row["impo_electricidad"], 1),
            h.fmt_num(row["expo_electricidad"], 1),
            h.fmt_num(row["saldo_neto"], 1),
        ])

    if len(df) == 1:
        chart = h.grouped_bar_chart(
            f"Intercambio eléctrico Uruguay - {anio_ult} (ktep)",
            [anio_ult],
            [
                ("Importación", [float(ult["impo_electricidad"])]),
                ("Exportación", [float(ult["expo_electricidad"])]),
            ],
            palette=h.COLORES_BEN,
        )
    else:
        chart = h.grouped_bar_chart(
            f"Intercambio eléctrico Uruguay ({rango}), ktep",
            df["anio"].tolist(),
            [
                ("Importación", df["impo_electricidad"].tolist()),
                ("Exportación", df["expo_electricidad"].tolist()),
            ],
            palette=h.COLORES_BEN,
        )

    return h.text_result("\n".join(lines), src, table=table, charts=[chart])
