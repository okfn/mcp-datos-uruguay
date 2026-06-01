"""
Constructores de gráficos Chart.js para las respuestas de las tools BEN.

`pie_chart`, `stacked_bar_chart`, `grouped_bar_chart`, `line_chart` y un
selector automático (`chart_for_mix`) que elige pie cuando hay 1 año y
stacked-bar cuando hay varios.
"""

import pandas as pd


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
        return pie_chart(f"{title} - {int(row['anio'])}", slices, palette)
    return stacked_bar_chart(
        title,
        df["anio"].tolist(),
        [(label, df[col].tolist()) for col, label in fuentes],
        palette,
    )
