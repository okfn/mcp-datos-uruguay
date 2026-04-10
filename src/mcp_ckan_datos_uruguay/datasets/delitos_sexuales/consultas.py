"""
Consultas analíticas sobre datos de delitos sexuales en Uruguay (2018-2024).

Complementa las tools YAML con análisis que requieren lógica Python:
  - Tendencia anual (extracción de año desde fechas)
  - Ranking de departamentos (conteo ordenado con porcentajes)

Las consultas básicas (listar, contar, valores únicos) están en los YAML.

Fuente: Ministerio del Interior - catalogodatos.gub.uy
"""

import json
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"

_eventos_cache = None


def _load_eventos():
    global _eventos_cache
    if _eventos_cache is not None:
        return _eventos_cache
    df = pd.read_csv(DATA_DIR / "eventos-de-delitos-sexuales-2018-2024.csv")
    df["Ingreso"] = pd.to_datetime(df["Ingreso"])
    df["Anio"] = df["Ingreso"].dt.year
    _eventos_cache = df
    return df


def _filtrar(df, anio=None, departamento=None, tipo_delito=None):
    """Aplica filtros comunes y devuelve (df_filtrado, label)."""
    labels = []
    if anio:
        df = df[df["Anio"] == anio]
        labels.append(f"en {anio}")
    if departamento:
        dep_upper = departamento.upper()
        df = df[df["Departamento"].str.upper() == dep_upper]
        labels.append(f"en {departamento.title()}")
    if tipo_delito:
        tipo_upper = tipo_delito.upper()
        df = df[df["Título"].str.upper() == tipo_upper]
        labels.append(f"de tipo {tipo_delito.title()}")
    label = " ".join(labels)
    return df, label


def tendencia_anual(departamento=None, tipo_delito=None):
    """Tendencia anual de eventos de delitos sexuales."""
    df = _load_eventos()
    df, label = _filtrar(df, departamento=departamento, tipo_delito=tipo_delito)

    if df.empty:
        return f"No se encontraron eventos {label}."

    por_anio = df.groupby("Anio").size().sort_index()
    total = por_anio.sum()
    lines = [f"Tendencia anual de delitos sexuales {label} ({total} eventos totales):"]

    # Texto legible
    table_rows = [["Año", "Eventos"]]
    max_val = por_anio.max()
    for anio, count in por_anio.items():
        bar_len = int(count / max_val * 20)
        bar = "█" * bar_len
        lines.append(f"  {anio}: {count:>5}  {bar}")
        table_rows.append([str(anio), str(count)])

    lines.append(
        "\nFuente: Ministerio del Interior, Uruguay "
        "(catalogodatos.gub.uy)"
    )

    # Tabla estructurada
    table = f"<table>{json.dumps(table_rows, ensure_ascii=False)}</table>"

    # Gráfico de línea (tendencia)
    chart_data = json.dumps({
        "type": "line",
        "title": f"Tendencia anual de delitos sexuales {label}".strip(),
        "labels": [str(a) for a in por_anio.index],
        "datasets": [{
            "label": "Eventos",
            "data": [int(v) for v in por_anio.values],
        }],
    }, ensure_ascii=False)
    chart = f"<chart>{chart_data}</chart>"

    # Gráfico de barras por tipo de delito si no se filtró por tipo
    chart_tipo = ""
    if not tipo_delito:
        df_full = _load_eventos()
        df_full, _ = _filtrar(df_full, departamento=departamento)
        if not df_full.empty:
            pivot = df_full.groupby(["Anio", "Título"]).size().unstack(fill_value=0)
            datasets = []
            for tipo in pivot.columns:
                datasets.append({
                    "label": tipo,
                    "data": [int(pivot.loc[a, tipo]) if a in pivot.index else 0
                             for a in por_anio.index],
                })
            chart_tipo_data = json.dumps({
                "type": "bar",
                "stacked": True,
                "title": f"Delitos sexuales por tipo y año {label}".strip(),
                "labels": [str(a) for a in por_anio.index],
                "datasets": datasets,
            }, ensure_ascii=False)
            chart_tipo = f"<chart>{chart_tipo_data}</chart>"

    return "\n".join(lines) + table + chart + chart_tipo


def eventos_por_departamento(anio=None, tipo_delito=None):
    """Ranking de departamentos por cantidad de eventos."""
    df = _load_eventos()
    df, label = _filtrar(df, anio=anio, tipo_delito=tipo_delito)

    if df.empty:
        return f"No se encontraron eventos {label}."

    total = len(df)
    por_depto = df["Departamento"].value_counts()
    lines = [f"Ranking de departamentos por eventos de delitos sexuales {label} "
             f"({total} eventos totales):"]

    # Texto legible + tabla
    table_rows = [["#", "Departamento", "Eventos", "%"]]
    for i, (depto, count) in enumerate(por_depto.items(), 1):
        pct = count / total * 100
        lines.append(f"  {i:>2}. {depto}: {count} ({pct:.1f}%)")
        table_rows.append([str(i), depto, str(count), f"{pct:.1f}%"])

    lines.append(
        "\nFuente: Ministerio del Interior, Uruguay "
        "(catalogodatos.gub.uy)"
    )

    # Tabla estructurada
    table = f"<table>{json.dumps(table_rows, ensure_ascii=False)}</table>"

    # Gráfico de barras horizontal (top 10)
    top10 = por_depto.head(10)
    chart_data = json.dumps({
        "type": "bar",
        "title": f"Top 10 departamentos - delitos sexuales {label}".strip(),
        "labels": list(top10.index),
        "datasets": [{
            "label": "Eventos",
            "data": [int(v) for v in top10.values],
        }],
    }, ensure_ascii=False)
    chart = f"<chart>{chart_data}</chart>"

    return "\n".join(lines) + table + chart
