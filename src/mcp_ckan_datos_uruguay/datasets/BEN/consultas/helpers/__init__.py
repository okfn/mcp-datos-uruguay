"""
Helpers compartidos de las consultas BEN, reunidos en un único namespace.

Cada submódulo agrupa una responsabilidad:
  - `data`        — URLs de datasets, `load_dataset`, `filter_years`.
  - `charts`      — constructores de gráficos Chart.js.
  - `format`      — `fmt_num`, builders de `CallToolResult`, tablas, notas fijas.
  - `units`       — explicación de unidades (`unit_blurb`).
  - `metodologia` — definiciones literales del Libro del BEN (`definiciones_relevantes`).

Este `__init__` los re-exporta para que las tools llamen `h.load_dataset(...)`,
`h.text_result(...)`, `h.unit_blurb(...)`, `h.definiciones_relevantes(...)`, etc.,
sin importar de qué submódulo viene cada función. Los submódulos se importan
entre sí directamente (no desde este paquete), así no hay ciclos de import.
"""

from .data import (
    PORTAL,
    DATASET_PAGES,
    DATASET_URLS,
    load_dataset,
    filter_years,
)
from .units import UNIT_BLURB, unit_blurb
from .charts import (
    PALETTE_DEFAULT,
    pie_chart,
    stacked_bar_chart,
    grouped_bar_chart,
    line_chart,
    chart_for_mix,
)
from .format import (
    ALREADY_TABLE,
    ALREADY_CHART,
    SOURCE_FOOTER,
    fmt_num,
    text_result,
    empty_result,
    mix_breakdown_lines,
    build_table,
)
from .metodologia import (
    DEFINICIONES_BEN,
    CONCEPTOS_DISPONIBLES,
    definiciones_relevantes,
)


__all__ = [
    # data
    "PORTAL", "DATASET_PAGES", "DATASET_URLS", "load_dataset", "filter_years",
    # units
    "UNIT_BLURB", "unit_blurb",
    # charts
    "PALETTE_DEFAULT", "pie_chart", "stacked_bar_chart", "grouped_bar_chart",
    "line_chart", "chart_for_mix",
    # format
    "ALREADY_TABLE", "ALREADY_CHART", "SOURCE_FOOTER", "fmt_num",
    "text_result", "empty_result", "mix_breakdown_lines", "build_table",
    # metodologia
    "DEFINICIONES_BEN", "CONCEPTOS_DISPONIBLES", "definiciones_relevantes",
]
