# Delitos Sexuales en Uruguay (2018-2024)

Fuente: Ministerio del Interior, Uruguay - [catalogodatos.gub.uy](https://catalogodatos.gub.uy)

## Datasets

- `data/eventos-de-delitos-sexuales-2018-2024.csv` - Eventos (denuncias) registrados
- `data/victimas-de-delitos-sexuales-2018-2024.csv` - Víctimas registradas
- `data/metadatos-eventos-de-delitos-sexuales-2018-2024.json` - Metadatos del dataset

## Preparacion de datos

Los CSV originales del portal estan codificados en ISO-8859-1 (latin-1).
Se convirtieron a UTF-8 para compatibilidad con pandas (default encoding).

```python

import pandas as pd
for f in [
    'victimas-de-delitos-sexuales-2018-2024.csv',
    'eventos-de-delitos-sexuales-2018-2024.csv'
]:
    path = f'src/mcp_ckan_datos_uruguay/datasets/delitos_sexuales/data/{f}'
    df = pd.read_csv(path, encoding='latin-1')
    df.to_csv(path, index=False, encoding='utf-8')
    print(f'Converted {f} to UTF-8')
```

## Tools MCP declarativas (YAML + engines)

Estas tools se definen declarativamente en archivos YAML y son procesadas
por los engines de [mcp-ckan](https://github.com/okfn/mcp-ckan).

| YAML | Engine | Tool | Descripcion |
|------|--------|------|-------------|
| `eventos_delitos_sexuales.yaml` | row_list | `eventos_delitos_sexuales_uy` | Lista denuncias con fecha, delito, jurisdiccion y departamento |
| `victimas_delitos_sexuales.yaml` | row_list | `victimas_delitos_sexuales_uy` | Lista victimas con sexo, nacimiento, fecha, delito y departamento |
| `total_eventos_delitos_sexuales.yaml` | aggregate | `total_eventos_delitos_sexuales_uy` | Cantidad total de denuncias |
| `total_victimas_delitos_sexuales.yaml` | aggregate | `total_victimas_delitos_sexuales_uy` | Cantidad total de victimas |
| `departamentos_delitos_sexuales.yaml` | unique_values | `departamentos_con_delitos_sexuales_uy` | Departamentos con denuncias |
| `tipos_delitos_sexuales.yaml` | unique_values | `tipos_delitos_sexuales_uy` | Tipos de delitos registrados |

## Tools MCP programaticas (Python)

Complementan las tools YAML con analisis que requieren logica Python
(extraccion de año desde fechas, conteos ordenados con porcentajes).
Se definen en `consultas.py` y se registran en `__init__.py`.

| Tool | Descripcion | Por que no YAML |
|------|-------------|-----------------|
| `tendencia_anual_delitos_sexuales_uy` | Tendencia anual con grafico de barras de texto | Requiere extraer año de la fecha |
| `ranking_departamentos_delitos_sexuales_uy` | Ranking de departamentos con conteo y porcentaje | Requiere value_counts ordenado con % |

## Filtros disponibles

**YAML tools:**
- `departamento`: MONTEVIDEO, CANELONES, MALDONADO, etc.
- `tipo_delito`: ABUSO SEXUAL, VIOLACION, ATENTADO VIOLENTO AL PUDOR
- `sexo` (solo victimas): MUJER, VARON

**Python tools:**
- `departamento`: Montevideo, Canelones, Maldonado, etc.
- `tipo_delito`: Abuso Sexual, Violacion, Atentado Violento al Pudor
- `anio`: 2018-2024
