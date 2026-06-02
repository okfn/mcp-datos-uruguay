# BEN - Importación de petróleo y carga de refinería

**Página del dataset:** https://catalogodatos.gub.uy/dataset/ben-importacion-de-petroleo-y-carga-de-refineria
**CSV directo:** https://catalogodatos.gub.uy/dataset/db244448-da5e-4dff-8bca-8163ed9c8d6d/resource/7efe24ee-b7ee-45fb-8b97-5518b02930fe/download/impo_y_carga_refineria_petroleo.csv
**Archivo local:** `impo_y_carga_refineria_petroleo.csv` · **Metadata:** `metadatos-impo-y-carga-refineria-petroleo.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** 1965-2024 (anual)

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/db244448-da5e-4dff-8bca-8163ed9c8d6d/resource/7efe24ee-b7ee-45fb-8b97-5518b02930fe/download/impo_y_carga_refineria_petroleo.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Serie anual con dos magnitudes complementarias:
1. Volumen de petróleo crudo importado al país.
2. Carga de la refinería - petróleo efectivamente procesado.
Ambas en ktep. La diferencia entre importación y carga refleja stocks y
movimientos de inventario.

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `impo_petroleo` | ktep | Importación de petróleo crudo |
| `carga_refineria` | ktep | Petróleo crudo cargado en refinería |

## Head (primeras 10 líneas)
```
AÑO;impo_petroleo;carga_refineria
1965;1712.9;1607.8
1966;1765.3;1721.2
1967;1582.2;1584.6
1968;1417.0;1361.7
1969;1645.3;1615.2
1970;1769.3;1763.7
1971;1777.8;1747.8
1972;1581.9;1694.2
1973;1751.3;1720.3
```
Últimos años: 2023 = 1354.3 / 1474.8 ; 2024 = 1447.9 / 1338.0.

## Preguntas que ayuda a responder
- **3. Dependencia externa y seguridad energética** - toda la sección.
- **6. Vulnerabilidad ante shock del precio del petróleo** - input clave.
