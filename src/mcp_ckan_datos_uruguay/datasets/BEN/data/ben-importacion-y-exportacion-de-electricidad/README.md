# BEN - Importación y exportación de electricidad

**Página del dataset:** https://catalogodatos.gub.uy/dataset/ben-importacion-y-exportacion-de-electricidad
**CSV directo:** https://catalogodatos.gub.uy/dataset/b0ab3d1f-4d00-490b-86fc-727238a405cc/resource/dbec6e8f-0202-46dc-8bf5-64ef4018cab9/download/impo_y_expo_electricidad.csv
**Archivo local:** `impo_y_expo_electricidad.csv` · **Metadata:** `metadatos-impo_y_expo_electricidad.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** 1965-2024 (anual)

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/b0ab3d1f-4d00-490b-86fc-727238a405cc/resource/dbec6e8f-0202-46dc-8bf5-64ef4018cab9/download/impo_y_expo_electricidad.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Intercambio anual de electricidad de Uruguay con países vecinos (Argentina,
Brasil), separado en importaciones y exportaciones. Un valor reportado
como `0` corresponde a un valor pequeño; un campo vacío significa que no
existió intercambio en ese año.

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `impo_electricidad` | ktep | Electricidad importada |
| `expo_electricidad` | ktep | Electricidad exportada |

## Head (primeras 10 líneas)
```
AÑO;impo_electricidad;expo_electricidad
1965;0.1;0.1
1966;0.2;0
1967;0.1;0
1968;0.3;0.1
1969;2.0;
1970;2.4;
1971;3.0;
1972;2.9;
1973;2.3;
```
Últimos años: 2023 = 120.2 / 21 ; 2024 = 0.6 / 174.3 (Uruguay exportador
neto en años secos invertidos / húmedos). Atención: signos no negativos -
las dos columnas son magnitudes positivas.

## Preguntas que ayuda a responder
- **3. Dependencia externa** - saldo importador/exportador eléctrico.
- **2. Matriz energética** - Uruguay como exportador eléctrico es
  consecuencia del crecimiento de renovables (cruzar con generación).
