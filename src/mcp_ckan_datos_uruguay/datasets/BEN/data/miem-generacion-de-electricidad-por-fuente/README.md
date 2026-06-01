# BEN - Generación de electricidad por fuente

**Página del dataset:** https://catalogodatos.gub.uy/dataset/miem-generacion-de-electricidad-por-fuente
**CSV directo:** https://catalogodatos.gub.uy/dataset/91118d5f-11be-4c84-82f0-79370ab7b089/resource/101c92a0-2906-4232-ad8d-34234a5eca17/download/generacion-electricidad-por-fuente.csv
**Archivo local:** `generacion-electricidad-por-fuente.csv` · **Metadata:** `metadatos-generacion-electricidad-por-fuente.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** **2002-2024** (anual, 23 años)

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/91118d5f-11be-4c84-82f0-79370ab7b089/resource/101c92a0-2906-4232-ad8d-34234a5eca17/download/generacion-electricidad-por-fuente.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Generación eléctrica anual por tipo de fuente, **en GWh** (no en ktep).
Incluye toda la generación entregada al SIN (servicio público +
autoproducción industrial). Cobertura más corta que el resto: arranca en
2002.

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `EE_F` | GWh | Eléctrica generada con combustibles fósiles |
| `EE_B` | GWh | Eléctrica generada con biomasa |
| `EE_H` | GWh | Eléctrica generada con energía hidráulica |
| `EE_Eo` | GWh | Eléctrica generada con energía eólica |
| `EE_S` | GWh | Eléctrica generada con energía solar |
| `TOTAL` | GWh | Generación total |

## Head (primeras 10 líneas)
```
AÑO;EE_F;EE_B;EE_H;EE_Eo;EE_S;TOTAL
2002;26.426;0.0;9535.259;;;9561.685
2003;6.604;0.0;8529.549;;;8536.153
2004;1076.798;27.272;4780.749;;;5884.819
2005;956.339;24.456;6683.62;;;7664.415
2006;1971.163;38.611;3545.09;;;5554.864
2007;1224.622;134.054;8021.144;;;9379.82
2008;3388.735;872.418;4500.71;7.291;;8769.154
2009;2634.035;929.909;5059.788;42.098;;8665.83
2010;1165.081;1089.849;8407.154;69.914;;10731.998
```
2024: Total = 17204 GWh. Mix: hidro 7331 (43%), eólica 4751 (28%),
biomasa 4419 (26%), solar 518 (3%), fósil 185 (1%) - año excelente para
renovables.

## Preguntas que ayuda a responder
- **2. Matriz energética eléctrica** - fuente principal de generación,
  fuentes que más crecen, % renovables eléctricas.
- **4. Análisis temporal** - variación inter-anual (sólo anual, no estacional).
- **6. Transición** - entrada de eólica desde 2008, solar desde ~2014,
  caída de fósil.

## Notas
- **Diferencia con `abastecimiento-de-energia-por-fuente`:** allí la
  electricidad está en ktep (energía contenida); aquí en GWh (energía
  efectivamente entregada). No multiplicar por factores de conversión a
  la ligera - usar la columna `EE_CE_SP` del dataset
  `miem-ben-factor-de-emision-de-co2-del-sin` como referencia.
- Sin granularidad mensual / horaria - no se pueden responder preguntas
  de pico horario o estacional.
