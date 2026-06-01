# BEN - Factor de emisión de CO2 del SIN

**Página del dataset:** https://catalogodatos.gub.uy/dataset/miem-ben-factor-de-emision-de-co2-del-sin
**CSV directo:** https://catalogodatos.gub.uy/dataset/10967adb-2b87-4d25-b8e3-7accbcc917f5/resource/79af11a5-a11d-49e8-8e56-b54b5539af0c/download/factor_emision-co2-sin.csv
**Archivo local:** `factor_emision-co2-sin.csv` · **Metadata:** `metadatos-factor_emision-co2-sin.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** 1965-2024 (anual)

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/10967adb-2b87-4d25-b8e3-7accbcc917f5/resource/79af11a5-a11d-49e8-8e56-b54b5539af0c/download/factor_emision-co2-sin.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Indicador clave de la transición energética eléctrica: cuánto CO2 emite el
Sistema Interconectado Nacional (SIN) por GWh entregado. Se obtiene
dividiendo las emisiones de las centrales de servicio público por la
energía eléctrica entregada al SIN.

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `Em_CE_SP` | Gg CO2 | Emisiones de CO2 de centrales eléctricas de servicio público |
| `EE_CE_SP` | GWh | Energía eléctrica entregada al SIN por centrales de servicio público |
| `FE_SIN` | t CO2/GWh | Factor de emisión = Em_CE_SP × 1000 / EE_CE_SP |

## Head (primeras 10 líneas)
```
AÑO;Em_CE_SP;EE_CE_SP;FE_SIN
1965;1009.136;1648.837;612.029
1966;475.488;1839.535;258.483
1967;597.602;1903.488;313.951
1968;733.472;1883.721;389.374
1969;930.645;2024.419;459.71
1970;850.127;2132.558;398.642
1971;741.621;2287.209;324.247
1972;1140.337;2306.977;494.299
1973;806.201;2430.233;331.738
```
2023 = 56 t CO2/GWh, 2024 = 6.3 t CO2/GWh - reflejo de un año hidráulico
muy favorable. Los picos altos (≈100+) corresponden a años secos donde la
térmica fósil tuvo que cubrir el déficit hidro.

## Preguntas que ayuda a responder
- **2. Matriz energética / transición** - el FE_SIN es la métrica única
  que resume la "limpieza" del sistema eléctrico.
- **6. Tendencias de transición energética** - caída sostenida del FE_SIN
  desde ~2010 con la entrada de eólica.

## Notas
- Volatilidad alta: depende del régimen hidrológico anual (Uruguay tiene
  alta penetración hidro).
- No incluye autoproducción industrial ni térmica fuera de servicio
  público.
