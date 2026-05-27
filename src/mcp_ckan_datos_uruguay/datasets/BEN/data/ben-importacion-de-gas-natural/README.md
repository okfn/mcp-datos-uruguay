# BEN - Importación de gas natural

**Página del dataset:** https://catalogodatos.gub.uy/dataset/ben-importacion-de-gas-natural
**CSV directo:** https://catalogodatos.gub.uy/dataset/61631f9e-03ac-4080-a225-a4ddad8da40f/resource/9690c070-d697-40ff-a848-7f9dc98454ab/download/impo_gas_natural.csv
**Archivo local:** `impo_gas_natural.csv` · **Metadata:** `metadatos-impo_gas_natural.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** 1965-2024 (anual, 60 años) · **Filas de datos:** 60

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/61631f9e-03ac-4080-a225-a4ddad8da40f/resource/9690c070-d697-40ff-a848-7f9dc98454ab/download/impo_gas_natural.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Serie anual de importaciones de gas natural a Uruguay, expresada en ktep
(kilo-toneladas equivalentes de petróleo). Forma parte del Balance Energético
Nacional (BEN) publicado por el MIEM. Una celda vacía significa que no hay
dato para ese año (en este caso, todos los años anteriores a 1998 están
vacíos: el gas natural empezó a importarse a partir del cruce con la
Argentina por el gasoducto Cruz del Sur).

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `impo_gas_natural` | ktep | Importación de gas natural |

## Head (primeras 10 líneas)
```
AÑO;impo_gas_natural
1965;
1966;
1967;
1968;
1969;
1970;
1971;
1972;
1973;
...
2017;58.5
2018;55.2
2019;80.8
2020;59.8
2021;68.4
2022;71.9
2023;65.7
2024;67.8
```
Últimos años (referencia 2024 = 67.8 ktep).

## Preguntas que ayuda a responder
- **3. Dependencia externa** - qué tipo de energía se importa, evolución de
  las importaciones de gas natural.
- **2. Matriz energética** - peso del gas natural en la oferta primaria
  (combinar con `miem-abastecimiento-de-energia-por-fuente`).
