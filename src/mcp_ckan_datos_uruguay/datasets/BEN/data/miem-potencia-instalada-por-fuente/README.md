# BEN - Potencia instalada por fuente

**Página del dataset:** https://catalogodatos.gub.uy/dataset/miem-potencia-instalada-por-fuente
**CSV directo:** https://catalogodatos.gub.uy/dataset/a3254965-7f67-4c91-b52d-7c50d2192f40/resource/1306f14e-fdf8-4e26-82c6-7e12319ee0ec/download/potencia-instalada-por-fuente.csv
**Archivo local:** `potencia-instalada-por-fuente.csv` · **Metadata:** `metadatos-potencia-instalada-por-fuente.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** 1965-2024 (años con datos: 1990+)

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/a3254965-7f67-4c91-b52d-7c50d2192f40/resource/1306f14e-fdf8-4e26-82c6-7e12319ee0ec/download/potencia-instalada-por-fuente.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Potencia eléctrica instalada (capacidad) en MW al cierre de cada año,
abierta por tecnología y fuente energética. **Diferencia clave** vs
generación: aquí se mide **capacidad** (instalada físicamente),
no producción. Las filas 1965-1989 vienen vacías; la serie con datos
empieza en 1990.

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `TCR_F` | MW | Turbinas Ciclo Rankine (vapor) - fósil |
| `TCB_F` | MW | Turbinas Ciclo Brayton (gas) - fósil |
| `M_F` | MW | Motores - fósil |
| `TOTAL_F` | MW | **Total fósil** = TCR_F + TCB_F + M_F |
| `TCR_B` | MW | Turbinas Ciclo Rankine - biomasa |
| `M_B` | MW | Motores - biomasa |
| `TOTAL_B` | MW | **Total biomasa** = TCR_B + M_B |
| `TOTAL_H` | MW | Total hidráulica |
| `TOTAL_Eo` | MW | Total eólica |
| `TOTAL_S` | MW | Total solar fotovoltaica |
| `TOTAL` | MW | Capacidad total instalada |

## Head (primeras 10 líneas)
```
AÑO;TCR_F;TCB_F;M_F;TOTAL_F;TCR_B;M_B;TOTAL_B;TOTAL_H;TOTAL_Eo;TOTAL_S;TOTAL
1965;;;;;;;;;;;
1966;;;;;;;;;;;
1967;;;;;;;;;;;
...
2020;180.0;925.7;70.96;1176.66;414.5;1.67;416.17;1538.0;1516.4875;257.8752088;4905.1927088
2021;180.0;925.7;70.96;1176.66;414.5;1.82;416.32;1538.0;1516.4875;266.4640138;4913.9315138
2022;180.0;925.7;70.96;1176.66;414.5;2.62;417.12;1538.0;1516.4875;280.4791738;4928.7466738
2023;180.0;925.7;70.96;1176.66;728.5;2.62;731.12;1538.0;1516.4875;300.7188748;5262.9863748
2024;180.0;925.7;70.96;1176.66;728.5;2.62;731.12;1538.0;1516.4875;336.3258828;5298.5933828
```
(filas 1965-1989 totalmente vacías - los datos reales empiezan en 1990).

2024: TOTAL = 5298.6 MW. Mix: Hidro 1538, Eólica 1516, Solar 336, Fósil
1177 (Rankine 180 + Brayton 926 + Motores 71), Biomasa 731.

## Preguntas que ayuda a responder
- **2. Matriz energética** - diversificación de tecnologías, dependencia
  histórica de hidro vs nuevas renovables.
- **6. Transición energética** - fechas de incorporación de cada
  tecnología (eólica masiva 2014-2017, solar 2014+).

## Notas
- **Cobertura útil real: 1990-2024.** Para series largas de generación
  ver `miem-generacion-de-electricidad-por-fuente` (2002+).
- Capacidad ≠ generación. Una planta hidráulica puede tener gran
  potencia pero generar poco en años secos. Para cuestiones de
  utilización efectiva combinar este dataset con generación.
