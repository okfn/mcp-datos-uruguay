# BEN - Consumo final energético por sector

**Página del dataset:** https://catalogodatos.gub.uy/dataset/miem-consumo-final-energetico-por-sector
**CSV directo:** https://catalogodatos.gub.uy/dataset/9beed8c2-b881-4e9b-a3d8-9412e48b9554/resource/11142183-96cb-4309-8f90-2b368f8eb99c/download/consumo-final-energetico-por-sector.csv
**Archivo local:** `consumo-final-energetico-por-sector.csv` · **Metadata:** `metadatos-consumo-final-energetico-por-sector.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** 1965-2024 (anual)

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/9beed8c2-b881-4e9b-a3d8-9412e48b9554/resource/11142183-96cb-4309-8f90-2b368f8eb99c/download/consumo-final-energetico-por-sector.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Misma magnitud que `miem-consumo-final-energetico-por-fuente` pero abierta
por **sector consumidor** en lugar de por fuente energética. Permite ver
quién está consumiendo la energía.

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `R` | ktep | Residencial - hogares (no incluye transporte vehicular). En 1965-1979 incluye también Comercial/Servicios/Sector Público |
| `C_S_SP` | ktep | Comercial / Servicios / Sector público (CIIU Rev. 4 secciones D-U excepto F + alumbrado público). Vacío 1965-1979 |
| `T` | ktep | Transporte (aéreo, terrestre, fluvial; individual y colectivo). No incluye búnker internacional ni transporte interno de empresas |
| `I` | ktep | Industrial - manufactura + construcción (CIIU Rev. 4 secciones C y F), incluye agroindustria y pesca industrial |
| `AP` | ktep | Actividades primarias - agropecuaria, forestal, pesca, minería |
| `NI` | ktep | No identificado - consumos sin sector asignable |
| `TOTAL` | ktep | Consumo final energético total |

## Head (primeras 10 líneas)
```
AÑO;R;C_S_SP;T;I;AP;NI;TOTAL
1965;551.8;37.6;518.8;463.5;102.2;7.3;1681.2
1966;553.9;39.7;514.1;458.6;101.5;2.3;1670.1
1967;557.7;39.0;497.4;436.3;90.0;1.3;1621.7
1968;559.0;40.4;486.4;412.1;88.8;2.0;1588.7
1969;584.7;43.8;522.3;446.6;88.3;1.2;1686.9
1970;600.4;53.2;551.9;486.2;78.2;1.2;1771.1
1971;637.2;55.6;572.4;489.6;74.7;11.3;1840.8
1972;634.8;52.4;614.2;493.1;67.8;3.6;1865.9
1973;635.6;54.1;594.7;495.0;68.9;0.0;1848.3
```
2024: TOTAL = 6076.4 ktep. Mix: Industrial 3141.3 (52%), Transporte 1492.6
(25%), Residencial 864.1 (14%), Comercial/Servicios 364.9 (6%), Primarias
184.2 (3%).

## Preguntas que ayuda a responder
- **5. Consumo por sector** - toda la sección. Sector con mayor consumo,
  evolución, sector que más crece.
- **1. Evolución de la demanda** - vía `TOTAL`.

## Notas
- **Quiebre 1980:** entre 1965-1979 el consumo Comercial/Servicios/Sector
  Público está incluido dentro de Residencial; tratar el ratio R/C_S_SP
  con cuidado en series largas.
- El **sub-consumo de transporte** (auto vs ómnibus vs aviación etc.) NO
  está disponible en este dataset - la pregunta del README "¿cómo se
  distribuye el consumo dentro del sector transporte?" **no se puede
  responder con BEN**, requiere otra fuente.
