# BEN - Abastecimiento de energía por fuente

**Página del dataset:** https://catalogodatos.gub.uy/dataset/miem-abastecimiento-de-energia-por-fuente
**CSV directo:** https://catalogodatos.gub.uy/dataset/fc5e7e9c-73cc-4c10-a3b7-6c27848bc095/resource/eb3fadb3-a0b5-4324-8742-5355632f9b24/download/abastecimiento-de-energia-por-fuente.csv
**Archivo local:** `abastecimiento-de-energia-por-fuente.csv` · **Metadata:** `metadatos-abastecimiento-de-energia-por-fuente.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** 1965-2024 (anual)

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/fc5e7e9c-73cc-4c10-a3b7-6c27848bc095/resource/eb3fadb3-a0b5-4324-8742-5355632f9b24/download/abastecimiento-de-energia-por-fuente.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Oferta primaria total de energía del país desagregada por fuente. Cada
columna es un agregado de las fuentes que ingresan al sistema energético
nacional (importación + producción local). Es la **matriz energética
primaria** de Uruguay. Todas las cantidades en ktep para que sean
agregables.

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `EE_i` | ktep | Energía eléctrica importada |
| `EE_H` | ktep | Electricidad de origen hidráulico |
| `EE_Eo` | ktep | Electricidad de origen eólico |
| `S` | ktep | Solar (térmica + fotovoltaica) |
| `GN` | ktep | Gas natural (importado) |
| `P_D` | ktep | Petróleo + saldo neto comercio exterior de derivados |
| `C_C` | ktep | Carbón mineral + coque (importado) |
| `B` | ktep | Biomasa (leña, residuos, biocombustibles, carbón vegetal) |
| `RI` | ktep | Residuos industriales |
| `TOTAL` | ktep | Suma - abastecimiento total |

## Head (primeras 10 líneas)
```
AÑO;EE_i;EE_H;EE_Eo;S;GN;P_D;C_C;B;RI;TOTAL
1965;0.1;52.5;;;;1767.5;34.8;372.7;;2227.6
1966;0.2;120.3;;;;1696.2;31.8;372.6;;2221.1
1967;0.1;113.4;;;;1547.1;32.0;371.5;;2064.1
1968;0.3;98.9;;;;1545.0;29.2;376.2;;2049.6
1969;2.0;91.8;;;;1701.4;23.6;382.7;;2201.5
1970;2.4;106.8;;;;1829.3;28.9;391.2;;2358.6
1971;3.0;126.3;;;;1762.7;23.9;393.4;;2309.3
1972;2.9;85.6;;;;1785.7;25.3;407.0;;2306.5
1973;2.3;133.8;;;;1731.5;24.5;404.1;;2296.2
```
2024: Total = 6842.4 ktep. Mix 2024: hidro 630.5 / eólica 408.6 / solar
52.7 / gas 67.8 / petróleo+derivados 2403.9 / carbón 1.5 / biomasa 3267.7
/ residuos ind. 9.1.

## Preguntas que ayuda a responder
- **1. Visión macro** - fuentes que predominan en la matriz energética.
- **2. Matriz energética** - % renovables vs no renovables, evolución de la
  participación, diversificación.
- **3. Dependencia externa** - combinar `EE_i + GN + P_D + C_C` (todo
  importado) versus producción local (`EE_H + EE_Eo + S + B`).
- **6. Eficiencia / transformación** - comparar `TOTAL` (oferta primaria)
  con consumo final (`miem-consumo-final-energetico-por-fuente.TOTAL`); la
  diferencia son pérdidas de transformación, distribución y consumo propio
  del sector energético.

## Cruces clave
- Renovable% por año = `(EE_H + EE_Eo + S + B) / TOTAL`.
- Importado% por año = `(EE_i + GN + P_D + C_C) / TOTAL` (residuos
  industriales son locales; biomasa es local salvo importación neta de
  carbón vegetal incluida en `B`).
