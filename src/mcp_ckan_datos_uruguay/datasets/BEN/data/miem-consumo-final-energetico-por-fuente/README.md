# BEN - Consumo final energético por fuente

**Página del dataset:** https://catalogodatos.gub.uy/dataset/miem-consumo-final-energetico-por-fuente
**CSV directo:** https://catalogodatos.gub.uy/dataset/1737fc66-58b7-4286-a246-f1864533975d/resource/6e6af1ef-15ee-434a-ba11-b091641b292d/download/consumo-final-energetico-por-fuente.csv
**Archivo local:** `consumo-final-energetico-por-fuente.csv` · **Metadata:** `metadatos-consumo-final-energetico-por-fuente.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** 1965-2024 (anual)

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/1737fc66-58b7-4286-a246-f1864533975d/resource/6e6af1ef-15ee-434a-ba11-b091641b292d/download/consumo-final-energetico-por-fuente.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Energía efectivamente consumida por los usuarios finales (no incluye lo que
se transforma o se pierde en el sector energético), abierta por la fuente
energética. Es la mitad "demanda" del balance, simétrica al
abastecimiento.

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `CM` | ktep | Carbón mineral (antracita, turba, alquitranes, brea) |
| `GN` | ktep | Gas natural |
| `S` | ktep | Solar térmica |
| `L_CV` | ktep | Leña y carbón vegetal |
| `RB` | ktep | Residuos de biomasa (cáscara de arroz/girasol, bagazo, licor negro, etc.) |
| `RI` | ktep | Residuos industriales (neumáticos, aceites usados, etc.) |
| `DP` | ktep | Derivados del petróleo (GLP, naftas, gasoil, fueloil, queroseno...) - incluye gas manufacturado 1978-2005 |
| `Bc` | ktep | Biocombustibles (bioetanol, biodiésel) |
| `DC` | ktep | Derivados del carbón (coque) - hasta 1977 incluye gas manufacturado |
| `EE` | ktep | Energía eléctrica |
| `TOTAL` | ktep | Consumo final energético total |

## Head (primeras 10 líneas)
```
AÑO;CM;GN;S;L_CV;RB;RI;DP;Bc;DC;EE;TOTAL
1965;5.1;;;355.8;15.1;;1164.1;;22.6;118.5;1681.2
1966;1.4;;;357.7;13.3;;1148.7;;21.0;128.0;1670.1
1967;4.6;;;360.1;10.8;;1091.6;;19.6;135.0;1621.7
1968;1.0;;;364.5;9.9;;1058.7;;20.3;134.3;1588.7
1969;0.4;;;366.7;15.2;;1139.4;;19.4;145.8;1686.9
1970;1.5;;;368.4;17.2;;1209.7;;21.0;153.3;1771.1
1971;1.1;;;371.0;16.2;;1266.4;;18.9;167.2;1840.8
1972;1.5;;;372.5;27.8;;1289.2;;17.6;157.3;1865.9
1973;1.0;;;377.1;20.9;;1271.7;;16.4;161.2;1848.3
```
2024: TOTAL = 6076.4 ktep. Top fuentes: derivados de petróleo 2130.4,
residuos de biomasa 2222.4, electricidad 1136.6, leña/carbón vegetal 471.9.

## Preguntas que ayuda a responder
- **1. Visión macro** - consumo total y su evolución; fuentes
  predominantes del lado demanda.
- **2. Matriz energética (consumo)** - % renovables = `(S + L_CV + RB +
  Bc) / TOTAL` (residuos industriales son recuperación, no estrictamente
  renovables).
- **6. Pérdidas de transformación** - comparar `TOTAL` aquí con
  `miem-abastecimiento-de-energia-por-fuente.TOTAL`. Diferencia 2024 ≈
  6842.4 - 6076.4 = 766 ktep (≈11%) son pérdidas + autoconsumo del sector
  energético.

## Notas
- TOTAL coincide con el TOTAL de `miem-consumo-final-energetico-por-sector`
  (cross-check: 2024 ambos = 6076.4) - son la misma magnitud
  desagregada por dos dimensiones distintas.
