# BEN - Emisiones de CO2 por sector

**Página del dataset:** https://catalogodatos.gub.uy/dataset/miem-emisiones-de-co2-por-sector
**CSV directo:** https://catalogodatos.gub.uy/dataset/fded3eeb-2904-43a5-a348-0b31434ef085/resource/0276c0ec-a444-4221-8354-b4909b5bd2d6/download/emisiones-de-co2-por-sector.csv
**Archivo local:** `emisiones-de-co2-por-sector.csv` · **Metadata:** `metadatos-emisiones-de-co2-por-sector.json`
**Encoding:** ISO-8859-1 · **Separador:** `;` · **Cobertura:** 1965-2024 (anual)

```python
import pandas as pd
URL = "https://catalogodatos.gub.uy/dataset/fded3eeb-2904-43a5-a348-0b31434ef085/resource/0276c0ec-a444-4221-8354-b4909b5bd2d6/download/emisiones-de-co2-por-sector.csv"
df = pd.read_csv(URL, sep=";", encoding="latin-1")
```

## Descripción
Emisiones de CO2 derivadas de la quema de combustibles fósiles, abiertas
por sector. Cubre tanto las "Industrias de la Energía" (centrales
eléctricas y consumo propio del sector) como los sectores de consumo
final.

## Diccionario de columnas
| Columna | Unidad | Descripción |
|---|---|---|
| `AÑO` | año | Año del registro |
| `CE_SP` | Gg CO2 | Centrales eléctricas de servicio público (SIN) |
| `CP` | Gg CO2 | Consumo propio del sector energético |
| `I_E` | Gg CO2 | **Subtotal Industrias de la Energía** = CE_SP + CP |
| `R` | Gg CO2 | Residencial |
| `C_S_SP` | Gg CO2 | Comercial / Servicios / Sector público |
| `T` | Gg CO2 | Transporte |
| `I` | Gg CO2 | Industrial (incluye autoproducción eléctrica industrial) |
| `AP` | Gg CO2 | Actividades primarias |
| `NI` | Gg CO2 | No identificado |
| `S_C` | Gg CO2 | **Subtotal Sectores de Consumo** = R+C_S_SP+T+I+AP+NI |
| `TOTAL` | Gg CO2 | Total = I_E + S_C |
| `Q_B` | Gg CO2 | Quema de biomasa - **partida informativa, NO se suma al TOTAL** |
| `BI` | Gg CO2 | Búnkers internacionales - **partida informativa, NO se suma al TOTAL** |

## Head (primeras 10 líneas)
```
AÑO;CE_SP;CP;I_E;R;C_S_SP;T;I;AP;NI;S_C;TOTAL;Q_B;BI
1965;1009.136;118.117;1127.254;593.634;0.0;1561.063;1274.773;308.208;21.562;3759.24;4886.494;1733.318;307.09
1966;475.488;130.168;605.655;582.694;0.0;1547.79;1252.468;306.169;6.673;3695.795;4301.45;1734.273;319.956
1967;597.602;146.577;744.179;573.139;0.0;1492.905;1184.931;271.867;3.772;3526.615;4270.794;1734.641;247.397
1968;733.472;141.271;874.743;575.331;0.0;1460.471;1103.378;268.937;5.803;3413.92;4288.663;1751.925;198.366
1969;930.645;148.351;1078.996;630.437;0.0;1567.558;1193.656;268.372;3.482;3663.505;4742.501;1784.431;226.917
1970;850.127;166.129;1016.256;657.625;0.0;1662.116;1339.255;237.768;3.482;3900.246;4916.502;1819.617;231.633
1971;741.621;162.285;903.907;743.516;0.0;1722.722;1335.013;226.691;34.675;4062.617;4966.524;1827.203;266.128
1972;1140.337;150.273;1290.61;744.673;0.0;1852.112;1310.978;206.28;10.445;4124.488;5415.098;1883.223;284.806
1973;806.201;153.167;959.367;740.493;0.0;1799.301;1332.443;209.926;0.0;4082.162;5041.529;1875.904;311.076
```
2024: TOTAL = 7099 Gg CO2. Transporte 4350 (61%), Industrial 1465 (21%),
Residencial 349, C/S/SP 256, Primarias 446.

## Preguntas que ayuda a responder
- **2. Matriz energética** - emisiones del sector eléctrico (vía CE_SP)
  son el "termómetro" de la transición.
- **5. Consumo por sector** - qué sectores son los grandes emisores.
- **6. Eficiencia / transición** - emisiones / consumo final = intensidad
  de carbono.

## Notas
- `Q_B` (biomasa) y `BI` (búnker internacional) son partidas
  **informativas**: por convención IPCC no se suman al inventario
  nacional.
- `I_E + S_C = TOTAL` (verificar suma como QA).
- Para vincular emisión eléctrica con generación: ver
  `miem-ben-factor-de-emision-de-co2-del-sin`.
