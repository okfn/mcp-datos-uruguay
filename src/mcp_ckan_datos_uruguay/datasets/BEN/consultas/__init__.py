"""
Paquete de consultas BEN — Balance Energético Nacional de Uruguay (MIEM).

Submódulos por familia de dataset:
  - electricidad   (generación, capacidad instalada, factor emisión SIN)
  - consumo        (consumo final por sector, por fuente, demanda total)
  - abastecimiento (oferta primaria, dependencia externa, pérdidas)
  - importaciones  (gas natural, petróleo, intercambio eléctrico)
  - emisiones      (CO2 por sector)
  - _common        (loaders, builders de gráficos, helpers de unidades)

Cada función pública aquí re-exportada está pensada para ser registrada
como tool MCP desde `mcp_ckan_datos_uruguay/__init__.py:register_tools`.
"""

from .electricidad import (
    matriz_generacion_electrica,
    potencia_instalada_por_fuente,
    factor_emision_electrico,
)
from .consumo import (
    consumo_final_por_sector,
    consumo_final_por_fuente,
    tendencia_demanda_total,
)
from .abastecimiento import (
    matriz_abastecimiento_primario,
    dependencia_energetica_externa,
    perdidas_transformacion,
)
from .importaciones import (
    importacion_petroleo,
    importacion_gas_natural,
    intercambio_electricidad,
)
from .emisiones import (
    emisiones_co2_por_sector,
)


__all__ = [
    "matriz_generacion_electrica",
    "potencia_instalada_por_fuente",
    "factor_emision_electrico",
    "consumo_final_por_sector",
    "consumo_final_por_fuente",
    "tendencia_demanda_total",
    "matriz_abastecimiento_primario",
    "dependencia_energetica_externa",
    "perdidas_transformacion",
    "importacion_petroleo",
    "importacion_gas_natural",
    "intercambio_electricidad",
    "emisiones_co2_por_sector",
]
