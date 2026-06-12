"""
Colores oficiales BEN para los gráficos de las tools.

`COLORES_BEN` mapea cada etiqueta de serie/slice usada por las tools BEN al
color oficial de la paleta MIEM/DNE.

Cada entrada se resuelve por nombre contra la paleta original via
`ben_color(hoja, nombre)`, asi un cambio en el xlsx solo requiere regenerar
`ben_palette.py`. Donde la etiqueta de la tool no existe literal en la
paleta, el comentario indica que concepto BEN se le asigno.
"""

from .ben_palette import BEN_PALETTE


_INDEX = {
    (hoja, entrada["name"]): entrada["hex"]
    for hoja, entradas in BEN_PALETTE.items()
    for entrada in entradas
}

# Paginas de la planilla de cálculo.
_FUENTES = "Fuentes de energía"
_SECTORES = "Sectores y categ INGEI"
_OFERTA = "Oferta y demanda"
_CENTROS = "Centros de transformacion"


def ben_color(hoja, nombre):
    """Hex oficial de un color de la paleta BEN, por hoja y nombre exactos."""
    return _INDEX[(hoja, nombre)]


# Traductor de los nombres usado acá a los nombres de la paleta
# oficial del BEN.
COLORES_BEN = {
    # --- Fuentes de energia (matriz primaria, generacion, consumo por fuente)
    "Petróleo + derivados": ben_color(_FUENTES, "Petróleo crudo"),
    "Derivados del petróleo": ben_color(_FUENTES, "Derivados de petroleo"),
    "Biomasa": ben_color(_FUENTES, "Biomasa"),
    "Residuos de biomasa": ben_color(_FUENTES, "Residuos de biomasa"),
    "Leña / carbón vegetal": ben_color(_FUENTES, "Leña"),
    "Biocombustibles": ben_color(_FUENTES, "Biomasa para biocombustibles"),
    "Hidráulica": ben_color(_FUENTES, "Hidroenergía"),
    "Eólica": ben_color(_FUENTES, "Eólica"),
    "Solar": ben_color(_FUENTES, "Solar"),
    "Solar térmica": ben_color(_FUENTES, "Solar"),
    "Gas natural": ben_color(_FUENTES, "Gas natural"),
    "Carbón mineral": ben_color(_FUENTES, "Carbón mineral"),
    "Carbón + coque": ben_color(_FUENTES, "Carbón mineral"),
    "Derivados del carbón": ben_color(_FUENTES, "Coque de carbón"),
    "Electricidad": ben_color(_FUENTES, "Electricidad"),
    "Electricidad importada": ben_color(_OFERTA, "Electricidad importada"),
    "Residuos industriales": ben_color(_FUENTES, "Residuos Industriales"),
    # 'Fósil' (generacion/potencia electrica) usa el color 'No renovable'
    "Fósil": ben_color(_OFERTA, "No renovable"),

    # --- Sectores de consumo / emisiones (hoja Sectores y categ INGEI)
    "Industrial": ben_color(_SECTORES, "Industrial"),
    "Transporte": ben_color(_SECTORES, "Transporte"),
    "Residencial": ben_color(_SECTORES, "Residencial"),
    "Comercial/Servicios/Sec.Público": ben_color(_SECTORES, "Comercial/Servicios/Sector público"),
    "Actividades primarias": ben_color(_SECTORES, "Actividades primarias"),
    "No identificado": ben_color(_SECTORES, "No identificado"),
    "Centrales eléctricas (servicio público)": ben_color(_SECTORES, "Centrales térmicas"),
    "Consumo propio sector energético": ben_color(_SECTORES, "Consumo propio"),

    # --- Flujos de oferta y demanda
    "Importación": ben_color(_OFERTA, "Importación"),
    "Exportación": ben_color(_OFERTA, "Exportación"),
    "Importado": ben_color(_OFERTA, "Energía importada"),
    "Local": ben_color(_OFERTA, "Energía de origen local"),
    "Oferta primaria": ben_color(_OFERTA, "OFERTA"),
    "Consumo final": ben_color(_OFERTA, "CONSUMO FINAL TOTAL"),
    "Consumo total": ben_color(_OFERTA, "CONSUMO NETO TOTAL"),
    "Pérdidas + autoconsumo": ben_color(_OFERTA, "Pérdidas"),
    "Carga refinería": ben_color(_CENTROS, "Refinerías"),
    "Importación gas natural": ben_color(_FUENTES, "Gas natural"),
    # Factor de emision del SIN: emisiones de las centrales termicas
    "FE_SIN": ben_color(_SECTORES, "Centrales térmicas"),
}
