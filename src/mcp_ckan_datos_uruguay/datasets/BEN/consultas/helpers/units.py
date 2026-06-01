"""
Explicación de unidades del BEN para concatenar al texto de respuesta.

`unit_blurb(*units)` arma el bloque ' Unidades' para que el lector no técnico
entienda ktep, GWh, MW, Gg CO2 y t CO2/GWh.
"""

UNIT_BLURB = {
    # "ktep": Already defined in glosario
    "GWh": (
        "GWh = gigavatio-hora = 1 millón de kWh. Energía eléctrica "
        "efectivamente entregada en un período."
    ),
    "MW": (
        "MW = megavatio. Unidad de **potencia** (capacidad), no de energía. "
        "Una central de 100 MW al 100% durante 1 año generaría 876 GWh."
    ),
    "Gg CO2": (
        "Gg = gigagramo = 1.000 toneladas (= 1 kt). Convención IPCC para "
        "inventarios nacionales de gases de efecto invernadero."
    ),
    "t CO2/GWh": (
        "Intensidad de carbono: toneladas de CO2 emitidas por GWh de "
        "electricidad generada. Cuanto más bajo, más limpia la matriz."
    ),
}


def unit_blurb(*units):
    """Bloque ' Unidades' para concatenar al texto de respuesta."""
    lines = ["", "Unidades:"]
    for u in units:
        if u in UNIT_BLURB:
            lines.append(f"  - {UNIT_BLURB[u]}")
    return "\n".join(lines)
