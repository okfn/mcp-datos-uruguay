"""
Tool BEN - Glosario.

Devuelve la definición oficial de un concepto del Balance Energético Nacional,
tomada literal del Libro del BEN (ver `helpers/metodologia.py`). El título, la
URI y la URL del libro se leen del recurso registrado en `resources/ben/libro.py`
(fuente única), no se hardcodean acá.
"""

from mcp_server import DataToolOutput
from mcp_ckan_datos_uruguay.resources.ben.libro import data as libro

from . import helpers as h


def glosario(concepto) -> DataToolOutput:
    """Definición oficial de un concepto del BEN como `DataToolOutput`."""
    libro_url = libro["annotations"]["url"]
    d = h.DEFINICIONES_BEN.get(concepto)
    if d is None:
        disponibles = ", ".join(h.CONCEPTOS_DISPONIBLES)
        texto = (
            f"No hay una definición «{concepto}» en el glosario del BEN. "
            f"Conceptos disponibles: {disponibles}."
        )
        return h.text_result(texto, [libro_url])

    texto = (
        f"{d['termino']} - definición oficial del Libro del BEN 2024 (MIEM), "
        f"{d['seccion']}:\n\n"
        f"«{d['definicion']}»\n\n"
        f"Fuente textual oficial: {libro['name']} (recurso {libro['uri']})."
    )
    return h.text_result(texto, [libro_url])
