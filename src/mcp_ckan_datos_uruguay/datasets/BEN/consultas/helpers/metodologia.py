"""
Definiciones oficiales del Libro del Balance Energético Nacional (MIEM).

Fuente única de verdad para la metodología del BEN. El texto de cada
definición es **literal** del capítulo "8. Metodología" del Libro del BEN 2024
(`resources/ben/libro-2024/text/08-metodologia.txt`): se unieron las líneas
cortadas por el salto de página del PDF y se quitaron los marcadores de viñeta,
pero no se reinterpretó ni se resumió el contenido. La sección (`§8.x`) de cada
entrada permite rastrear la cita en el libro.

`definiciones_relevantes(*conceptos)` arma el bloque de texto que las tools de
datos del BEN concatenan a su respuesta (igual que `units.unit_blurb`), para que
la IA responda con la metodología oficial sin un segundo llamado. La tool
on-demand de glosario (`consultas/glosario.py`) consume este mismo diccionario.

Mantener este módulo alineado con el .txt: si cambia la edición del libro,
re-extraer el texto y revisar cada definición literal antes de actualizarla.
"""


# concepto -> {termino, definicion (literal del §8), seccion, tools}
# `tools` documenta qué tools de datos inyectan esta definición en su respuesta.
DEFINICIONES_BEN = {
    # ── §8.1 Definiciones generales ──────────────────────────────────────
    "energia_primaria": {
        "termino": "Fuente de energía primaria",
        "definicion": (
            "es la fuente de energía provista por la naturaleza, ya sea en "
            "forma directa como la hidráulica y la eólica, después de atravesar "
            "un proceso minero, (como los hidrocarburos, el gas natural y el "
            "carbón mineral), o a través de la fotosíntesis (como en el caso de "
            "la leña y los residuos de biomasa originados en las actividades "
            "urbana, agropecuaria y agroindustrial). También se consideran "
            "fuentes de energía primaria aquellas fuentes secundarias para las "
            "que no es posible cuantificar las fuentes de energía primaria que "
            "le da origen, por ejemplo, el biogás."
        ),
        "seccion": "§8.1",
        "tools": ["matriz_energetica_primaria_uy"],
    },
    "energia_secundaria": {
        "termino": "Fuente de energía secundaria",
        "definicion": (
            "es la fuente de energía obtenida a partir de una fuente primaria "
            "(o de otra secundaria), después de someterla a un proceso "
            "físico-químico que modifica sus características iniciales."
        ),
        "seccion": "§8.1",
        "tools": ["matriz_energetica_primaria_uy"],
    },
    "energia_bruta": {
        "termino": "Energía bruta",
        "definicion": (
            "energía primaria o secundaria a la cual no se le han deducido las "
            "pérdidas de transformación, transmisión, transporte, distribución "
            "y almacenamiento, ni aquella cantidad de energía que no haya sido "
            "utilizada."
        ),
        "seccion": "§8.1",
        "tools": [],
    },
    "energia_neta": {
        "termino": "Energía neta",
        "definicion": (
            "energía primaria o secundaria cuyo destino es el consumo y a la "
            "que se le han deducido las pérdidas anteriormente mencionadas y la "
            "energía no utilizada."
        ),
        "seccion": "§8.1",
        "tools": ["perdidas_transformacion_energetica_uy"],
    },
    "energia_final": {
        "termino": "Energía final",
        "definicion": (
            "energía primaria o secundaria que es utilizada directamente por "
            "los sectores socioeconómicos. Entra al sector de consumo y se "
            "diferencia de la anterior por el consumo propio del sector "
            "energético. Incluye al consumo energético y al no energético."
        ),
        "seccion": "§8.1",
        "tools": [
            "consumo_energetico_por_sector_uy",
            "consumo_energetico_por_fuente_uy",
            "tendencia_demanda_energetica_uy",
        ],
    },
    "centro_de_transformacion": {
        "termino": "Centro de transformación",
        "definicion": (
            "instalación donde la energía primaria o secundaria es sometida a "
            "procesos que modifican sus propiedades o su naturaleza original "
            "mediante cambios físico-químicos y cuyo objetivo es convertirla en "
            "otra forma de energía más adecuada para el consumo. Se clasifican "
            "en 'primarios', si solamente procesan fuentes primarias y "
            "'secundarios', si al centro de transformación ingresan fuentes "
            "primarias y/o secundarias."
        ),
        "seccion": "§8.1",
        "tools": ["perdidas_transformacion_energetica_uy"],
    },
    "sector_de_consumo": {
        "termino": "Sector de consumo",
        "definicion": (
            "parte de la actividad socioeconómica que recibe la energía final "
            "para su utilización. El consumo propio se considera en forma "
            "independiente y corresponde a la energía consumida por el sector "
            "energético para la producción, transformación, transporte y "
            "distribución de energía (no incluye la utilizada como insumo para "
            "la transformación a otro tipo de energía)."
        ),
        "seccion": "§8.1",
        "tools": [
            "consumo_energetico_por_sector_uy",
            "emisiones_co2_por_sector_uy",
        ],
    },

    # ── §8.2 Rubros del balance (energía primaria y secundaria) ──────────
    "produccion": {
        "termino": "Producción",
        "definicion": (
            "cantidad de energía primaria extraída de la naturaleza o cantidad "
            "de energía secundaria originada en un centro de transformación."
        ),
        "seccion": "§8.2",
        "tools": ["matriz_energetica_primaria_uy"],
    },
    "importacion": {
        "termino": "Importación",
        "definicion": (
            "energía primaria o secundaria proveniente del exterior del país."
        ),
        "seccion": "§8.2",
        "tools": [
            "importacion_petroleo_uy",
            "importacion_gas_natural_uy",
            "intercambio_electrico_uy",
            "dependencia_energetica_externa_uy",
        ],
    },
    "exportacion": {
        "termino": "Exportación",
        "definicion": (
            "energía primaria o secundaria enviada al exterior. Las "
            "exportaciones a zona franca no se consideran exportaciones como "
            "tales, sino que se incluyen en el consumo final como ventas en el "
            "mercado interno."
        ),
        "seccion": "§8.2",
        "tools": ["intercambio_electrico_uy"],
    },
    "perdidas": {
        "termino": "Pérdidas",
        "definicion": (
            "pérdidas de energía originadas durante el transporte, "
            "almacenamiento, transmisión y distribución (pérdidas técnicas). "
            "Hasta 2005 las pérdidas no técnicas del sector eléctrico se "
            "computaron como 'pérdidas'; a partir de 2006 se contabilizaron "
            "como 'consumo final energético'. Las pérdidas sociales se "
            "incluyeron en el sector residencial y el resto de las pérdidas no "
            "técnicas se distribuyeron en proporción de consumo. Desde 2023 "
            "estas últimas se integraron al sector 'No identificado'."
        ),
        "seccion": "§8.2",
        "tools": ["perdidas_transformacion_energetica_uy"],
    },
    "variacion_de_inventario": {
        "termino": "Variación de inventario",
        "definicion": (
            "diferencia entre las existencias de una fuente energética al 31 "
            "de diciembre del año i-1 y al 31 de diciembre del año i."
        ),
        "seccion": "§8.2",
        "tools": [],
    },
    "energia_no_utilizada": {
        "termino": "Energía no utilizada",
        "definicion": (
            "cantidad de energía que no se utiliza por la naturaleza técnica "
            "y/o económica de su explotación."
        ),
        "seccion": "§8.2",
        "tools": [],
    },

    # ── §8.2.1 Aclaraciones por fuente de energía primaria ───────────────
    "carbon_mineral": {
        "termino": "Carbón mineral",
        "definicion": (
            "incluye antracita, turba, alquitranes de hulla, brea, entre otros "
            "tipos de carbón. No se considera la turba de uso no energético, "
            "según metodología IRES."
        ),
        "seccion": "§8.2.1",
        "tools": ["matriz_energetica_primaria_uy"],
    },
    "gas_natural": {
        "termino": "Gas natural",
        "definicion": (
            "los datos están considerados en condiciones estándar "
            "(1 atm y 15°C)."
        ),
        "seccion": "§8.2.1",
        "tools": ["importacion_gas_natural_uy"],
    },
    "energia_solar": {
        "termino": "Energía solar",
        "definicion": (
            "incluye energía solar fotovoltaica y energía solar térmica."
        ),
        "seccion": "§8.2.1",
        "tools": [
            "matriz_generacion_electrica_uy",
            "potencia_instalada_electrica_uy",
        ],
    },
    "residuos_de_biomasa": {
        "termino": "Residuos de biomasa",
        "definicion": (
            "incluye cáscara de arroz y de girasol, bagazo de caña, licor "
            "negro, gases olorosos, metanol, casullo de cebada, residuos "
            "forestales y de aserradero (chips, aserrín, pellets, etc.), "
            "glicerina, rumen y lodo mezcla de tratamiento de efluentes."
        ),
        "seccion": "§8.2.1",
        "tools": ["consumo_energetico_por_fuente_uy"],
    },
    "biomasa_para_biocombustibles": {
        "termino": "Biomasa para producción de biocombustibles",
        "definicion": "considera la producción de bioetanol y biodiésel.",
        "seccion": "§8.2.1",
        "tools": ["matriz_energetica_primaria_uy"],
    },
    "residuos_industriales": {
        "termino": "Residuos industriales",
        "definicion": (
            "incluye residuos industriales no renovables como ser neumáticos "
            "fuera de uso (NFU), combustibles líquidos alternativos (CLA), "
            "aceites usados y combustibles sólidos residuales (CSR). Se aclara "
            "que estos últimos residuos contienen una mezcla de productos que "
            "no es posible desagregar y, por lo tanto, pueden contener una "
            "pequeña fracción de algún residuo renovable."
        ),
        "seccion": "§8.2.1",
        "tools": ["matriz_energetica_primaria_uy"],
    },

    # ── §8.3 Unidades y formato de datos ─────────────────────────────────
    "ktep": {
        "termino": "ktep (unidad del balance)",
        "definicion": (
            "La unidad adoptada para expresar los flujos energéticos que "
            "componen el Balance Energético Nacional es el ktep (miles de "
            "toneladas equivalentes de petróleo). 1 ktep = 1.000 tep. "
            "1 tep = 10.000.000 kcal. La conversión de las magnitudes "
            "correspondientes a cada fuente a su expresión en tep se realiza a "
            "través de su respectivo Poder Calorífico Inferior (PCI). En el "
            "caso de la electricidad se aplica el criterio técnico de "
            "0,086 tep / MWh."
        ),
        "seccion": "§8.3",
        "tools": [],
    },

    # ── §8.4.8 Emisiones de CO2 ──────────────────────────────────────────
    "emisiones_co2": {
        "termino": "Emisiones de CO2",
        "definicion": (
            "En la publicación del BEN se incluyen las emisiones de dióxido de "
            "carbono (CO2) correspondientes a las actividades de quema de "
            "combustibles en las industrias de la energía y los sectores de "
            "consumo. A su vez, se incluyen las emisiones de CO2 provenientes "
            "de la quema de biomasa y de búnkers internacionales, las que se "
            "presentan como partidas informativas ya que no se consideran en "
            "los totales. La serie comienza en 1965. Las emisiones de CO2 son "
            "calculadas siguiendo las directrices del IPCC para los "
            "Inventarios Nacionales de Gases de Efecto Invernadero versión 2006."
        ),
        "seccion": "§8.4.8",
        "tools": [
            "emisiones_co2_por_sector_uy",
            "factor_emision_electrico_uy",
        ],
    },

    # ── §8.4.9 Matriz de energía primaria (abastecimiento) ───────────────
    "matriz_primaria": {
        "termino": "Matriz primaria (abastecimiento)",
        "definicion": (
            "En la 'matriz primaria', o también llamada 'matriz de "
            "abastecimiento', se representa el aprovisionamiento de energía al "
            "país con la siguiente apertura: 'electricidad', 'solar', 'petróleo "
            "y derivados', 'gas natural', 'biomasa' y 'carbón/coque'. Como "
            "metodología general, para su elaboración se consideran las "
            "actividades de oferta que correspondan para cada energético "
            "(producción, importación, exportación y búnker internacional). En "
            "el caso de fuentes secundarias, no se considera la producción "
            "porque ya se contempla esa contribución en la fuente primaria que "
            "le da origen."
        ),
        "seccion": "§8.4.9",
        "tools": ["matriz_energetica_primaria_uy"],
    },
    "clasificacion_por_origen": {
        "termino": "Abastecimiento por origen",
        "definicion": (
            "Local: producción nacional. Importada: importaciones netas."
        ),
        "seccion": "§8.4.9",
        "tools": ["dependencia_energetica_externa_uy"],
    },
    "clasificacion_por_tipo": {
        "termino": "Abastecimiento por tipo",
        "definicion": (
            "Renovable: electricidad de origen hidráulico, eólico y solar "
            "fotovoltaico / biomasa / solar térmica. No renovable: gas natural "
            "/ petróleo y derivados / carbón y coque. Electricidad importada."
        ),
        "seccion": "§8.4.9",
        "tools": [
            "matriz_energetica_primaria_uy",
            "matriz_generacion_electrica_uy",
            "potencia_instalada_electrica_uy",
        ],
    },
}


CONCEPTOS_DISPONIBLES = sorted(DEFINICIONES_BEN)


def definiciones_relevantes(*conceptos):
    """Bloque de texto con definiciones oficiales del Libro del BEN.

    Pensado para concatenarse al texto de respuesta de una tool de datos
    (igual que `units.unit_blurb`), de modo que la definición oficial entre
    al contexto de la IA. Cita el recurso del libro para que la IA pueda
    indicar que usó parte del libro. Conceptos desconocidos se ignoran.
    """
    lines = ["", "Atención - definiciones relevantes del Libro del BEN 2024 (MIEM):"]
    for concepto in conceptos:
        d = DEFINICIONES_BEN.get(concepto)
        if d is None:
            continue
        lines.append(f"  - {d['termino']} ({d['seccion']}): {d['definicion']}")
    lines.append("Libro del Balance Energético Nacional 2024 (MIEM)")
    return "\n".join(lines)
