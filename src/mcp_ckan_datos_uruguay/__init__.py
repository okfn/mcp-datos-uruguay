from mcp_server import DataToolOutput
# from mcp_ckan_datos_uruguay.datasets.compras_ocds import consultas
# from mcp_ckan_datos_uruguay.datasets.delitos_sexuales import consultas as delitos
from mcp_ckan_datos_uruguay.datasets.BEN import consultas as ben


def register_tools(mcp):
    _register_ben_tools(mcp)
    # _register_other_tools(mcp)


def _register_ben_tools(mcp):
    """BEN — Balance Energético Nacional (MIEM).

    Fuente: catalogodatos.gub.uy. Datos anuales desde 1965 (la serie de
    generación eléctrica arranca en 2002). Detalle por dataset y mapa
    pregunta → fuente en `datasets/BEN/data/INDEX.md`.
    """
    mcp.set_plugin_info(
        description=(
            "Herramientas sobre datos abiertos del Uruguay (catalogodatos.gub.uy). "
        ),
        sample_questions=[
            "¿Cuál es la principal fuente de generación eléctrica de Uruguay?",
            "¿Qué porcentaje de la generación es renovable?",
            "¿Cómo evolucionó la matriz desde 2002?",
            "¿Cuándo entró la energía eólica/solar en Uruguay?",
            "¿Qué fuentes están creciendo más?",
            "¿Cuánta potencia eólica/solar/hidráulica tiene Uruguay?",
            "¿Qué fuentes están creciendo en capacidad?",
            "¿Cuándo entró la primera planta solar?",
            "¿Qué tan limpia es la matriz eléctrica de Uruguay?",
            "¿Cómo evolucionó la 'limpieza' del SIN con la transición?",
            "¿En qué años fue más sucio (sequía hidráulica)?",
            "¿Uruguay importa o exporta electricidad?",
            "¿Cómo evolucionó la posición exportadora del país?",
            "¿En qué años fue exportador / importador neto?",
        ],
    )

    # ── Delitos sexuales (2018-2024) — tools Python ─────────────
    # Fuente: Ministerio del Interior, catalogodatos.gub.uy
    # Complementan las tools YAML con análisis que requieren lógica:
    #  - Tendencia anual (requiere extraer año de la fecha)
    #  - Ranking de departamentos (requiere conteo ordenado con porcentajes)
    # Las consultas básicas (listar, contar, valores únicos) están en YAML.

    @mcp.tool()
    def matriz_generacion_electrica_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Matriz de generación eléctrica de Uruguay por fuente y por año, en GWh.
            Responde preguntas sobre la **matriz energética eléctrica**, la
            **transición a renovables** y la **evolución del peso de cada fuente**
            (hidráulica, eólica, solar, biomasa, fósil) en el sistema
            interconectado nacional (SIN). Útil para preguntas como:
                - ¿Cuál es la principal fuente de generación eléctrica de Uruguay?
                - ¿Qué porcentaje de la generación es renovable?
                - ¿Cómo evolucionó la matriz desde 2002?
                - ¿Cuándo entró la energía eólica/solar en Uruguay?
                - ¿Qué fuentes están creciendo más?
            Cobertura: 2002-2024 (anual; sin granularidad mensual ni horaria).
            Devuelve un texto con el mix del último año + % renovables, una
            tabla año-por-año con todas las fuentes, y un gráfico: **pie
            chart** si se pide un único año (mix de ese año), o **barras
            apiladas (stacked)** si se piden varios años (evolución).
            Datos del Ministerio de Industria, Energía y Minería (MIEM) — Balance
            Energético Nacional, publicados en catalogodatos.gub.uy. Unidad: GWh
            (gigavatios-hora) — energía eléctrica efectivamente generada,
            incluye servicio público + autoproducción industrial entregada al
            SIN. **No** confundir con `MW` (potencia instalada / capacidad) ni
            con `ktep` (que es la convención de balance energético usada en
            consumo y abastecimiento).

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 2002, el
                primer año de la serie. Para ver sólo la "era renovable" usar
                2014.
            anio_hasta: Año final del rango (incluido). Default: el último año
                disponible (típicamente 2024).

        Returns:
            str: Resumen del mix del último año (GWh y % por fuente, % de
                 renovables totales), tabla con todos los años en el rango y
                 gráfico de barras stacked por fuente.

        Examples:
            - matriz_generacion_electrica_uy()
            - matriz_generacion_electrica_uy(anio_desde=2014)
            - matriz_generacion_electrica_uy(anio_desde=2020, anio_hasta=2024)
        """
        return ben.matriz_generacion_electrica(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def potencia_instalada_electrica_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Capacidad eléctrica instalada en Uruguay por fuente, en MW (megavatios).
            Responde preguntas sobre **capacidad** (potencia físicamente
            instalada) — no confundir con generación. Cuándo se incorporó cada
            tecnología, cuánto MW de eólica/solar/hidro/biomasa/fósil hay,
            % renovables del parque generador.
            Útil para preguntas como:
                - ¿Cuánta potencia eólica/solar/hidráulica tiene Uruguay?
                - ¿Qué fuentes están creciendo en capacidad?
                - ¿Cuándo entró la primera planta solar?
            **MW = potencia (capacidad), no energía.** Una central de 100 MW
            funcionando todo el año al 100% generaría 876 GWh; el cociente
            real (factor de capacidad) es típicamente 30-90%. Para energía
            efectivamente generada usar `matriz_generacion_electrica_uy`.
            Cobertura útil real: 2003-2024 (datos previos casi todos vacíos).
            Pie chart si se pide 1 año, stacked bar si se piden varios.

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 2003.
            anio_hasta: Año final del rango (incluido). Default: último
                año disponible.

        Examples:
            - potencia_instalada_electrica_uy()
            - potencia_instalada_electrica_uy(anio_desde=2024, anio_hasta=2024)
            - potencia_instalada_electrica_uy(anio_desde=2010)
        """
        return ben.potencia_instalada_por_fuente(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def factor_emision_electrico_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Intensidad de carbono del sistema eléctrico uruguayo (FE_SIN), en
            **toneladas de CO2 por GWh** generado. Métrica única que resume
            qué tan limpio es el SIN: cae fuerte cuando entra eólica/solar y
            sube en años hidráulicos secos donde toca despachar térmica fósil.
            Útil para responder:
                - ¿Qué tan limpia es la matriz eléctrica de Uruguay?
                - ¿Cómo evolucionó la 'limpieza' del SIN con la transición?
                - ¿En qué años fue más sucio (sequía hidráulica)?
            Referencias: térmica gas ciclo combinado ≈ 350-400 t CO2/GWh,
            térmica carbón ≈ 800-1000, renovables ≈ 0. Uruguay 2024 = 6.3
            (año hidráulico excelente). Cobertura: 1965-2024 anual.
            Devuelve gráfico de líneas (varios años) o de barras vs
            referencias técnicas (un solo año).

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último
                año disponible.

        Examples:
            - factor_emision_electrico_uy()
            - factor_emision_electrico_uy(anio_desde=2010)
            - factor_emision_electrico_uy(anio_desde=2024, anio_hasta=2024)
        """
        return ben.factor_emision_electrico(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def consumo_energetico_por_sector_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Consumo final energético de Uruguay desagregado por sector, en ktep.
            Responde quién consume la energía: industria, transporte,
            residencial, comercial/servicios/sector público, primarias.
            Útil para:
                - ¿Qué sector consume más energía?
                - ¿Cómo evolucionó el consumo por sector?
                - ¿Qué sector creció más?
            Cobertura: 1965-2024 (anual). Limitación: no hay sub-segmentación
            del transporte (auto/ómnibus/aviación no se distinguen). Para
            'intensidad energética industrial' (energía/VAB) hay que cruzar
            con BCU — no está en BEN.
            Pie chart si se pide 1 año, stacked bar si se piden varios.
            **ktep ≈ 11.63 GWh** — unidad estándar de balance energético.

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último
                año disponible (típicamente 2024).

        Examples:
            - consumo_energetico_por_sector_uy()
            - consumo_energetico_por_sector_uy(anio_desde=2024, anio_hasta=2024)
            - consumo_energetico_por_sector_uy(anio_desde=2000)
        """
        return ben.consumo_final_por_sector(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def consumo_energetico_por_fuente_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Consumo final energético de Uruguay desagregado por fuente, en ktep.
            Responde **qué tipo de energía** consumen los usuarios finales:
            derivados del petróleo, electricidad, biomasa (leña/residuos),
            gas natural, biocombustibles, etc.
            Útil para:
                - ¿Qué fuentes predominan del lado demanda?
                - ¿Cuánto pesan los combustibles fósiles en el consumo final?
                - ¿% renovable del consumo final?
            El TOTAL coincide con el de `consumo_energetico_por_sector_uy`
            (misma magnitud, dos desagregaciones).
            Cobertura: 1965-2024 (anual). Pie chart si se pide 1 año,
            stacked bar si se piden varios.
            **ktep ≈ 11.63 GWh** — unidad estándar de balance energético.

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último año.

        Examples:
            - consumo_energetico_por_fuente_uy()
            - consumo_energetico_por_fuente_uy(anio_desde=2024, anio_hasta=2024)
            - consumo_energetico_por_fuente_uy(anio_desde=2000)
        """
        return ben.consumo_final_por_fuente(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def tendencia_demanda_energetica_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Evolución del consumo energético TOTAL de Uruguay, en ktep/año.
            Vista macro: ¿el país aumenta o reduce su demanda? ¿Cuánto creció
            en los últimos 5 años? ¿Cuál es el consumo total del último año?
            Devuelve serie de tiempo (gráfico de líneas) con todos los años
            del rango.
            Cobertura: 1965-2024 (anual).
            **No** computa per-cápita ni intensidad/PIB: BEN no incluye
            población ni PIB; cruzar con INE/BCU para esas métricas.

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último año.

        Examples:
            - tendencia_demanda_energetica_uy()
            - tendencia_demanda_energetica_uy(anio_desde=2020)
            - tendencia_demanda_energetica_uy(anio_desde=2000)
        """
        return ben.tendencia_demanda_total(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def matriz_energetica_primaria_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Oferta primaria de energía total del país por fuente, en ktep.
            Es **la matriz energética** completa (no sólo eléctrica): suma
            todas las fuentes que ingresan al sistema (importación +
            producción local) — petróleo y derivados, biomasa, hidráulica,
            eólica, solar, gas natural, carbón/coque, electricidad importada,
            residuos industriales.
            Útil para:
                - ¿Qué fuentes predominan en la matriz energética?
                - ¿% renovable vs no renovable de la oferta primaria?
                - ¿Cómo evolucionó la participación renovable?
                - Diversificación (índice de Herfindahl normalizado).
            Cobertura: 1965-2024 (anual). Pie chart si 1 año, stacked bar si
            varios.
            **ktep ≈ 11.63 GWh.** Para sólo el sub-sistema eléctrico ver
            `matriz_generacion_electrica_uy`.

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último año.

        Examples:
            - matriz_energetica_primaria_uy()
            - matriz_energetica_primaria_uy(anio_desde=2024, anio_hasta=2024)
            - matriz_energetica_primaria_uy(anio_desde=2000)
        """
        return ben.matriz_abastecimiento_primario(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def dependencia_energetica_externa_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """% de energía importada vs producida localmente, año a año (ktep).
            Indicador de **seguridad energética y dependencia externa**.
            Definición usada: importado = electricidad importada + gas
            natural + petróleo y derivados + carbón y coque. Local = el
            resto (renovables locales, biomasa, residuos industriales).
            Útil para:
                - ¿Cuánta energía se importa vs se produce localmente?
                - ¿Cuál es el nivel de dependencia energética del país?
                - ¿Cómo evolucionó la dependencia con la entrada de
                  renovables locales?
            Cobertura: 1965-2024. Pie chart (importado vs local) si se pide
            1 año; stacked bar evolutivo si se piden varios años.

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último año.

        Examples:
            - dependencia_energetica_externa_uy()
            - dependencia_energetica_externa_uy(anio_desde=2024, anio_hasta=2024)
            - dependencia_energetica_externa_uy(anio_desde=2000)
        """
        return ben.dependencia_energetica_externa(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def perdidas_transformacion_energetica_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Pérdidas de transformación + autoconsumo del sector energético, en ktep.
            Calcula la diferencia entre **oferta primaria** (lo que entra al
            sistema) y **consumo final** (lo que llega a los usuarios).
            Esta diferencia agrupa: pérdidas físicas en transporte y
            distribución, autoconsumo del sector energético (refinería,
            centrales) y pérdidas termodinámicas de transformación
            (calor → electricidad).
            Útil para:
                - ¿Qué pérdidas tiene el sistema en transformación y
                  distribución?
                - ¿Cómo evolucionó la eficiencia agregada del sistema?
            Cobertura: 1965-2024. Cruza dos datasets: abastecimiento +
            consumo final por fuente.

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último año.

        Examples:
            - perdidas_transformacion_energetica_uy()
            - perdidas_transformacion_energetica_uy(anio_desde=2010)
            - perdidas_transformacion_energetica_uy(anio_desde=2024, anio_hasta=2024)
        """
        return ben.perdidas_transformacion(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def importacion_petroleo_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Importación de petróleo crudo + carga de la refinería La Teja
            (ANCAP), en ktep.
            Dos magnitudes: cuánto crudo se importó y cuánto efectivamente
            se procesó en refinería. La diferencia refleja stocks. En años
            con parada técnica de refinería la carga cae fuerte.
            Útil para:
                - ¿Cuánto petróleo importa Uruguay?
                - ¿Cómo evolucionaron las importaciones de petróleo?
                - Vulnerabilidad ante shocks del precio del petróleo (input
                  clave).
            Cobertura: 1965-2024. Devuelve gráfico de líneas (varios años) o
            barras (un único año).

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último año.

        Examples:
            - importacion_petroleo_uy()
            - importacion_petroleo_uy(anio_desde=2010)
            - importacion_petroleo_uy(anio_desde=2024, anio_hasta=2024)
        """
        return ben.importacion_petroleo(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def importacion_gas_natural_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Serie anual de importaciones de gas natural a Uruguay, en ktep.
            Uruguay no produce gas natural: lo importa desde Argentina por
            el gasoducto Cruz del Sur (operativo desde 1998). Volúmenes
            modestos comparados con petróleo y biomasa.
            Útil para:
                - ¿Cuánto gas natural importa Uruguay?
                - ¿Cómo evolucionaron las importaciones de gas?
                - Peso del gas natural en la matriz primaria (combinar con
                  `matriz_energetica_primaria_uy`).
            Cobertura útil: 1998-2024 (años pre-1998 vacíos).

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1998.
            anio_hasta: Año final del rango (incluido). Default: último año.

        Examples:
            - importacion_gas_natural_uy()
            - importacion_gas_natural_uy(anio_desde=2010)
            - importacion_gas_natural_uy(anio_desde=2024, anio_hasta=2024)
        """
        return ben.importacion_gas_natural(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def intercambio_electrico_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Intercambio eléctrico de Uruguay con Argentina y Brasil — importación,
            exportación y saldo neto, en ktep.
            Las dos columnas son **magnitudes positivas** (no usar signo):
            el saldo neto se calcula como exportación menos importación. Con
            la entrada masiva de eólica desde 2014, Uruguay pasó de
            importador estructural a exportador neto en años hidráulicos
            buenos.
            Útil para:
                - ¿Uruguay importa o exporta electricidad?
                - ¿Cómo evolucionó la posición exportadora del país?
                - ¿En qué años fue exportador / importador neto?
            Cobertura: 1965-2024 (anual). Devuelve gráfico de barras
            agrupadas (impo vs expo) por año.

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último año.

        Examples:
            - intercambio_electrico_uy()
            - intercambio_electrico_uy(anio_desde=2010)
            - intercambio_electrico_uy(anio_desde=2024, anio_hasta=2024)
        """
        return ben.intercambio_electricidad(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )

    @mcp.tool()
    def emisiones_co2_por_sector_uy(
        anio_desde: int | None = None, anio_hasta: int | None = None
    ) -> DataToolOutput:
        """Emisiones de CO2 por sector en Uruguay (combustión de combustibles
            fósiles), en **Gg CO2** (gigagramos = kilotoneladas).
            Sectores: transporte, industrial, centrales eléctricas (servicio
            público), residencial, comercial/servicios/sec.público,
            actividades primarias, consumo propio del sector energético.
            Útil para:
                - ¿Qué sectores son los grandes emisores?
                - ¿Cómo evolucionaron las emisiones por sector?
                - 'Termómetro' de la transición: caída de CE_SP con la
                  entrada de renovables.
            Cobertura: 1965-2024. Pie chart si 1 año, stacked bar si varios.
            Notas IPCC: las partidas Q_B (quema de biomasa) y BI (búnker
            internacional) son **informativas**, no se suman al inventario
            nacional. Sólo CO2 (no CH4/N2O) y sólo combustión (no procesos
            industriales como cemento). Para intensidad eléctrica del SIN
            usar `factor_emision_electrico_uy`.

        Args:
            anio_desde: Año inicial del rango (incluido). Default: 1965.
            anio_hasta: Año final del rango (incluido). Default: último año.

        Examples:
            - emisiones_co2_por_sector_uy()
            - emisiones_co2_por_sector_uy(anio_desde=2024, anio_hasta=2024)
            - emisiones_co2_por_sector_uy(anio_desde=2000)
        """
        return ben.emisiones_co2_por_sector(
            anio_desde=anio_desde, anio_hasta=anio_hasta,
        )


# def _register_other_tools(mcp):
#     """Tools no-BEN: delitos sexuales, compras públicas, misc."""

#     # ── Fallback obligatorio para tool_choice="required" ────────
#     # El gateway fuerza al modelo a llamar al menos una tool en la primera
#     # ronda. Cuando ninguna tool real aplica, esta es la salida limpia
#     # (mejor que improvisar mal con una tool real).

#     @mcp.tool()
#     def no_tool_disponible(razon: str = None) -> str:
#         """Llamar a esta tool **únicamente** cuando ninguna otra tool
#             disponible puede responder la pregunta del usuario. Por ejemplo:
#             preguntas sobre clima, deportes, otros países, o cualquier tema
#             no cubierto por los datasets de Uruguay (energía, compras
#             públicas, delitos sexuales).
#             **Nunca** la uses si hay una tool real que pueda contribuir,
#             aunque sea parcialmente — siempre preferí la tool específica.
#             Esta tool sólo emite un mensaje estándar diciéndole al usuario
#             que no hay datos disponibles para esa pregunta en el sistema.

#         Args:
#             razon: Breve explicación (1 frase) de por qué ninguna otra tool
#                 aplica. Ej: "no hay datasets de clima", "pregunta sobre
#                 otro país". Se incluye en el mensaje al usuario para que
#                 entienda el alcance del sistema.

#         Examples:
#             - no_tool_disponible(razon="no hay datasets sobre clima")
#             - no_tool_disponible(razon="pregunta sobre Argentina, no Uruguay")
#         """
#         msg = (
#             "No hay ninguna tool interna que pueda responder esta pregunta "
#             "con datos del sistema."
#         )
#         if razon:
#             msg += f" Motivo: {razon}."
#         msg += (
#             " El sistema cubre datos abiertos de Uruguay sobre energía "
#             "(BEN/MIEM), compras públicas (OCDS) y delitos sexuales "
#             "(Ministerio del Interior). Para otros temas, consultá fuentes "
#             "externas."
#         )
#         return msg

#     # ── Delitos sexuales (2018-2024) — tools Python ─────────────
#     # Fuente: Ministerio del Interior, catalogodatos.gub.uy
#     # Complementan las tools YAML con análisis que requieren lógica:
#     #  - Tendencia anual (requiere extraer año de la fecha)
#     #  - Ranking de departamentos (requiere conteo ordenado con porcentajes)
#     # Las consultas básicas (listar, contar, valores únicos) están en YAML.

#     @mcp.tool()
#     def tendencia_anual_delitos_sexuales_uy(
#         departamento: str | None = None, tipo_delito: str | None = None
#     ) -> DataToolOutput:
#         """Tendencia año a año de eventos de delitos sexuales en Uruguay (2018-2024).
#             Muestra un gráfico de barras de texto con la cantidad por año.

#         Args:
#             departamento: Departamento, e.g. "Montevideo", "Canelones".
#             tipo_delito: Tipo de delito: "Abuso Sexual", "Violación" o "Atentado Violento al Pudor".

#         Examples:
#             - tendencia_anual_delitos_sexuales_uy()
#             - tendencia_anual_delitos_sexuales_uy(departamento="Montevideo")
#             - tendencia_anual_delitos_sexuales_uy(tipo_delito="Abuso Sexual")
#         """
#         return delitos.tendencia_anual(departamento=departamento,
#                                        tipo_delito=tipo_delito)

#     @mcp.tool()
#     def ranking_departamentos_delitos_sexuales_uy(
#         anio: int | None = None, tipo_delito: str | None = None
#     ) -> DataToolOutput:
#         """Ranking de departamentos de Uruguay por cantidad de eventos de delitos sexuales.
#             Muestra todos los departamentos ordenados de mayor a menor cantidad.

#         Args:
#             anio: Año para filtrar (2018-2024). None para todos.
#             tipo_delito: Tipo de delito: "Abuso Sexual", "Violación" o "Atentado Violento al Pudor".

#         Examples:
#             - ranking_departamentos_delitos_sexuales_uy()
#             - ranking_departamentos_delitos_sexuales_uy(anio=2024)
#             - ranking_departamentos_delitos_sexuales_uy(tipo_delito="Violación")
#         """
#         return delitos.eventos_por_departamento(anio=anio, tipo_delito=tipo_delito)

#     # ── Compras públicas OCDS ─────────────────────────────────────

#     @mcp.tool()
#     def buscar_empresa_uruguay(nombre: str, limit: int = 10) -> DataToolOutput:
#         """Busca empresas proveedoras del estado uruguayo por nombre aproximado.
#             Usa esta herramienta cuando el usuario menciona una empresa y necesitas
#             encontrar el nombre exacto en la base de datos. Soporta nombres parciales
#             o con errores de ortografía.

#         Args:
#             nombre: Nombre o parte del nombre de la empresa. Ej: "Copernico", "ANTEL".
#             limit: Máximo de resultados a devolver. Default 10.

#         Returns:
#             str: Lista de empresas proveedoras similares al nombre buscado.

#         Examples:
#             - buscar_empresa_uruguay(nombre="Copernico")
#             - buscar_empresa_uruguay(nombre="laboratorio")
#             - buscar_empresa_uruguay(nombre="ANTEL", limit=5)
#         """
#         return consultas.buscar_empresa(nombre=nombre, limit=limit)

#     @mcp.tool()
#     def buscar_producto_uruguay(texto: str, limit: int = 10) -> DataToolOutput:
#         """Busca productos, rubros o insumos en las compras públicas de Uruguay.
#             Usa esta herramienta para encontrar la descripción exacta de un producto
#             antes de consultar compras_producto_uruguay.

#         Args:
#             texto: Descripción del producto. Ej: "medicamento", "computadora", "alimento".
#             limit: Máximo de resultados a devolver. Default 10.

#         Returns:
#             str: Lista de productos/rubros similares encontrados.

#         Examples:
#             - buscar_producto_uruguay(texto="medicamento")
#             - buscar_producto_uruguay(texto="computadora")
#             - buscar_producto_uruguay(texto="combustible")
#         """
#         return consultas.buscar_producto(texto=texto, limit=limit)

#     @mcp.tool()
#     def licitaciones_empresa_uruguay(
#         nombre_empresa: str,
#         year: int | None = None,
#         comprador: str | None = None,
#         metodo: str | None = None,
#         limit: int = 20,
#     ) -> DataToolOutput:
#         """Lista licitaciones y adjudicaciones en las que participó una empresa como proveedora.
#             Responde preguntas como: "En qué licitaciones participó la empresa X?"
#             Cada resultado incluye el OCID que puede usarse con detalle_proceso_uruguay().
#             Si no conoce el nombre exacto, use primero buscar_empresa_uruguay().

#         Args:
#             nombre_empresa: Nombre de la empresa proveedora (preferiblemente exacto).
#             year: Año para filtrar (2024 o 2025). None para todos los años.
#             comprador: Filtrar por organismo comprador (parcial). Ej: "Intendencia de Montevideo".
#             metodo: Filtrar por método de contratación (parcial). Ej: "Compra Directa".
#             limit: Máximo de resultados. Default 20.

#         Returns:
#             str: Detalle de licitaciones/adjudicaciones con OCID, fecha, comprador, título y método.

#         Examples:
#             - licitaciones_empresa_uruguay(nombre_empresa="COPERNICO COOPERATIVA INFORMATICA")
#             - licitaciones_empresa_uruguay(nombre_empresa="TILSOR S A", comprador="Intendencia de Montevideo")
#             - licitaciones_empresa_uruguay(nombre_empresa="ANTEL", metodo="Licitación Pública")
#         """
#         return consultas.licitaciones_empresa(
#             nombre_empresa=nombre_empresa,
#             year=year,
#             comprador=comprador,
#             metodo=metodo,
#             limit=limit,
#         )

#     @mcp.tool()
#     def resumen_empresa_uruguay(
#         nombre_empresa: str,
#         year: int | None = None,
#         comprador: str | None = None,
#         producto: str | None = None,
#         metodo: str | None = None,
#     ) -> DataToolOutput:
#         """Resumen de montos adjudicados a una empresa proveedora del estado uruguayo.
#             Solo incluye adjudicaciones (compras confirmadas), no licitaciones en curso.
#             Muestra montos en pesos uruguayos (UYU) por mes con gráfico de barras
#             stacked desglosado por organismo comprador.
#             Soporta filtros para refinar la consulta: por comprador, producto o método.
#             Si no conoce el nombre exacto, use primero buscar_empresa_uruguay().

#         Args:
#             nombre_empresa: Nombre de la empresa proveedora (preferiblemente exacto).
#             year: Año para filtrar (2024 o 2025). None para todos los años.
#             comprador: Filtrar por organismo comprador (parcial). Ej: "Intendencia de Montevideo".
#             producto: Filtrar por producto/rubro adjudicado (parcial). Ej: "licencia software".
#             metodo: Filtrar por método de contratación (parcial). Valores comunes:
#                 "Compra Directa", "Concurso de Precios", "Licitación Abreviada",
#                 "Licitación Pública", "Compra por Excepción".

#         Returns:
#             str: Resumen con montos UYU por mes desglosados por comprador
#                  (tabla y gráfico de barras stacked), principales compradores
#                  y rubros adjudicados.

#         Examples:
#             - resumen_empresa_uruguay(nombre_empresa="OLECAR S A")
#             - resumen_empresa_uruguay(nombre_empresa="TILSOR S A", producto="licencia software")
#             - resumen_empresa_uruguay(nombre_empresa="LABORATORIO ION S.A.", comprador="Hospital Maciel")
#             - resumen_empresa_uruguay(nombre_empresa="OLECAR S A", metodo="Licitación Abreviada")
#         """
#         return consultas.resumen_empresa(
#             nombre_empresa=nombre_empresa,
#             year=year,
#             comprador=comprador,
#             producto=producto,
#             metodo=metodo,
#         )

#     @mcp.tool()
#     def compras_producto_uruguay(
#         producto: str, year: int | None = None, limit: int = 20
#     ) -> DataToolOutput:
#         """Busca qué empresas le venden un producto o servicio al gobierno de Uruguay.
#             Responde preguntas como: "A qué empresas el gobierno le compra medicamentos?"
#             Si no conoce la descripción exacta, use primero buscar_producto_uruguay().

#         Args:
#             producto: Descripción del producto o servicio a buscar.
#             year: Año para filtrar (2024 o 2025). None para todos los años.
#             limit: Máximo de resultados. Default 20.

#         Returns:
#             str: Lista de proveedores con detalle de comprador, producto, fecha y cantidad.

#         Examples:
#             - compras_producto_uruguay(producto="medicamento")
#             - compras_producto_uruguay(producto="combustible", year=2024)
#             - compras_producto_uruguay(producto="computadora", limit=10)
#         """
#         return consultas.compras_producto(producto=producto, year=year, limit=limit)

#     @mcp.tool()
#     def resumen_producto_uruguay(
#         producto: str,
#         year: int | None = None,
#         proveedor: str | None = None,
#         comprador: str | None = None,
#         metodo: str | None = None,
#         agrupar_por: str = "proveedor",
#     ) -> DataToolOutput:
#         """Resumen de montos adjudicados de un producto o servicio, agrupados por mes.
#             Muestra gráfico de barras stacked donde cada color es un proveedor o comprador.
#             Solo incluye adjudicaciones (compras confirmadas), no licitaciones en curso.
#             Soporta filtros para refinar la consulta: por proveedor, comprador o método.
#             Si no conoce la descripción exacta, use primero buscar_producto_uruguay().

#         Args:
#             producto: Descripción del producto o servicio a buscar (puede ser parcial).
#             year: Año para filtrar (2024 o 2025). None para todos los años.
#             proveedor: Filtrar por empresa proveedora (parcial). Ej: "TILSOR".
#             comprador: Filtrar por organismo comprador (parcial). Ej: "Intendencia de Montevideo".
#             metodo: Filtrar por método de contratación (parcial). Valores comunes:
#                 "Compra Directa", "Concurso de Precios", "Licitación Abreviada",
#                 "Licitación Pública", "Compra por Excepción".
#             agrupar_por: "proveedor" o "comprador". Define cómo se desglosan las barras
#                 del gráfico. Use "proveedor" para ver quién vende (default).
#                 Use "comprador" para ver qué organismos compran.

#         Returns:
#             str: Resumen con montos por mes desglosados por proveedor o comprador
#                  (tabla y gráfico de barras stacked).

#         Examples:
#             - resumen_producto_uruguay(producto="licencia software")
#             - resumen_producto_uruguay(producto="hardware", agrupar_por="comprador")
#             - resumen_producto_uruguay(producto="combustible", proveedor="ANCAP")
#             - resumen_producto_uruguay(producto="medicamento", comprador="Hospital Maciel")
#         """
#         return consultas.resumen_producto(
#             producto=producto,
#             year=year,
#             proveedor=proveedor,
#             comprador=comprador,
#             metodo=metodo,
#             agrupar_por=agrupar_por,
#         )

#     @mcp.tool()
#     def detalle_proceso_uruguay(ocid: str) -> DataToolOutput:
#         """Muestra todos los detalles de un proceso de compra pública por su OCID.
#             Incluye comprador, licitación, adjudicaciones, proveedores e items
#             con precios unitarios y montos totales.
#             Use esta herramienta para responder preguntas específicas sobre un
#             proceso en particular, como "qué se compró exactamente en esa licitación"
#             o "cuánto costó cada item".
#             Los OCID se obtienen de las otras herramientas (licitaciones_empresa_uruguay,
#             compras_producto_uruguay, etc.).

#         Args:
#             ocid: Identificador OCID del proceso. Ej: "ocds-yfs5dr-1307121".

#         Returns:
#             str: Detalle completo del proceso con eventos, licitación, adjudicaciones,
#                  proveedores e items con precios.

#         Examples:
#             - detalle_proceso_uruguay(ocid="ocds-yfs5dr-1307121")
#             - detalle_proceso_uruguay(ocid="ocds-yfs5dr-i481866")
#         """
#         return consultas.detalle_proceso(ocid=ocid)

#     @mcp.tool()
#     def political_questions(country=None) -> DataToolOutput:
#         """To anwer when people ask about political questions that are not answerable with data,
#             but are common questions about Uruguay Government.
#             For example: "Why is X data not available?" or "What did the Gobvernment open this data in this way?"

#         Returns:
#             str: A formatted response

#         Examples:
#             - political_questions()
#         """

#         response = (
#             "Lo siento, pero no puedo responder a esa pregunta. "
#             "Las decisiones gubernamentales relacionadas con la divulgación de datos pueden "
#             "depender de muchos factores, incluyendo consideraciones de privacidad, "
#             "seguridad, recursos disponibles y prioridades políticas. "
#             "Si tienes dudas específicas sobre la disponibilidad de "
#             "determinados datos, te recomiendo que te pongas en contacto directamente "
#             "con las autoridades gubernamentales responsables de la gestión de datos "
#             "en Uruguay para obtener información más detallada."
#         )

#         return response


def main() -> None:
    print("Hello from mcp-ckan-datos-uruguay")
