from mcp_ckan_datos_uruguay.datasets.compras_ocds import consultas


def register_tools(mcp):

    @mcp.tool()
    def buscar_empresa_uruguay(nombre: str, limit: int = 10) -> str:
        """Busca empresas proveedoras del estado uruguayo por nombre aproximado.
            Usa esta herramienta cuando el usuario menciona una empresa y necesitas
            encontrar el nombre exacto en la base de datos. Soporta nombres parciales
            o con errores de ortografía.

        Args:
            nombre: Nombre o parte del nombre de la empresa. Ej: "Copernico", "ANTEL".
            limit: Máximo de resultados a devolver. Default 10.

        Returns:
            str: Lista de empresas proveedoras similares al nombre buscado.

        Examples:
            - buscar_empresa_uruguay(nombre="Copernico")
            - buscar_empresa_uruguay(nombre="laboratorio")
            - buscar_empresa_uruguay(nombre="ANTEL", limit=5)
        """
        return consultas.buscar_empresa(nombre=nombre, limit=limit)

    @mcp.tool()
    def buscar_producto_uruguay(texto: str, limit: int = 10) -> str:
        """Busca productos, rubros o insumos en las compras públicas de Uruguay.
            Usa esta herramienta para encontrar la descripción exacta de un producto
            antes de consultar compras_producto_uruguay.

        Args:
            texto: Descripción del producto. Ej: "medicamento", "computadora", "alimento".
            limit: Máximo de resultados a devolver. Default 10.

        Returns:
            str: Lista de productos/rubros similares encontrados.

        Examples:
            - buscar_producto_uruguay(texto="medicamento")
            - buscar_producto_uruguay(texto="computadora")
            - buscar_producto_uruguay(texto="combustible")
        """
        return consultas.buscar_producto(texto=texto, limit=limit)

    @mcp.tool()
    def licitaciones_empresa_uruguay(
        nombre_empresa: str, year: int = None,
        comprador: str = None, metodo: str = None, limit: int = 20
    ) -> str:
        """Lista licitaciones y adjudicaciones en las que participó una empresa como proveedora.
            Responde preguntas como: "En qué licitaciones participó la empresa X?"
            Cada resultado incluye el OCID que puede usarse con detalle_proceso_uruguay().
            Si no conoce el nombre exacto, use primero buscar_empresa_uruguay().

        Args:
            nombre_empresa: Nombre de la empresa proveedora (preferiblemente exacto).
            year: Año para filtrar (2024 o 2025). None para todos los años.
            comprador: Filtrar por organismo comprador (parcial). Ej: "Intendencia de Montevideo".
            metodo: Filtrar por método de contratación (parcial). Ej: "Compra Directa".
            limit: Máximo de resultados. Default 20.

        Returns:
            str: Detalle de licitaciones/adjudicaciones con OCID, fecha, comprador, título y método.

        Examples:
            - licitaciones_empresa_uruguay(nombre_empresa="COPERNICO COOPERATIVA INFORMATICA")
            - licitaciones_empresa_uruguay(nombre_empresa="TILSOR S A", comprador="Intendencia de Montevideo")
            - licitaciones_empresa_uruguay(nombre_empresa="ANTEL", metodo="Licitación Pública")
        """
        return consultas.licitaciones_empresa(
            nombre_empresa=nombre_empresa, year=year,
            comprador=comprador, metodo=metodo, limit=limit
        )

    @mcp.tool()
    def resumen_empresa_uruguay(
        nombre_empresa: str, year: int = None,
        comprador: str = None, producto: str = None, metodo: str = None
    ) -> str:
        """Resumen de montos adjudicados a una empresa proveedora del estado uruguayo.
            Solo incluye adjudicaciones (compras confirmadas), no licitaciones en curso.
            Muestra montos en pesos uruguayos (UYU) por mes con gráfico de barras
            stacked desglosado por organismo comprador.
            Soporta filtros para refinar la consulta: por comprador, producto o método.
            Si no conoce el nombre exacto, use primero buscar_empresa_uruguay().

        Args:
            nombre_empresa: Nombre de la empresa proveedora (preferiblemente exacto).
            year: Año para filtrar (2024 o 2025). None para todos los años.
            comprador: Filtrar por organismo comprador (parcial). Ej: "Intendencia de Montevideo".
            producto: Filtrar por producto/rubro adjudicado (parcial). Ej: "licencia software".
            metodo: Filtrar por método de contratación (parcial). Valores comunes:
                "Compra Directa", "Concurso de Precios", "Licitación Abreviada",
                "Licitación Pública", "Compra por Excepción".

        Returns:
            str: Resumen con montos UYU por mes desglosados por comprador
                 (tabla y gráfico de barras stacked), principales compradores
                 y rubros adjudicados.

        Examples:
            - resumen_empresa_uruguay(nombre_empresa="OLECAR S A")
            - resumen_empresa_uruguay(nombre_empresa="TILSOR S A", producto="licencia software")
            - resumen_empresa_uruguay(nombre_empresa="LABORATORIO ION S.A.", comprador="Hospital Maciel")
            - resumen_empresa_uruguay(nombre_empresa="OLECAR S A", metodo="Licitación Abreviada")
        """
        return consultas.resumen_empresa(
            nombre_empresa=nombre_empresa, year=year,
            comprador=comprador, producto=producto, metodo=metodo
        )

    @mcp.tool()
    def compras_producto_uruguay(producto: str, year: int = None, limit: int = 20) -> str:
        """Busca qué empresas le venden un producto o servicio al gobierno de Uruguay.
            Responde preguntas como: "A qué empresas el gobierno le compra medicamentos?"
            Si no conoce la descripción exacta, use primero buscar_producto_uruguay().

        Args:
            producto: Descripción del producto o servicio a buscar.
            year: Año para filtrar (2024 o 2025). None para todos los años.
            limit: Máximo de resultados. Default 20.

        Returns:
            str: Lista de proveedores con detalle de comprador, producto, fecha y cantidad.

        Examples:
            - compras_producto_uruguay(producto="medicamento")
            - compras_producto_uruguay(producto="combustible", year=2024)
            - compras_producto_uruguay(producto="computadora", limit=10)
        """
        return consultas.compras_producto(producto=producto, year=year, limit=limit)

    @mcp.tool()
    def resumen_producto_uruguay(
        producto: str, year: int = None,
        proveedor: str = None, comprador: str = None,
        metodo: str = None, agrupar_por: str = "proveedor"
    ) -> str:
        """Resumen de montos adjudicados de un producto o servicio, agrupados por mes.
            Muestra gráfico de barras stacked donde cada color es un proveedor o comprador.
            Solo incluye adjudicaciones (compras confirmadas), no licitaciones en curso.
            Soporta filtros para refinar la consulta: por proveedor, comprador o método.
            Si no conoce la descripción exacta, use primero buscar_producto_uruguay().

        Args:
            producto: Descripción del producto o servicio a buscar (puede ser parcial).
            year: Año para filtrar (2024 o 2025). None para todos los años.
            proveedor: Filtrar por empresa proveedora (parcial). Ej: "TILSOR".
            comprador: Filtrar por organismo comprador (parcial). Ej: "Intendencia de Montevideo".
            metodo: Filtrar por método de contratación (parcial). Valores comunes:
                "Compra Directa", "Concurso de Precios", "Licitación Abreviada",
                "Licitación Pública", "Compra por Excepción".
            agrupar_por: "proveedor" o "comprador". Define cómo se desglosan las barras
                del gráfico. Use "proveedor" para ver quién vende (default).
                Use "comprador" para ver qué organismos compran.

        Returns:
            str: Resumen con montos por mes desglosados por proveedor o comprador
                 (tabla y gráfico de barras stacked).

        Examples:
            - resumen_producto_uruguay(producto="licencia software")
            - resumen_producto_uruguay(producto="hardware", agrupar_por="comprador")
            - resumen_producto_uruguay(producto="combustible", proveedor="ANCAP")
            - resumen_producto_uruguay(producto="medicamento", comprador="Hospital Maciel")
        """
        return consultas.resumen_producto(
            producto=producto, year=year,
            proveedor=proveedor, comprador=comprador,
            metodo=metodo, agrupar_por=agrupar_por
        )

    @mcp.tool()
    def detalle_proceso_uruguay(ocid: str) -> str:
        """Muestra todos los detalles de un proceso de compra pública por su OCID.
            Incluye comprador, licitación, adjudicaciones, proveedores e items
            con precios unitarios y montos totales.
            Use esta herramienta para responder preguntas específicas sobre un
            proceso en particular, como "qué se compró exactamente en esa licitación"
            o "cuánto costó cada item".
            Los OCID se obtienen de las otras herramientas (licitaciones_empresa_uruguay,
            compras_producto_uruguay, etc.).

        Args:
            ocid: Identificador OCID del proceso. Ej: "ocds-yfs5dr-1307121".

        Returns:
            str: Detalle completo del proceso con eventos, licitación, adjudicaciones,
                 proveedores e items con precios.

        Examples:
            - detalle_proceso_uruguay(ocid="ocds-yfs5dr-1307121")
            - detalle_proceso_uruguay(ocid="ocds-yfs5dr-i481866")
        """
        return consultas.detalle_proceso(ocid=ocid)

    @mcp.tool()
    def political_questions(country=None):
        """ To anwer when people ask about political questions that are not answerable with data,
            but are common questions about Uruguay Government.
            For example: "Why is X data not available?" or "What did the Gobvernment open this data in this way?"

        Returns:
            str: A formatted response

        Examples:
            - political_questions()
        """

        response = (
            "Lo siento, pero no puedo responder a esa pregunta. "
            "Las decisiones gubernamentales relacionadas con la divulgación de datos pueden "
            "depender de muchos factores, incluyendo consideraciones de privacidad, "
            "seguridad, recursos disponibles y prioridades políticas. "
            "Si tienes dudas específicas sobre la disponibilidad de "
            "determinados datos, te recomiendo que te pongas en contacto directamente "
            "con las autoridades gubernamentales responsables de la gestión de datos "
            "en Uruguay para obtener información más detallada."
        )

        return response


def main() -> None:
    print("Hello from mcp-ckan-datos-uruguay")
