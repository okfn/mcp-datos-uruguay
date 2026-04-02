

def register_tools(mcp):

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
    print("Hello from mcp-ckan-dados-brasil")
