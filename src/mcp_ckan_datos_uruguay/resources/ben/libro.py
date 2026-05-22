data = {
  "uri": "ben/libro-2024.pdf",
  "name": "BEN 2024 - Libro MIEM",
  "description": (
      "Balance Energético Nacional 2024 (MIEM). Documento de referencia "
      "con la metodología, definiciones, factores de conversión y series "
      "anuales del balance energético uruguayo. Útil para entender el "
      "contexto de las tools BEN (matriz eléctrica, potencia instalada, "
      "factor de emisión, etc.)."
  ),
  "mime_type":"application/pdf",
  "annotations": {
      "publisher": "Ministerio de Industria, Energía y Minería (MIEM)",
      "year": 2024,
      "language": "es",
      # BEN url with a download button. There is no direct URL
      "url": "https://documentosben.miem.gub.uy/?language=es&documentType=1&year=2024",
      # So we use GitHub release files so we avoid using a 15MB file in the git history.
      "github_release_url":(
          "https://github.com/okfn/mcp-datos-uruguay/releases/download/"
          "0.1.0/Uruguay-BEN-libro-2024.pdf"
      ),
  },
}
