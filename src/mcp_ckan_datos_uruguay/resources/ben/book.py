data = {
  "uri": "ben/book-2024.pdf",
  "name": "BEN 2024 - MIEM Book",
  "description": (
      "Uruguay National Energy Balance 2024 (MIEM). Reference document "
      "with the methodology, definitions, conversion factors and annual "
      "time series of the Uruguayan energy balance. Useful for "
      "understanding the context of the BEN tools (electricity mix, "
      "installed capacity, emission factor, etc.)."
  ),
  "mime_type":"application/pdf",
  "annotations": {
      "publisher": "Ministry of Industry, Energy and Mining (MIEM)",
      "year": 2024,
      "language": "en",
      # BEN url with a download button. There is no direct URL
      "url": "https://documentosben.miem.gub.uy/?language=en&documentType=1&year=2024",
      # So we use GitHub release files so we avoid using a 15MB file in the git history.
      "github_release_url":(
          "https://github.com/okfn/mcp-datos-uruguay/releases/download/"
          "0.1.0/Uruguay-BEN-book-2024.pdf"
      ),
  },
}
