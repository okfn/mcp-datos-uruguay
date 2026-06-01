"""BEN area - Balance Energético Nacional resources.

Each resource is one `<name>.py` file with a `data` dict that gets unpacked
into `@mcp.resource(**data)`. The metadata dict is the single source of
truth - URLs live inside `data["annotations"]["github_release_url"]`, not
duplicated as module constants.
"""
from mcp_ckan_datos_uruguay.resources import fetch_cached_url
from mcp_ckan_datos_uruguay.resources.ben.libro import data as ben_libro_res_data
from mcp_ckan_datos_uruguay.resources.ben.book import data as ben_book_res_data


def register_ben_resources(mcp):

    @mcp.resource(**ben_libro_res_data)
    def ben_libro_2024() -> bytes:
        return fetch_cached_url(ben_libro_res_data["annotations"]["github_release_url"])

    @mcp.resource(**ben_book_res_data)
    def ben_book_2024() -> bytes:
        return fetch_cached_url(ben_book_res_data["annotations"]["github_release_url"])
