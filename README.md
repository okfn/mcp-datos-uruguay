# MCP datos Uruguay

Este repositorio contiene definiciones de datasets declarativas para el [portal de datos abiertos de Uruguay](https://catalogodatos.gub.uy/).

**Nota:** Esto es un trabajo en progreso en etapa Alpha.  

## Pruebas
![captura-pruebas](/extras/imgs/capture-ia.png)

Esto está listo para ejecutarse con el servidor OKFN MCP definido en https://github.com/okfn/mcp-ckan.  

## Agregar esto a un servidor OKFN MCP

Agrega este repositorio a la configuración de tu servidor MCP en el archivo `deploy/tool_sources.yaml`, agregando una nueva entrada como esta:

```yaml
  - name: mcp-datos-uruguay
    repo: git@github.com:okfn/mcp-datos-uruguay.git
    # path is the place in which the MCP tools live
    path: datasets
    ref: main
    # if the repo is private, use a key
    # This private key file should be generated with something like
    # ssh-keygen -t ed25519 -f keys/mcp-datos-uruguay-key -N "" -C "deploy@mcp-server"
    # and then add this public key to the GitHub repo's deploy keys (with read access, it'll be enough)
    # This key must be deployed in the MCP server's filesystem at the path specified below, and the private key file must have permissions set to 600 (read/write for owner only)
    key: deploy/keys/mcp-datos-uruguay-key
```

## Cómo funciona

Puedes definir herramientas manuales en Python o crear archivos `.yaml` que definan un dataset y sus herramientas MCP de manera declarativa.  

## Dataset `compras-ocds`: ubicación de la base de datos

El dataset de compras OCDS usa una base SQLite (~1GB) generada a partir de los
zips anuales con `src/mcp_ckan_datos_uruguay/datasets/compras_ocds/load_db.py`.
La base de datos **no se empaqueta**: ambos procesos (carga y consulta)
resuelven la misma ruta, de modo que la base escrita por `load_db.py` sea la
que luego lee el servidor MCP tras `pip install`.


## Agregar un nuevo dataset

1. Crea un nuevo archivo `.yaml` en `datasets/`
2. Configura el `engine` adecuado. Lee sobre ellos en https://github.com/okfn/mcp-ckan/tree/main/src/mcp_server/engines
3. Define los metadatos del `dataset` y la `source` (URL del CSV)
4. Define las `tools` con sus parámetros y lógica
5. Haz push a este repositorio y vuelve a obtener los datos en el servidor MCP
