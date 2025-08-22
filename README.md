# functions\_for\_notebooks

**functions\_for\_notebooks** es un conjunto de utilidades y scripts listos para usar en cuadernos Jupyter o Google Colab. Su objetivo es simplificar tareas habituales de análisis y ETL en la nube, ofreciendo funciones para gestionar ficheros, manejar DataFrames, interactuar con Google Cloud (BigQuery, Storage, Secret Manager), AWS S3 y servicios externos.

## Contenidos

| Módulo/paquete   | Descripción breve                                                                                                                                                                                                            |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`src/utils/`** | Funciones generales: tratamiento de PDFs, manipulación de campos de `pandas`, unión y eliminación de duplicados en DataFrames, copiado de tipos (`dtype`) y carga de datos desde múltiples fuentes.                          |
| **`src/etl/`**   | Herramientas ETL específicas para BigQuery (`dpm_GBQ.py` y `dpm_SQL.py`), Google Cloud Storage (`dpm_GCS.py`), Google Cloud Platform (`dpm_GCP.py`), Amazon S3 (`dpm_S3.py`) y scraping de Box Office Mojo (`dpm_scrap.py`). |
| **`src/apps/`**  | Aplicaciones específicas como `file_admin.py`, que extrae metadatos de archivos de vídeo en rutas locales.                                                                                                                   |

## Instalación

1. **Clonar el repositorio** (si lo utilizarás desde el código fuente):

   ```bash
   git clone https://github.com/DavidP0011/functions_for_notebooks.git
   cd functions_for_notebooks
   ```

2. **Crear un entorno virtual** (opcional pero recomendado):

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # en Windows: .venv\Scripts\activate
   ```

3. **Instalar dependencias**: algunas de las bibliotecas utilizadas son `pandas`, `pandas_gbq`, `PyPDF2`, `google-cloud-bigquery`, `google-cloud-storage`, `phonenumbers`, `beautifulsoup4`, `boto3`, `requests`, `dateparser`… Ajusta la instalación a tus necesidades:

   ```bash
   pip install pandas pandas-gbq PyPDF2 google-cloud-bigquery \
               google-cloud-storage google-cloud-secret-manager \
               phonenumbers beautifulsoup4 boto3 requests dateparser
   ```

   > **Nota:** En versiones futuras estas dependencias se incluirán en el archivo `pyproject.toml` para una instalación automatizada.

4. **Instalar el paquete localmente**:

   ```bash
   pip install -e .
   ```

   Esto permitirá que puedas importar los módulos desde cualquier cuaderno sin modificar la variable `PYTHONPATH`.

## Uso rápido

A continuación se muestran algunos ejemplos de uso. Para más detalles consulta los docstrings de cada función.

### Unir páginas de dos PDFs

```python
from functions_for_notebooks.utils.dpm_pdf_utils import pdf_merge_intercalated_pages_file

config = {
    "impares_path": "path/al/pdf_impar.pdf",
    "pares_path": "path/al/pdf_par.pdf",
    "output_path": "salida/archivo_combinado.pdf"
}
pdf_merge_intercalated_pages_file(config)
```

### Formatear nombres de campos y consolidar duplicados

```python
from functions_for_notebooks.utils.dpm_tables import fields_name_format, tables_consolidate_duplicates_df
import pandas as pd

# Formatear nombres de campos
config_format = {
    "fields_name_raw_list": ["Primer Nombre", "apellido-materno", "ID Cliente"],
    "formato_final": "snake_case",
    "reemplazos": {"Nombre": "nombre"},
    "siglas": ["ID"]
}
df_names = fields_name_format(config_format)
print(df_names)

# Consolidar duplicados de dos DataFrames
df_initial = pd.DataFrame([{"id":1, "valor":100}, {"id":2, "valor":200}])
df_to_merge = pd.DataFrame([{"id":2, "valor":300}, {"id":3, "valor":400}])
config_dup = {
    "df_initial": df_initial,
    "df_to_merge": df_to_merge,
    "id_fields": ["id"],
    "duplicate_policy": "keep_newest",
    "duplicate_date_field": None
}
df_merged = tables_consolidate_duplicates_df(config_dup)
```

### Eliminar todas las tablas de un dataset de BigQuery

```python
from functions_for_notebooks.etl.dpm_GBQ import GBQ_delete_tables

config = {
    "project_id": "tu-proyecto",
    "dataset_ids": ["dataset_temp"],
    "ini_environment_identificated": "LOCAL",
    "json_keyfile_local": "/ruta/credenciales.json",
}
GBQ_delete_tables(config)
```

### Cargar datos desde GCS a BigQuery

```python
from functions_for_notebooks.etl.dpm_GCS import GCS_files_to_GBQ

params = {
    "gcp_project_id": "tu-proyecto",
    "gcs_bucket_name": "mi-bucket",
    "gbq_dataset_id": "mi_dataset",
    "files_list": ["carpeta/data.csv"],
    "json_keyfile_colab": "/ruta/credenciales.json"
}
GCS_files_to_GBQ(params)
```

## Autenticación en Google Cloud

Muchas funciones requieren credenciales de Google Cloud. El parámetro `ini_environment_identificated` determina de dónde se leen:

* `"LOCAL"`: utiliza un `json_keyfile_local` con una cuenta de servicio.
* `"COLAB"`: utiliza `json_keyfile_colab` para trabajar en Google Colab.
* `"COLAB_ENTERPRISE"` o un ID de proyecto: utiliza `json_keyfile_GCP_secret_id`, que debe existir en Secret Manager.

Asegúrate de conceder permisos adecuados a las cuentas de servicio para BigQuery, GCS, Secret Manager y otros servicios utilizados.

## Contribuciones

Se agradecen contribuciones de código, documentación y sugerencias. Algunas mejoras prioritarias:

* Declarar dependencias en `pyproject.toml`.
* Dividir los módulos demasiado largos en submódulos más pequeños.
* Sustituir las funciones de impresión por el uso del módulo `logging` de Python.
* Añadir pruebas unitarias y notebooks de ejemplo.

## Licencia

Este proyecto se distribuye bajo licencia MIT. Consulta el archivo `LICENSE` para más detalles.
