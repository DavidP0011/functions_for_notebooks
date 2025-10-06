
import pandas as pd
from common.dpm_GCP_ini import _ini_authenticate_API

# __________________________________________________________________________________________________________________________________________________________
# GBQ_tables_schema_df  (FIX: añade scopes obligatorios y opción de override por config)
# __________________________________________________________________________________________________________________________________________________________
def GBQ_tables_schema_df(config: dict) -> pd.DataFrame:
    """
    Retorna un DataFrame con la información de datasets, tablas y campos de un proyecto de BigQuery,
    añadiendo al final las columnas 'fecha_actualizacion_GBQ' (fecha en la que la tabla fue creada o modificada)
    y 'fecha_actualizacion_df' (fecha en la que se creó el DataFrame).

    La autenticación se realiza mediante _ini_authenticate_API(), que ahora recibe explícitamente los SCOPES.
    Puedes sobreescribirlos pasando 'gcp_scopes_list' en config.

    Args:
        config (dict):
            - project_id (str) [requerido]
            - datasets (list) [opcional]
            - include_tables (bool) [opcional] (def. True)
            - ini_environment_identificated (str) [requerido]
            - json_keyfile_GCP_secret_id (str) [según entorno]
            - json_keyfile_colab (str) [según entorno]
            - gcp_scopes_list (list[str]) [opcional]: scopes a usar en la autenticación

    Returns:
        pd.DataFrame con columnas:
            ['project_id','dataset_id','table_name','field_name','field_type',
             'num_rows','num_columns','size_mb','fecha_actualizacion_GBQ','fecha_actualizacion_df']

    Raises:
        ValueError  : Validación o autenticación
        RuntimeError: Errores de extracción/transformación
    """
    import os
    from google.cloud import bigquery
    import pandas as pd

    print("\n🔹🔹🔹 [START ▶️] Inicio del proceso de extracción del esquema de BigQuery 🔹🔹🔹\n", flush=True)

    # ────────────────────────────── VALIDACIÓN INICIAL ──────────────────────────────
    ini_env = config.get("ini_environment_identificated")
    if not ini_env:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en config.")

    project_id_str = (config.get('project_id') or "").strip()
    if not project_id_str:
        raise ValueError("[VALIDATION [ERROR ❌]] El 'project_id' es un argumento requerido en la configuración.")
    print(f"[METRICS [INFO ℹ️]] Proyecto de BigQuery: {project_id_str}", flush=True)

    # ────────────────────────────── SCOPES (FIX PRINCIPAL) ──────────────────────────────
    scopes_list = config.get(
        "gcp_scopes_list",
        [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",  # útil si accedes a BQ con export/import vía Drive
        ],
    )

    # ────────────────────────────── AUTENTICACIÓN ──────────────────────────────
    print("[AUTHENTICATION [START ▶️]] Iniciando autenticación utilizando _ini_authenticate_API()...", flush=True)
    try:
        creds = _ini_authenticate_API(config, project_id_str, scopes_list)
        print("[AUTHENTICATION [SUCCESS ✅]] Autenticación completada.", flush=True)
    except Exception as e:
        raise ValueError(f"[AUTHENTICATION [ERROR ❌]] Error durante la autenticación: {e}")

    # ────────────────────────────── PARÁMETROS DE CONSULTA ──────────────────────────────
    datasets_incluidos_list = config.get('datasets', None)
    include_tables_bool = bool(config.get('include_tables', True))

    # ────────────────────────────── CLIENTE BIGQUERY ──────────────────────────────
    print("[START ▶️] Inicializando cliente de BigQuery...", flush=True)
    try:
        client = bigquery.Client(project=project_id_str, credentials=creds)
        print("[LOAD [SUCCESS ✅]] Cliente de BigQuery inicializado correctamente.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[LOAD [ERROR ❌]] Error al inicializar el cliente de BigQuery: {e}")

    # ────────────────────────────── OBTENCIÓN DE DATASETS ──────────────────────────────
    print("[EXTRACTION [START ▶️]] Obteniendo datasets del proyecto...", flush=True)
    try:
        if datasets_incluidos_list:
            datasets = [client.get_dataset(f"{project_id_str}.{dataset_id}") for dataset_id in datasets_incluidos_list]
            print(f"[EXTRACTION [INFO ℹ️]] Se especificaron {len(datasets_incluidos_list)} datasets para consulta.", flush=True)
        else:
            datasets = list(client.list_datasets(project=project_id_str))
            print(f"[EXTRACTION [INFO ℹ️]] Se encontraron {len(datasets)} datasets en el proyecto.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Error al obtener los datasets: {e}")

    # ────────────────────────────── TABLAS Y CAMPOS ──────────────────────────────
    tables_info_list = []
    for dataset in datasets:
        dataset_id_str = dataset.dataset_id
        full_dataset_id_str = f"{project_id_str}.{dataset_id_str}"
        print(f"\n[EXTRACTION [START ▶️]] Procesando dataset: {full_dataset_id_str}", flush=True)
        if include_tables_bool:
            print(f"[EXTRACTION [START ▶️]] Listando tablas para {full_dataset_id_str}...", flush=True)
            try:
                tables = list(client.list_tables(full_dataset_id_str))
                print(f"[EXTRACTION [SUCCESS ✅]] Se encontraron {len(tables)} tablas en {full_dataset_id_str}.", flush=True)
            except Exception as e:
                print(f"[EXTRACTION [ERROR ❌]] Error al listar tablas en {full_dataset_id_str}: {e}", flush=True)
                continue

            for table_item in tables:
                try:
                    table_ref = client.get_table(table_item.reference)
                    table_name_str = table_item.table_id
                    num_rows_int = table_ref.num_rows
                    num_columns_int = len(table_ref.schema)
                    size_mb_float = table_ref.num_bytes / (1024 * 1024)

                    # fecha actualización (created o modified)
                    fecha_actualizacion_GBQ_str = None
                    if hasattr(table_ref, 'created') and table_ref.created:
                        fecha_actualizacion_GBQ_str = table_ref.created.strftime("%Y-%m-%d %H:%M:%S")
                    elif hasattr(table_ref, 'modified') and table_ref.modified:
                        fecha_actualizacion_GBQ_str = table_ref.modified.strftime("%Y-%m-%d %H:%M:%S")

                    print(f"[METRICS [INFO ℹ️]] Tabla: {table_name_str} | Filas: {num_rows_int} | Cols: {num_columns_int} | Tamaño: {round(size_mb_float,2)} MB", flush=True)
                    if table_ref.schema:
                        for field in table_ref.schema:
                            tables_info_list.append({
                                'project_id': project_id_str,
                                'dataset_id': dataset_id_str,
                                'table_name': table_name_str,
                                'field_name': field.name,
                                'field_type': field.field_type,
                                'num_rows': num_rows_int,
                                'num_columns': num_columns_int,
                                'size_mb': round(size_mb_float, 2),
                                'fecha_actualizacion_GBQ': fecha_actualizacion_GBQ_str
                            })
                    else:
                        tables_info_list.append({
                            'project_id': project_id_str,
                            'dataset_id': dataset_id_str,
                            'table_name': table_name_str,
                            'field_name': None,
                            'field_type': None,
                            'num_rows': num_rows_int,
                            'num_columns': num_columns_int,
                            'size_mb': round(size_mb_float, 2),
                            'fecha_actualizacion_GBQ': fecha_actualizacion_GBQ_str
                        })
                except Exception as e:
                    print(f"[EXTRACTION [ERROR ❌]] Error al procesar la tabla en {full_dataset_id_str}: {e}", flush=True)
        else:
            print(f"[EXTRACTION [INFO ℹ️]] Se omiten las tablas para {full_dataset_id_str}.", flush=True)
            tables_info_list.append({
                'project_id': project_id_str,
                'dataset_id': dataset_id_str,
                'table_name': None,
                'field_name': None,
                'field_type': None,
                'num_rows': None,
                'num_columns': None,
                'size_mb': None,
                'fecha_actualizacion_GBQ': None
            })

    # ────────────────────────────── DATAFRAME ──────────────────────────────
    print("\n[TRANSFORMATION [START ▶️]] Convirtiendo información recopilada a DataFrame...", flush=True)
    try:
        df_tables_fields = pd.DataFrame(tables_info_list)
        print(f"[TRANSFORMATION [SUCCESS ✅]] DataFrame generado exitosamente con {df_tables_fields.shape[0]} registros.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[TRANSFORMATION [ERROR ❌]] Error al convertir la información a DataFrame: {e}")

    # fecha de creación del DataFrame
    df_tables_fields["fecha_actualizacion_df"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n🔹🔹🔹 [END [FINISHED ✅]] Esquema de BigQuery extraído y procesado correctamente. 🔹🔹🔹\n", flush=True)
    return df_tables_fields















import copy
import pandas as pd
from common.dpm_GCP_ini import _ini_authenticate_API

# __________________________________________________________________________________________________________________________________________________________
# GCS_objects_schema_df (antes GCS_tables_schema_df)
# Evita el intento de Secret Manager en entornos sin ADC, sin modificar _ini_authenticate_API()
# __________________________________________________________________________________________________________________________________________________________
def GCS_objects_schema_df(config: dict) -> pd.DataFrame:
    """
    Extrae metadatos de GCS a nivel de bucket y, opcionalmente, a nivel de objeto (blob).

    Estrategia para evitar excepciones/timeout:
    - Se hace una copia de 'config' y, si NO hay ADC disponible (p.ej. Colab/Local),
      se limpia 'json_keyfile_GCP_secret_id' en esa copia, forzando a _ini_authenticate_API()
      a usar keyfile directamente y evitando el intento de Secret Manager.

    Args:
        config (dict):
          - project_id (str) [obligatorio]
          - buckets (list[str]) [opcional]
          - include_objects (bool) [opcional] (def. True)
          - ini_environment_identificated (str) [obligatorio]
          - json_keyfile_local / json_keyfile_colab / json_keyfile_GCP_secret_id [según entorno]
          - gcp_scopes_list (list[str]) [opcional] → scopes a usar (def. devstorage.read_only)

    Returns:
        pd.DataFrame: metadatos de buckets y objetos.
    """
    import os
    from google.cloud import storage
    from google.auth import default as default_auth
    from google.auth.transport.requests import Request

    print("\n🔹🔹🔹 [START ▶️] Inicio del proceso de extracción extendida de GCS 🔹🔹🔹\n", flush=True)

    # ────────────────────────────── VALIDACIÓN ──────────────────────────────
    ini_env = (config.get("ini_environment_identificated") or "").strip()
    if not ini_env:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en config.")

    project_id_str = (config.get('project_id') or "").strip()
    if not project_id_str:
        raise ValueError("[VALIDATION [ERROR ❌]] 'project_id' es obligatorio en la configuración.")
    print(f"[METRICS [INFO ℹ️]] Proyecto de GCP: {project_id_str}", flush=True)

    # ────────────────────────────── SCOPES ──────────────────────────────
    scopes_list = config.get(
        "gcp_scopes_list",
        ["https://www.googleapis.com/auth/devstorage.read_only"]
    )

    # ────────────────────────────── DETECCIÓN ADC ──────────────────────────────
    def _adc_available() -> bool:
        try:
            creds, _ = default_auth(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            if getattr(creds, "requires_scopes", False):
                creds = creds.with_scopes(["https://www.googleapis.com/auth/cloud-platform"])
            # refresh rápido para validar token/metadata server
            creds.refresh(Request())
            return True
        except Exception:
            return False

    # Copia defensiva del config para no mutar el original del usuario
    safe_config = copy.deepcopy(config)

    # Si NO hay ADC (típico en COLAB/LOCAL), limpiamos el secret_id para evitar SM y gRPC noise
    if not _adc_available() and (ini_env.upper() in {"LOCAL", "COLAB"}):
        if safe_config.get("json_keyfile_GCP_secret_id"):
            print("[AUTHENTICATION [INFO ℹ️]] ADC no disponible en este entorno; se omite Secret Manager y se forzará keyfile.", flush=True)
        safe_config["json_keyfile_GCP_secret_id"] = ""  # ← clave del truco

    # ────────────────────────────── AUTENTICACIÓN ──────────────────────────────
    print("[AUTHENTICATION [START ▶️]] Iniciando autenticación utilizando _ini_authenticate_API()...", flush=True)
    try:
        creds = _ini_authenticate_API(safe_config, project_id_str, scopes_list)
        print("[AUTHENTICATION [SUCCESS ✅]] Autenticación completada.", flush=True)
    except Exception as e:
        raise ValueError(f"[AUTHENTICATION [ERROR ❌]] Error durante la autenticación: {e}")

    # ────────────────────────────── PARÁMETROS ──────────────────────────────
    buckets_incluidos_list = safe_config.get('buckets') or None
    include_objects_bool = bool(safe_config.get('include_objects', True))

    # ────────────────────────────── CLIENTE STORAGE ──────────────────────────────
    print("[START ▶️] Inicializando cliente de Google Cloud Storage...", flush=True)
    try:
        storage_client = storage.Client(project=project_id_str, credentials=creds)
        print("[LOAD [SUCCESS ✅]] Cliente de GCS inicializado correctamente.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[LOAD [ERROR ❌]] Error al inicializar el cliente de GCS: {e}")

    # ────────────────────────────── OBTENER BUCKETS ──────────────────────────────
    print("[EXTRACTION [START ▶️]] Obteniendo buckets del proyecto...", flush=True)
    try:
        if buckets_incluidos_list:
            buckets = [storage_client.bucket(b_name) for b_name in buckets_incluidos_list]
            print(f"[EXTRACTION [INFO ℹ️]] Se han especificado {len(buckets_incluidos_list)} buckets para la consulta.", flush=True)
        else:
            buckets = list(storage_client.list_buckets(project=project_id_str))
            print(f"[EXTRACTION [INFO ℹ️]] Se encontraron {len(buckets)} buckets en el proyecto.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Error al obtener los buckets: {e}")

    # ────────────────────────────── Helper: acceso público ──────────────────────────────
    def _is_public(bucket_obj):
        try:
            policy = bucket_obj.get_iam_policy(requested_policy_version=3)
        except Exception:
            return None
        for binding in policy.bindings:
            if "allUsers" in binding.get("members", []) or "allAuthenticatedUsers" in binding.get("members", []):
                return True
        return False

    # ────────────────────────────── RECOPILACIÓN ──────────────────────────────
    gcs_info_list = []
    for bucket_obj in buckets:
        bucket_name_str = bucket_obj.name

        try:
            bucket_obj.reload()
        except Exception as e:
            print(f"[EXTRACTION [WARNING ⚠️]] No se pudieron recargar propiedades para '{bucket_name_str}': {e}", flush=True)

        bp = bucket_obj._properties  # dict crudo
        fecha_creacion_bucket_str = bucket_obj.time_created.strftime("%Y-%m-%d %H:%M:%S") if bucket_obj.time_created else None
        fecha_ultima_modificacion_bucket_str = bp.get("updated")
        tipo_ubicacion = bp.get("locationType")
        ubicacion = bucket_obj.location
        clase_almacenamiento = bucket_obj.storage_class
        acceso_publico_bool = _is_public(bucket_obj)
        ubla = bucket_obj.iam_configuration.get("uniformBucketLevelAccess", {})
        control_acceso_str = "UNIFORM" if ubla.get("enabled", False) else "FINE"
        public_access_prevention = bucket_obj.iam_configuration.get("publicAccessPrevention")
        versioning_enabled = bucket_obj.versioning_enabled
        proteccion_str = f"publicAccessPrevention={public_access_prevention}, versioning={versioning_enabled}"
        retencion_seg = bucket_obj.retention_period
        reglas_ciclo_vida = bucket_obj.lifecycle_rules
        etiquetas_dict = bucket_obj.labels
        pagos_solicitante_bool = bucket_obj.requester_pays
        replication_rpo = bucket_obj.rpo
        encriptacion_str = bucket_obj.default_kms_key_name or "Google-managed"

        if include_objects_bool:
            print(f"\n[EXTRACTION [INFO ℹ️]] Listando objetos en bucket '{bucket_name_str}'...", flush=True)
            try:
                blobs = list(bucket_obj.list_blobs())
                print(f"[EXTRACTION [SUCCESS ✅]] Se encontraron {len(blobs)} objetos en '{bucket_name_str}'.", flush=True)
            except Exception as e:
                print(f"[EXTRACTION [ERROR ❌]] Error al listar objetos en '{bucket_name_str}': {e}", flush=True)
                blobs = []

            for blob in blobs:
                size_mb_float = round((blob.size or 0) / (1024 * 1024), 2)
                if blob.time_created:
                    fecha_actualizacion_GCS_str = blob.time_created.strftime("%Y-%m-%d %H:%M:%S")
                elif blob.updated:
                    fecha_actualizacion_GCS_str = blob.updated.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    fecha_actualizacion_GCS_str = None

                gcs_info_list.append({
                    'project_id': project_id_str,
                    'bucket_name': bucket_name_str,
                    'fecha_creacion_bucket': fecha_creacion_bucket_str,
                    'tipo_ubicacion': tipo_ubicacion,
                    'ubicacion': ubicacion,
                    'clase_almacenamiento_predeterminada': clase_almacenamiento,
                    'fecha_ultima_modificacion_bucket': fecha_ultima_modificacion_bucket_str,
                    'acceso_publico': acceso_publico_bool,
                    'control_acceso': control_acceso_str,
                    'proteccion': proteccion_str,
                    'retencion_buckets': retencion_seg,
                    'reglas_ciclo_vida': str(reglas_ciclo_vida) if reglas_ciclo_vida else None,
                    'etiquetas': str(etiquetas_dict) if etiquetas_dict else None,
                    'pagos_solicitante': pagos_solicitante_bool,
                    'replicacion': replication_rpo,
                    'encriptacion': encriptacion_str,
                    'object_name': blob.name,
                    'content_type': blob.content_type,
                    'size_mb': size_mb_float,
                    'fecha_actualizacion_GCS': fecha_actualizacion_GCS_str,
                })
        else:
            gcs_info_list.append({
                'project_id': project_id_str,
                'bucket_name': bucket_name_str,
                'fecha_creacion_bucket': fecha_creacion_bucket_str,
                'tipo_ubicacion': tipo_ubicacion,
                'ubicacion': ubicacion,
                'clase_almacenamiento_predeterminada': clase_almacenamiento,
                'fecha_ultima_modificacion_bucket': fecha_ultima_modificacion_bucket_str,
                'acceso_publico': acceso_publico_bool,
                'control_acceso': control_acceso_str,
                'proteccion': proteccion_str,
                'retencion_buckets': retencion_seg,
                'reglas_ciclo_vida': str(reglas_ciclo_vida) if reglas_ciclo_vida else None,
                'etiquetas': str(etiquetas_dict) if etiquetas_dict else None,
                'pagos_solicitante': pagos_solicitante_bool,
                'replicacion': replication_rpo,
                'encriptacion': encriptacion_str,
                'object_name': None,
                'content_type': None,
                'size_mb': None,
                'fecha_actualizacion_GCS': None,
            })

    # ────────────────────────────── DATAFRAME ──────────────────────────────
    print("\n[TRANSFORMATION [START ▶️]] Convirtiendo información recopilada en DataFrame...", flush=True)
    try:
        df_gcs = pd.DataFrame(gcs_info_list)
        print(f"[TRANSFORMATION [SUCCESS ✅]] DataFrame generado con {df_gcs.shape[0]} registros.", flush=True)
    except Exception as e:
        raise RuntimeError(f"[TRANSFORMATION [ERROR ❌]] Error al convertir la información a DataFrame: {e}")

    df_gcs["fecha_actualizacion_df"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n🔹🔹🔹 [END [FINISHED ✅]] Esquema extendido de GCS obtenido correctamente. 🔹🔹🔹\n", flush=True)
    return df_gcs

