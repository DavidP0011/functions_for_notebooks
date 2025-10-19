from common.dpm_GCP_ini import _ini_authenticate_API

from google.cloud import bigquery
import pandas as pd
import pandas_gbq

import unicodedata
import re
import time
import os
import io
import json
from typing import Dict, Any



# ----------------------------------------------------------------------------
# fields_name_format()
# ----------------------------------------------------------------------------
def fields_name_format(config):
    """
    Formatea nombres de campos de datos según configuraciones específicas.
    
    Parámetros en config:
      - fields_name_raw_list (list): Lista de nombres de campos.
      - formato_final (str, opcional): 'CamelCase', 'snake_case', 'Sentence case', o None.
      - reemplazos (dict, opcional): Diccionario de términos a reemplazar.
      - siglas (list, opcional): Lista de siglas que deben mantenerse en mayúsculas.
    
    Retorna:
        pd.DataFrame: DataFrame con columnas 'Campo Original' y 'Campo Formateado'.
    """
    print("[START 🚀] Iniciando formateo de nombres de campos...", flush=True)
    
    def aplicar_reemplazos(field, reemplazos):
        for key, value in sorted(reemplazos.items(), key=lambda x: -len(x[0])):
            if key in field:
                field = field.replace(key, value)
        return field

    def formatear_campo(field, formato, siglas):
        if formato is None or formato is False:
            return field
        words = [w for w in re.split(r'[_\-\s]+', field) if w]
        if formato == 'CamelCase':
            return ''.join(
                word.upper() if word.upper() in siglas
                else word.capitalize() if idx == 0
                else word.lower()
                for idx, word in enumerate(words)
            )
        elif formato == 'snake_case':
            return '_'.join(
                word.upper() if word.upper() in siglas
                else word.lower() for word in words
            )
        elif formato == 'Sentence case':
            return ' '.join(
                word.upper() if word.upper() in siglas
                else word.capitalize() if idx == 0
                else word.lower()
                for idx, word in enumerate(words)
            )
        else:
            raise ValueError(f"Formato '{formato}' no soportado.")
    
    resultado = []
    for field in config.get('fields_name_raw_list', []):
        original_field = field
        field = aplicar_reemplazos(field, config.get('reemplazos', {}))
        formatted_field = formatear_campo(field, config.get('formato_final', 'CamelCase'), [sig.upper() for sig in config.get('siglas', [])])
        resultado.append({'Campo Original': original_field, 'Campo Formateado': formatted_field})
    
    df_result = pd.DataFrame(resultado)
    print("[END [FINISHED 🏁]] Formateo de nombres completado.\n", flush=True)
    return df_result









# ----------------------------------------------------------------------------
# tables_consolidate_duplicates_df()
# ----------------------------------------------------------------------------
def tables_consolidate_duplicates_df(config_dic: dict):
    """
    Consolida dos ``pandas.DataFrame`` resolviendo duplicados de acuerdo con la
    política solicitada.

    Args:
        config_dic (dict):
            - validate_df_schemas_match (bool, opcional): Valida esquemas (def. True).
            - df_initial (pd.DataFrame): DataFrame base con prioridad.
            - df_to_merge (pd.DataFrame): DataFrame a fusionar.
            - id_fields (list[str]): Campos clave para identificar registros.
            - duplicate_policy (str): ``keep_newest`` | ``keep_oldest`` |
              ``keep_df_initial`` | ``keep_df_to_merge``.
            - duplicate_date_field (str, opcional): Campo fecha para políticas
              basadas en tiempo.
            - duplicate_date_field_format_str (str, opcional): Formato
              ``datetime.strptime`` de *duplicate_date_field*.
            - return_metadata (bool, opcional): Devuelve metadatos del proceso.

    Returns:
        pd.DataFrame | tuple[pd.DataFrame, dict]: DataFrame consolidado y,
        opcionalmente, un diccionario de metadatos.

    Raises:
        ValueError: Configuración errónea o esquemas distintos.
        TypeError : *df_initial* o *df_to_merge* no son ``pd.DataFrame``.
    """
    # ──────────────────────────
    # Imports locales mínimos
    # ──────────────────────────
    import pandas as pd  # type: ignore
    from datetime import datetime

    # ──────────────────────────
    # 1️⃣ Extracción y validación de parámetros
    # ──────────────────────────
    allowed_policies_set = {
        "keep_newest",
        "keep_oldest",
        "keep_df_initial",
        "keep_df_to_merge",
    }

    validate_schema_bool: bool = config_dic.get("validate_df_schemas_match", True)
    df_initial_df = config_dic.get("df_initial")
    df_to_merge_df = config_dic.get("df_to_merge")
    id_fields_list: list[str] = config_dic.get("id_fields", [])
    policy_str: str = config_dic.get("duplicate_policy", "keep_newest")
    date_col_str: str | None = config_dic.get("duplicate_date_field")
    date_fmt_str: str | None = config_dic.get("duplicate_date_field_format_str")
    return_meta_bool: bool = config_dic.get("return_metadata", False)

    print(
        f"[CONSOLIDATION START ▶️] {datetime.now().isoformat(timespec='seconds')}",
        flush=True,
    )
    print(f"[INFO ℹ️] id_fields={id_fields_list} | policy={policy_str}", flush=True)

    # Validaciones básicas
    if policy_str not in allowed_policies_set:
        raise ValueError(
            f"[VALIDATION [ERROR ❌]] "
            f"'duplicate_policy' debe ser uno de {allowed_policies_set}"
        )

    if not isinstance(df_initial_df, pd.DataFrame) or not isinstance(
        df_to_merge_df, pd.DataFrame
    ):
        raise TypeError(
            "[VALIDATION [ERROR ❌]] 'df_initial' y 'df_to_merge' deben ser DataFrame"
        )

    if not id_fields_list:
        raise ValueError("[VALIDATION [ERROR ❌]] 'id_fields' no puede ser vacío")

    for col_str in id_fields_list:
        if col_str not in df_initial_df.columns or col_str not in df_to_merge_df.columns:
            raise ValueError(
                f"[VALIDATION [ERROR ❌]] Columna clave '{col_str}' ausente en "
                "alguno de los DataFrames"
            )

    if policy_str in ("keep_newest", "keep_oldest"):
        if not date_col_str:
            raise ValueError(
                "[VALIDATION [ERROR ❌]] "
                "'duplicate_date_field' es obligatorio para políticas basadas en fecha"
            )
        if (
            date_col_str not in df_initial_df.columns
            or date_col_str not in df_to_merge_df.columns
        ):
            raise ValueError(
                f"[VALIDATION [ERROR ❌]] Campo fecha '{date_col_str}' inexistente "
                "en ambos DataFrames"
            )

    # ──────────────────────────
    # 2️⃣ Validación opcional de esquemas
    # ──────────────────────────
    if validate_schema_bool and set(df_initial_df.columns) != set(df_to_merge_df.columns):
        diff_left_set = set(df_initial_df.columns) - set(df_to_merge_df.columns)
        diff_right_set = set(df_to_merge_df.columns) - set(df_initial_df.columns)
        print(
            f"[VALIDATION [ERROR ❌]] Schemas difieren – "
            f"izquierda: {diff_left_set} | derecha: {diff_right_set}",
            flush=True,
        )
        raise ValueError("Esquemas distintos entre DataFrames")

    # ──────────────────────────
    # 3️⃣ Normalización opcional de campos‑id (espacios, mayúsculas)
    #     Evita duplicados encubiertos por diferencias de formato
    # ──────────────────────────
    def _normalise_id_fields(df: pd.DataFrame) -> None:
        for col in id_fields_list:
            if pd.api.types.is_string_dtype(df[col]):
                df[col] = (
                    df[col]
                    .astype(str, copy=False)
                    .str.strip()
                    .str.casefold()  # ignore‑case
                )

    _normalise_id_fields(df_initial_df)
    _normalise_id_fields(df_to_merge_df)

    # ──────────────────────────
    # 4️⃣ Concatenar y resolver duplicados
    # ──────────────────────────
    df_all_df = pd.concat([df_initial_df, df_to_merge_df], ignore_index=True)
    duplicates_before_int: int = len(df_all_df)

    if policy_str in ("keep_newest", "keep_oldest"):
        # Analizar fecha solo una vez y conservarla como helper
        df_all_df["_parsed_date"] = pd.to_datetime(
            df_all_df[date_col_str], format=date_fmt_str, errors="coerce"
        )

        # Orden estable (mergesort) para resolver empates de forma determinista
        ascending_date_bool = policy_str == "keep_oldest"
        df_all_df.sort_values(
            by=id_fields_list + ["_parsed_date"],
            ascending=[True] * len(id_fields_list) + [ascending_date_bool],
            inplace=True,
            kind="mergesort",
        )

        # Primer registro por grupo es el más nuevo o el más antiguo según orden
        result_df = (
            df_all_df.drop_duplicates(subset=id_fields_list, keep="first")
            .copy()
            .drop(columns="_parsed_date")
        )
    else:
        # El orden de concatenación decide la prioridad
        ordered_df_list = (
            [df_initial_df, df_to_merge_df]
            if policy_str == "keep_df_initial"
            else [df_to_merge_df, df_initial_df]
        )
        df_all_df = pd.concat(ordered_df_list, ignore_index=True)

        # Orden estable ⇒ se queda el primer registro de cada grupo
        result_df = df_all_df.drop_duplicates(
            subset=id_fields_list, keep="first"
        ).copy()

    result_df.reset_index(drop=True, inplace=True)

    # ──────────────────────────
    # 5️⃣ Alinear dtypes al esquema original
    # ──────────────────────────
    for col_str, dtype in df_initial_df.dtypes.items():
        try:
            result_df[col_str] = result_df[col_str].astype(dtype, copy=False)
        except (ValueError, TypeError):
            print(
                f"[TYPE WARNING ⚠️] No se pudo convertir '{col_str}' a {dtype}",
                flush=True,
            )

    # ──────────────────────────
    # 6️⃣ Metadatos y salida
    # ──────────────────────────
    duplicates_resolved_int = duplicates_before_int - len(result_df)
    print(
        f"[FINISHED ✅] registros finales={len(result_df)} | "
        f"duplicados_resueltos={duplicates_resolved_int}",
        flush=True,
    )

    if return_meta_bool:
        metadata_dic: dict = {
            "timestamp": datetime.now(),
            "initial_records": len(df_initial_df),
            "merge_records": len(df_to_merge_df),
            "final_records": len(result_df),
            "duplicates_resolved": duplicates_resolved_int,
            "records_added": len(result_df) - len(df_initial_df),
        }
        return result_df, metadata_dic

    return result_df















# ----------------------------------------------------------------------------
# DType_df_to_df()
# ----------------------------------------------------------------------------
def DType_df_to_df(config: Dict[str, Any]):
    """Copia *dtypes* entre DataFrames con coerción robusta.

    Claves flexibles admitidas en el `config`:
    - ``reference_dtype_df`` / ``source_df`` → DataFrame cuya *signature* de `dtypes` actúa de referencia.
    - ``target_dtype_df`` / ``targete_dtype_df`` / ``target_df`` → DataFrame al que se le aplicarán los dtypes.

    Parámetros opcionales:
    ---------------------
    inplace : bool  (default ``True``)
        Si *True*, muta el ``target`` *in‑place*; si *False* trabaja con una copia.
    return_metadata : bool  (default ``True``)
        Devuelve un segundo objeto con información de columnas casteadas / fallidas / omitidas.
    decimal_comma : bool  (default ``True``)
        Pre‑procesa strings reemplazando `"," → "."` antes de la coerción numérica.

    Returns
    -------
    pd.DataFrame | Tuple[pd.DataFrame, dict]
        El DataFrame transformado y, opcionalmente, un diccionario con metadatos.
    """
    import pandas as pd
    import numpy as np
    from typing import Dict, Any, List, Tuple

    # --------------------- VALIDACIÓN DE ENTRADA ---------------------
    source_df = config.get("reference_dtype_df", config.get("source_df"))
    target_df = config.get("target_dtype_df", config.get("targete_dtype_df", config.get("target_df")))

    if source_df is None or target_df is None:
        raise ValueError("[VALIDATION ❌] Debes proporcionar 'reference_dtype_df/source_df' y 'target_dtype_df/target_df'.")
    if not isinstance(source_df, pd.DataFrame):
        raise ValueError("[VALIDATION ❌] 'reference_dtype_df' no es DataFrame.")
    if not isinstance(target_df, pd.DataFrame):
        raise ValueError("[VALIDATION ❌] 'target_dtype_df' no es DataFrame.")

    inplace: bool = config.get("inplace", True)
    return_metadata: bool = config.get("return_metadata", True)
    decimal_comma: bool = config.get("decimal_comma", True)

    if not inplace:
        target_df = target_df.copy()

    print("🔹🔹🔹 [START ▶️] DTYPE COPY", flush=True)

    # ------------------------ LÓGICA PRINCIPAL ------------------------
    common_cols: List[str] = [c for c in source_df.columns if c in target_df.columns]
    if not common_cols:
        print("[DTYPE COPY ⚠️] No hay columnas coincidentes.", flush=True)
        empty_meta = {"cols_casted": [], "cols_failed": [], "cols_skipped": []}
        return (target_df, empty_meta) if return_metadata else target_df

    cols_casted, cols_failed, cols_skipped = [], [], []

    def _safe_cast(col: pd.Series, tgt_dtype) -> Tuple[pd.Series, bool]:
        """Intenta castear la *Series* a `tgt_dtype`.

        Estrategia:
        1. Intento directo ``astype``.
        2. Si falla y `tgt_dtype` es numérico → ``pd.to_numeric`` con coerción, gestionando
           *nullable integer* cuando hay *NaN*.
        3. Si falla y es fecha → ``pd.to_datetime``.
        4. *Fallback* → string; marca la conversión como fallida.
        """
        # ── Paso 0: pre‑procesar comas decimales ────────────────────
        series_proc = col.astype(str).str.replace(",", ".", regex=False) if decimal_comma else col

        # ── Paso 1: intento directo ─────────────────────────────────
        try:
            return col.astype(tgt_dtype), True
        except Exception:
            pass  # continuará con coerciones especializadas

        # ── Paso 2: coerción numérica ───────────────────────────────
        if pd.api.types.is_numeric_dtype(tgt_dtype):
            coerced = pd.to_numeric(series_proc, errors="coerce")
            if pd.api.types.is_integer_dtype(tgt_dtype):
                # Si existen NaN y el dtype destino es entero ⇒ usar Int64 (nullable)
                if coerced.isna().any():
                    return coerced.astype("Int64"), not coerced.isna().all()
                # sin NaNs: redondea y castea al entero exacto
                return coerced.round().astype(tgt_dtype), True
            # destino float
            return coerced.astype("float64"), not coerced.isna().all()

        # ── Paso 3: coerción de fechas ───────────────────────────────
        try:
            if np.issubdtype(tgt_dtype, np.datetime64):
                coerced_dt = pd.to_datetime(series_proc, errors="coerce")
                return coerced_dt, not coerced_dt.isna().all()
        except Exception:
            pass

        # ── Paso 4: fallback a string ───────────────────────────────
        return series_proc.astype(str), False

    total = len(common_cols)
    for idx, col in enumerate(common_cols, start=1):
        tgt_dtype = source_df[col].dtype
        if target_df[col].dtype == tgt_dtype:
            print(f"[DTYPE COPY ℹ️] ({idx}/{total}) '{col}' ya es {tgt_dtype}. Skipped.", flush=True)
            cols_skipped.append(col)
            continue

        target_df[col], ok = _safe_cast(target_df[col], tgt_dtype)
        if ok:
            print(f"[DTYPE COPY ✅] ({idx}/{total}) '{col}' → {target_df[col].dtype}.", flush=True)
            cols_casted.append(col)
        else:
            print(f"[DTYPE COPY ⚠️] ({idx}/{total}) No se pudo castear '{col}' → {tgt_dtype}.", flush=True)
            cols_failed.append(col)

    print(
        f"[DTYPE COPY ✔️] Cast fin — ok: {len(cols_casted)}/{total} | "
        f"fail: {len(cols_failed)} | skipped: {len(cols_skipped)}",
        flush=True,
    )

    meta = {
        "cols_casted": cols_casted,
        "cols_failed": cols_failed,
        "cols_skipped": cols_skipped,
    }

    return (target_df, meta) if return_metadata else target_df



















from common.dpm_GCP_ini import _ini_authenticate_API
# ----------------------------------------------------------------------------
# table_various_sources_to_DF()  — v2.1 (auth silenciosa + tipos robustos)
# ----------------------------------------------------------------------------
def table_various_sources_to_DF(params: dict):
    """
    Extrae datos desde Archivo/Sheets/BigQuery/GCS y devuelve un DataFrame.
    • Usa keyfile si está disponible (silencia errores de metadata/secret).
    • Normalización opcional de encabezados.
    • Conversión robusta de fechas y numéricos.
    """

    import os
    import re
    import io
    import time
    import unicodedata
    import pandas as pd

    # Dependencias opcionales
    try:
        from google.cloud import bigquery
    except Exception:
        bigquery = None
    try:
        from google.colab import files as colab_files
    except Exception:
        colab_files = None

    # ───────────── Utilidades ─────────────
    def _imprimir_encabezado(msg: str) -> None:
        print(f"\n[START ▶️] {msg}\n", flush=True)

    def _metricas(df: pd.DataFrame, origen: str, t0: float) -> None:
        try:
            print("\n[METRICS [INFO 📊]] INFORME ESTADÍSTICO DEL DATAFRAME", flush=True)
            print(f"  • Origen: {origen}", flush=True)
            print(f"  • Filas: {df.shape[0]} | Columnas: {df.shape[1]}", flush=True)
            print("  • dtypes:", flush=True); print(df.dtypes, flush=True)
            print(f"  • Tiempo total: {time.time()-t0:0.2f}s", flush=True)
            print("[END [FINISHED ✅]] Proceso completado con éxito.\n", flush=True)
        except Exception as e:
            print(f"[METRICS [WARNING ⚠️]] {e}", flush=True)

    def _es_fuente_archivo(p): return bool(p.get('file_source_table_path', '').strip())
    def _es_fuente_gsheet(p): return bool(p.get('spreadsheet_source_table_id', '').strip()) and bool(p.get('spreadsheet_source_table_worksheet_name', '').strip())
    def _es_fuente_gbq(p):    return bool(p.get('GBQ_source_table_name', '').strip())
    def _es_fuente_gcs(p):    return bool(p.get('GCS_source_table_bucket_name', '').strip()) and bool(p.get('GCS_source_table_file_path', '').strip())

    def _validar(p: dict) -> None:
        if _es_fuente_archivo(p): return
        if _es_fuente_gsheet(p) or _es_fuente_gbq(p) or _es_fuente_gcs(p):
            if not p.get('ini_environment_identificated'):
                raise ValueError("[VALIDATION [ERROR ❌]] Falta 'ini_environment_identificated'.")
            if not (p.get('json_keyfile_local') or p.get('json_keyfile_colab') or p.get('json_keyfile_GCP_secret_id')):
                raise ValueError("[VALIDATION [ERROR ❌]] Falta una credencial: keyfile (local/colab) o secret.")
        else:
            raise ValueError("[VALIDATION [ERROR ❌]] Indica exactamente UNA fuente válida.")

    def _apply_common_filters(df: pd.DataFrame, p: dict) -> pd.DataFrame:
        r0, r1 = p.get('source_table_row_start', 0), p.get('source_table_row_end', None)
        c0, c1 = p.get('source_table_col_start', 0), p.get('source_table_col_end', None)
        df = df.iloc[r0:r1] if r1 is not None else df.iloc[r0:]
        df = df.iloc[:, c0:c1] if c1 is not None else df.iloc[:, c0:]
        fields = p.get('source_table_fields_list') or []
        if fields:
            keep = [f for f in fields if f in df.columns]
            if keep: df = df[keep]
        return df

    def _strip_cells(df: pd.DataFrame, enable: bool) -> pd.DataFrame:
        if not enable: return df
        for c in df.select_dtypes(include=['object', 'string']).columns:
            try: df[c] = df[c].apply(lambda x: x.strip() if isinstance(x, str) else x)
            except Exception as e: print(f"[TRANSFORMATION [WARNING ⚠️]] strip '{c}': {e}", flush=True)
        return df

    def _normalize_header(name: str, style: str = 'forms') -> str:
        s = unicodedata.normalize('NFKD', name)
        s = "".join([c for c in s if not unicodedata.combining(c)]).strip()
        s = re.sub(r'\s+', ' ', s)
        s = s.lower()
        if style == 'snake':
            s = re.sub(r'[^a-z0-9]+', '_', s)
        elif style == 'slug':
            s = re.sub(r'[^a-z0-9]+', '-', s)
        else:  # forms
            s = re.sub(r'[^a-z0-9_ ]+', '', s).replace(' ', '_')
        return re.sub(r'[_-]+', lambda m: m.group(0)[0], s).strip('_-')

    def _normalize_headers(df: pd.DataFrame, use: bool, style: str) -> pd.DataFrame:
        if not use: return df
        mapping, seen = {}, set()
        for col in df.columns:
            nc, base, k = _normalize_header(str(col), style), None, 1
            base = nc
            while nc in seen:
                k += 1; nc = f"{base}_{k}"
            mapping[col] = nc; seen.add(nc)
        df = df.rename(columns=mapping)
        print(f"[TRANSFORMATION [SUCCESS ✅]] Encabezados normalizados (estilo='{style}').", flush=True)
        return df

    def _auto_convert(df: pd.DataFrame, auto_dates: bool, auto_numbers: bool) -> pd.DataFrame:
        try: df = df.convert_dtypes()
        except Exception: pass

        # 1) Heurística de fechas por nombre
        if auto_dates:
            for col in df.columns:
                cl = str(col).lower()
                if any(k in cl for k in ("fecha", "date", "day", "dt", "updated_at", "created_at")):
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                    except Exception as e:
                        print(f"[TRANSFORMATION [WARNING ⚠️]] datetime '{col}': {e}", flush=True)

        # 2) Heurística de números por nombre
        if auto_numbers:
            num_keys = ("importe","monto","valor","precio","saldo","cantidad","total","roas","cpl","ctr","cpc","size","rows","columns","num_")
            for col in df.columns:
                cl = str(col).lower()
                is_candidate = any(k in cl for k in num_keys)
                if is_candidate:
                    try:
                        def _to_float(x):
                            if isinstance(x, str):
                                xs = x.strip()
                                if xs == "": return None
                                # EU: 1.234,56  |  US: 1,234.56
                                if xs.count(',') == 1 and xs.count('.') > 1:
                                    xs = xs.replace('.', '').replace(',', '.')
                                elif xs.count(',') > 1 and xs.count('.') == 1:
                                    xs = xs.replace(',', '')
                                elif xs.count(',') == 1 and xs.count('.') == 0:
                                    xs = xs.replace(',', '.')
                                else:
                                    xs = xs.replace(' ', '')
                                return float(xs)
                            return x
                        df[col] = df[col].map(_to_float)
                    except Exception as e:
                        print(f"[TRANSFORMATION [WARNING ⚠️]] numeric '{col}': {e}", flush=True)

        # 3) Conversión numérica por densidad (>=80% casteable) — p.ej. num_rows/size_mb si no cazan por nombre
        if auto_numbers:
            for col in df.columns:
                s = df[col]
                if pd.api.types.is_numeric_dtype(s): continue
                try:
                    cleaned = s.dropna().astype(str).str.strip()
                    if len(cleaned) == 0: continue
                    # Intento robusto
                    tmp = cleaned.str.replace(r'\s', '', regex=True)
                    tmp = tmp.str.replace(r'\.(?=[0-9]{3}\b)', '', regex=True)  # separador miles .
                    tmp = tmp.str.replace(r',(?=[0-9]{3}\b)', '', regex=True)  # separador miles ,
                    tmp = tmp.str.replace(',', '.')  # coma decimal
                    casted = pd.to_numeric(tmp, errors='coerce')
                    ratio = casted.notna().mean()
                    if ratio >= 0.8:
                        df[col] = pd.to_numeric(tmp, errors='coerce')
                except Exception:
                    pass
        return df

    # ───────────── Auth (silenciosa y predecible) ─────────────
    def _get_project_id(p: dict) -> str:
        ie = p.get("ini_environment_identificated")
        if not ie: raise ValueError("[VALIDATION [ERROR ❌]] Falta 'ini_environment_identificated'.")
        return os.environ.get("GOOGLE_CLOUD_PROJECT") if ie == "COLAB_ENTERPRISE" else ie

    def _auth_with_scopes(p: dict, scopes: list):
        """
        • Prioriza keyfile si está presente (local/colab) → evita metadata/secret y ruido.
        • Si no hay keyfile, usa _ini_authenticate_API(p, project_id, scopes).
        """
        from google.oauth2 import service_account
        key_local = p.get("json_keyfile_local")
        key_colab = p.get("json_keyfile_colab")
        try:
            if key_local and isinstance(key_local, str) and os.path.exists(key_local):
                creds = service_account.Credentials.from_service_account_file(key_local, scopes=scopes)
                print(f"[AUTHENTICATION [SUCCESS ✅]] Credenciales desde keyfile: {key_local}", flush=True)
                return creds
            if isinstance(key_local, dict) and "client_email" in key_local:
                creds = service_account.Credentials.from_service_account_info(key_local, scopes=scopes)
                print("[AUTHENTICATION [SUCCESS ✅]] Credenciales desde dict local.", flush=True)
                return creds
            if key_colab and isinstance(key_colab, str) and os.path.exists(key_colab):
                creds = service_account.Credentials.from_service_account_file(key_colab, scopes=scopes)
                print(f"[AUTHENTICATION [SUCCESS ✅]] Credenciales desde keyfile (colab): {key_colab}", flush=True)
                return creds
            if isinstance(key_colab, dict) and "client_email" in key_colab:
                creds = service_account.Credentials.from_service_account_info(key_colab, scopes=scopes)
                print("[AUTHENTICATION [SUCCESS ✅]] Credenciales desde dict colab.", flush=True)
                return creds
        except Exception as e:
            print(f"[AUTHENTICATION [WARNING ⚠️]] Keyfile presente pero no utilizable: {e}", flush=True)

        # Fallback: helper (puede intentar Secret Manager/ADC)
        project_id = _get_project_id(p)
        try:
            creds = _ini_authenticate_API(p, project_id, scopes)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales obtenidas por helper.", flush=True)
            return creds
        except Exception as e:
            raise RuntimeError(f"[AUTHENTICATION [ERROR ❌]] {e}")

    # ───────────── Lectores ─────────────
    def _leer_archivo(p: dict) -> pd.DataFrame:
        _imprimir_encabezado("Cargando archivo local")
        file_path = p.get('file_source_table_path')
        skip_empty = p.get('source_table_filter_skip_row_empty_use', True)
        if not file_path:
            if colab_files is None:
                raise RuntimeError("[EXTRACTION [ERROR ❌]] Sin ruta de archivo y no estamos en Colab.")
            print("[EXTRACTION [INFO ℹ️]] Selecciona un archivo…", flush=True)
            up = colab_files.upload()
            file_path = list(up.keys())[0]
            file_input = io.BytesIO(up[file_path])
            print(f"[EXTRACTION [SUCCESS ✅]] Subido '{file_path}'.", flush=True)
        else:
            file_input = file_path
        _, ext = os.path.splitext(file_path); ext = ext.lower()
        try:
            print(f"[EXTRACTION [START ⏳]] Leyendo '{file_path}'…", flush=True)
            if ext in ('.xls', '.xlsx'):
                engine = 'xlrd' if ext == '.xls' else 'openpyxl'
                df = pd.read_excel(file_input, engine=engine)
            elif ext == '.csv':
                df = pd.read_csv(file_input, sep=',')
            elif ext == '.tsv':
                df = pd.read_csv(file_input, sep='\t')
            else:
                raise RuntimeError(f"Extensión '{ext}' no soportada.")
            if skip_empty:
                before = len(df); df = df.dropna(how='all')
                print(f"[TRANSFORMATION [SUCCESS ✅]] Filas vacías eliminadas: {before - len(df)}", flush=True)
            return df
        except Exception as e:
            raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Archivo: {e}")

    def _leer_google_sheet(p: dict) -> pd.DataFrame:
        from googleapiclient.discovery import build
        raw = p.get("spreadsheet_source_table_id")
        if "spreadsheets/d/" in str(raw):
            m = re.search(r"/d/([a-zA-Z0-9-_]+)", raw)
            if not m: raise ValueError("[VALIDATION [ERROR ❌]] No se pudo extraer el ID.")
            sid = m.group(1)
        else:
            sid = raw
        sheet = p.get("spreadsheet_source_table_worksheet_name")
        if not sid or not sheet:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan ID y/o worksheet.")

        scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        creds = _auth_with_scopes(p, scopes)
        try:
            svc = build('sheets', 'v4', credentials=creds)
            print("[EXTRACTION [START ⏳]] Extrayendo datos de Google Sheets…", flush=True)
            result = svc.spreadsheets().values().get(
                spreadsheetId=sid,
                range=sheet,
                valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="SERIAL_NUMBER"
            ).execute()
            data = result.get('values', []) or []
            if not data:
                print("[EXTRACTION [WARNING ⚠️]] Hoja sin datos.", flush=True)
                return pd.DataFrame()
            header, rows = data[0], data[1:]
            n = len(header)
            fixed = [(r + [None]*(n-len(r)))[:n] for r in rows]
            df = pd.DataFrame(fixed, columns=header)
            print(f"[EXTRACTION [SUCCESS ✅]] Datos leídos de '{sheet}'.", flush=True)
            return df
        except Exception as e:
            raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Sheets: {e}")

    def _leer_gbq(p: dict) -> pd.DataFrame:
        if bigquery is None:
            raise RuntimeError("[EXTRACTION [ERROR ❌]] Falta paquete 'google-cloud-bigquery'.")
        table = p.get("GBQ_source_table_name")
        if not table:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'GBQ_source_table_name'.")
        scopes = ["https://www.googleapis.com/auth/bigquery","https://www.googleapis.com/auth/drive"]
        creds = _auth_with_scopes(p, scopes)
        proj = _get_project_id(p)
        try:
            q = f"SELECT * FROM `{table}`"
            print(f"[EXTRACTION [START ⏳]] Ejecutando consulta: {q}", flush=True)
            client = bigquery.Client(credentials=creds, project=proj)
            return client.query(q).to_dataframe()
        except Exception as e:
            raise RuntimeError(f"[EXTRACTION [ERROR ❌]] BigQuery: {e}")

    def _leer_gcs(p: dict) -> pd.DataFrame:
        from google.cloud import storage
        bkt, path = p.get("GCS_source_table_bucket_name"), p.get("GCS_source_table_file_path")
        if not bkt or not path:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan bucket y/o file path.")
        scopes = ["https://www.googleapis.com/auth/devstorage.read_only"]
        creds = _auth_with_scopes(p, scopes)
        proj = _get_project_id(p)
        try:
            print(f"[EXTRACTION [START ⏳]] Descargando 'gs://{bkt}/{path}'…", flush=True)
            client = storage.Client(credentials=creds, project=proj)
            raw = client.bucket(bkt).blob(path).download_as_bytes()
            _, ext = os.path.splitext(path); ext = ext.lower()
            if ext in ('.xls', '.xlsx'):
                engine = 'xlrd' if ext == '.xls' else 'openpyxl'
                df = pd.read_excel(io.BytesIO(raw), engine=engine)
            elif ext == '.csv':
                df = pd.read_csv(io.BytesIO(raw), sep=',')
            elif ext == '.tsv':
                df = pd.read_csv(io.BytesIO(raw), sep='\t')
            else:
                raise RuntimeError(f"Extensión '{ext}' no soportada en GCS.")
            print("[EXTRACTION [SUCCESS ✅]] Archivo leído desde GCS.", flush=True)
            return df
        except Exception as e:
            raise RuntimeError(f"[EXTRACTION [ERROR ❌]] GCS: {e}")

    # ───────────── Flujo principal ─────────────
    t0 = time.time()
    try:
        _validar(params)
        fuentes = [_es_fuente_archivo(params), _es_fuente_gsheet(params), _es_fuente_gbq(params), _es_fuente_gcs(params)]
        if sum(map(bool, fuentes)) != 1:
            raise ValueError("[VALIDATION [ERROR ❌]] Indica UNA sola fuente (archivo | gsheet | gbq | gcs).")

        if _es_fuente_archivo(params):
            origen, df = "Archivo local", _leer_archivo(params)
        elif _es_fuente_gsheet(params):
            origen, df = "Google Sheets", _leer_google_sheet(params)
        elif _es_fuente_gbq(params):
            origen, df = "BigQuery", _leer_gbq(params)
        else:
            origen, df = "Google Cloud Storage", _leer_gcs(params)

        df = _apply_common_filters(df, params)
        df = _strip_cells(df, params.get("strip_whitespace_cells", True))
        df = _normalize_headers(df, params.get("normalize_headers_use", False), params.get("normalize_headers_style", "forms"))
        df = _auto_convert(df, params.get("auto_convert_dates", True), params.get("auto_convert_numbers", True))

        _metricas(df, origen, t0)
        return df
    except (ValueError, RuntimeError) as e:
        print(f"[END [ERROR ❌]] {e}", flush=True); raise
    except Exception as e:
        print(f"[PROCESS ERROR ❌] {e}", flush=True); raise






















def table_DF_to_various_targets(params: dict) -> None:
    """
    Escribe un DataFrame en distintos destinos (archivo local, Google Sheets,
    BigQuery o GCS) según la configuración definida en 'params'.

    Parámetros (params)
    -------------------
    - df (pd.DataFrame): DataFrame a exportar. [OBLIGATORIO]
    - ini_environment_identificated (str): 'LOCAL' | 'COLAB' | 'COLAB_ENTERPRISE' | 'GCP'
      (indica el ENTORNO, NO el project_id).
    - json_keyfile_local (str): Ruta a keyfile para LOCAL. (según entorno)
    - json_keyfile_colab (str): Ruta a keyfile en Drive para COLAB. (según entorno)
    - json_keyfile_GCP_secret_id (str): ID de Secret Manager o config GCE/GKE. (según entorno)

    - gcp_project_id (str): ID real del proyecto GCP. [RECOMENDADO]
      Si no se pasa, se intenta inferir desde:
        1) GBQ_target_table_name (si viene como project.dataset.table)
        2) Variable de entorno GOOGLE_CLOUD_PROJECT
        3) ini_environment_identificated sólo si NO es un valor reservado (LOCAL/COLAB/...)

    Destinos (elige uno):
    - Archivo local:
        file_target_table_path (str)
        file_target_table_overwrite_or_append (str): 'overwrite' | 'append'
    - Google Sheets:
        spreadsheet_target_table_id (str o URL)
        spreadsheet_target_table_worksheet_name (str)
        spreadsheet_target_table_overwrite_or_append (str): 'overwrite' | 'append'
    - BigQuery:
        GBQ_target_table_name (str): '[project.]dataset.table'
        GBQ_target_table_overwrite_or_append (str): 'overwrite' | 'append'
        GBQ_location_str (str): ubicación del dataset (p.ej. 'EU'). [opcional]
        GBQ_create_dataset_if_not_exists_bool (bool): default True
    - Google Cloud Storage:
        GCS_target_table_bucket_name (str)
        GCS_target_table_file_path (str)
        GCS_target_table_overwrite_or_append (str): 'overwrite' | 'append'

    Retorno
    -------
    - None

    Raises
    ------
    - ValueError: Si faltan parámetros obligatorios o inválidos.
    - RuntimeError: Si ocurre un error durante la escritura.
    """
    # ─────────────────────────────── IMPORTS BÁSICOS ────────────────────────────────
    import os, io, time, random
    import pandas as pd, numpy as np
    from google.cloud import bigquery
    from google.oauth2.service_account import Credentials
    from google.api_core.exceptions import NotFound, ServiceUnavailable

    # ──────────────────────────────── VALIDACIONES ────────────────────────────────
    print("\n🔹🔹🔹 [START ▶️] Iniciando escritura de DataFrame en destino configurado 🔹🔹🔹\n", flush=True)

    df = params.get("df")
    if df is None or not isinstance(df, pd.DataFrame):
        raise ValueError("[VALIDATION [ERROR ❌]] La clave 'df' debe contener un DataFrame válido.")
    print(f"[METRICS [INFO ℹ️]] DataFrame recibido: {df.shape[0]} filas × {df.shape[1]} columnas.", flush=True)

    if not any(params.get(k) for k in ("json_keyfile_GCP_secret_id", "json_keyfile_colab", "json_keyfile_local")):
        raise ValueError("[VALIDATION [ERROR ❌]] Falta un parámetro de keyfile para autenticación.")

    # ────────────────────────── DETECCIÓN DEL DESTINO ──────────────────────────
    _es_target_archivo = lambda p: bool(p.get('file_target_table_path', '').strip())
    _es_target_gsheet  = lambda p: (not _es_target_archivo(p)) and \
                                   bool(p.get('spreadsheet_target_table_id', '').strip()) and \
                                   bool(p.get('spreadsheet_target_table_worksheet_name', '').strip())
    _es_target_gbq     = lambda p: bool(p.get('GBQ_target_table_name', '').strip())
    _es_target_gcs     = lambda p: bool(p.get('GCS_target_table_bucket_name', '').strip()) and \
                                   bool(p.get('GCS_target_table_file_path', '').strip())

    # ──────────────────────────── HELPERS INTERNOS ─────────────────────────────
    def _resolve_project_id_str(p: dict) -> str:
        project_override_str = (p.get("gcp_project_id") or "").strip()
        if project_override_str:
            return project_override_str
        table_str = (p.get("GBQ_target_table_name") or "").strip()
        if table_str.count(".") == 2:
            return table_str.split(".")[0]
        env_val = (p.get("ini_environment_identificated") or "").strip()
        reserved_env = {"LOCAL", "COLAB", "COLAB_ENTERPRISE", "GCP"}
        if env_val and env_val not in reserved_env:
            return env_val
        env_var = (os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip()
        if env_var:
            return env_var
        raise ValueError("[VALIDATION [ERROR ❌]] No se pudo determinar 'gcp_project_id'. "
                         "Pase 'gcp_project_id' o use una tabla 'project.dataset.table'.")


    # ──────────────────────────── SUB-FUNCIONES DE ESCRITURA ───────────────────
    def _escribir_archivo(p: dict, d: pd.DataFrame) -> None:
        import os
        print("\n[LOAD [START ▶️]] Iniciando escritura en archivo local…", flush=True)
        mode  = p.get("file_target_table_overwrite_or_append", "overwrite").lower()
        fpath = p.get("file_target_table_path")
        if not fpath:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'file_target_table_path'.")
        _, ext = os.path.splitext(fpath); ext = ext.lower()

        try:
            if ext in {'.xls', '.xlsx'}:
                engine = 'openpyxl'
                if mode == "append" and os.path.exists(fpath):
                    d = pd.concat([pd.read_excel(fpath, engine=engine), d], ignore_index=True)
                d.to_excel(fpath, index=False, engine=engine)

            elif ext in {'.csv', '.tsv'}:
                sep    = '\t' if ext == '.tsv' else ','
                header = not (mode == "append" and os.path.exists(fpath))
                d.to_csv(fpath, sep=sep, index=False,
                         mode=('a' if mode == "append" else 'w'),
                         header=header)
            else:
                raise RuntimeError(f"Extensión '{ext}' no soportada para archivos locales.")

            print(f"[LOAD [SUCCESS ✅]] DataFrame escrito en '{fpath}'.", flush=True)
            print(f"[METRICS [INFO ℹ️]] Destino final: file://{fpath}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en archivo local: {e}")

    def _escribir_google_sheet(p: dict, d: pd.DataFrame) -> None:
        import re
        from googleapiclient.discovery import build
        print("\n[LOAD [START ▶️]] Iniciando escritura en Google Sheets…", flush=True)

        mode = p.get("spreadsheet_target_table_overwrite_or_append", "overwrite").lower()
        raw_id   = p.get("spreadsheet_target_table_id")
        ws_name  = p.get("spreadsheet_target_table_worksheet_name")
        if not raw_id or not ws_name:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'spreadsheet_target_table_id' o 'spreadsheet_target_table_worksheet_name'.")

        # Normalizar el ID (por si viene una URL completa)
        m = re.search(r"/d/([A-Za-z0-9-_]+)", raw_id)
        sheet_id = m.group(1) if m else raw_id

        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        project_id_str = _resolve_project_id_str(p)  # sólo para logs/consistencia
        creds = _ini_authenticate_API(p, project_id_str, scopes=scopes)
        service = build('sheets', 'v4', credentials=creds)

        # Convertir datetime a texto con coma decimal
        datetime_cols = d.select_dtypes(include=["datetime64[ns]", "datetime64[ns, utc]"]).columns
        if len(datetime_cols) > 0:
            d = d.copy()
            for col in datetime_cols:
                d[col] = d[col].dt.strftime("%Y-%m-%d %H:%M:%S,%f")

        from decimal import Decimal, InvalidOperation
        def _cast(value):
            if pd.isna(value):
                return None
            if isinstance(value, (float, np.floating, Decimal)):
                try:
                    return float(value)
                except (ValueError, InvalidOperation):
                    return str(value)
            if isinstance(value, (int, np.integer)):
                return int(value)
            return str(value)

        rows = [[_cast(val) for val in row] for row in d.itertuples(index=False, name=None)]
        values = ([d.columns.tolist()] + rows) if mode == "overwrite" else rows

        # Limpiar rango si es overwrite
        if mode == "overwrite":
            service.spreadsheets().values().clear(
                spreadsheetId=sheet_id,
                range=ws_name,
                body={}
            ).execute()

        request = (service.spreadsheets().values().update if mode == "overwrite"
                   else service.spreadsheets().values().append)
        result = request(
            spreadsheetId   = sheet_id,
            range           = ws_name,
            valueInputOption= "RAW",
            body            = {"values": values},
            **({"insertDataOption": "INSERT_ROWS"} if mode == "append" else {})
        ).execute()

        affected = result.get('updates', {}).get('updatedCells', 'N/A')
        print(f"[LOAD [SUCCESS ✅]] Se actualizaron {affected} celdas en Google Sheets.", flush=True)
        print(f"[METRICS [INFO ℹ️]] Destino final: https://docs.google.com/spreadsheets/d/{sheet_id}", flush=True)

    def _escribir_gbq(p: dict, d: pd.DataFrame) -> None:
        from google.cloud.bigquery import LoadJobConfig, WriteDisposition
        print("\n[LOAD [START ▶️]] Iniciando carga en BigQuery…", flush=True)

        mode  = p.get("GBQ_target_table_overwrite_or_append", "overwrite").lower()
        table = p.get("GBQ_target_table_name")
        if not table:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'GBQ_target_table_name'.")

        # Resolver project_id correctamente
        project_id_str = _resolve_project_id_str(p)
        location_str = (p.get("GBQ_location_str") or "EU").strip()
        scopes_bq = ["https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/drive"]
        creds = _ini_authenticate_API(p, project_id_str, scopes=scopes_bq)
        client = bigquery.Client(credentials=creds, project=project_id_str, location=location_str)

        # Autocreación de dataset si procede
        create_dataset = p.get("GBQ_create_dataset_if_not_exists_bool", True)
        try:
            parts = table.split(".")
            if len(parts) == 3:
                dataset_id = f"{parts[0]}.{parts[1]}"
            elif len(parts) == 2:
                dataset_id = f"{project_id_str}.{parts[0]}"
                table = f"{project_id_str}.{table}"
            else:
                raise ValueError("[VALIDATION [ERROR ❌]] Formato de 'GBQ_target_table_name' inválido. Use dataset.table o project.dataset.table.")

            if create_dataset:
                try:
                    client.get_dataset(dataset_id)
                except NotFound:
                    from google.cloud.bigquery import Dataset
                    ds = Dataset(dataset_id)
                    ds.location = location_str
                    client.create_dataset(ds, exists_ok=True)
                    print(f"[EXTRACTION [INFO ℹ️]] Dataset creado: {dataset_id}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error preparando dataset/tabla: {e}")

        job_cfg = LoadJobConfig(
            write_disposition=(WriteDisposition.WRITE_TRUNCATE if mode == "overwrite"
                               else WriteDisposition.WRITE_APPEND)
        )

        # Reintentos básicos para 503
        max_tries = 4
        for attempt in range(1, max_tries + 1):
            try:
                client.load_table_from_dataframe(d, table, job_config=job_cfg).result()
                print("[LOAD [SUCCESS ✅]] DataFrame cargado exitosamente en BigQuery.", flush=True)
                print(f"[METRICS [INFO ℹ️]] Destino final: https://console.cloud.google.com/bigquery?project={project_id_str}", flush=True)
                break
            except ServiceUnavailable as e:
                if attempt == max_tries:
                    raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en BigQuery tras {max_tries} intentos: {e}")
                sleep_secs = (2 ** attempt) + random.uniform(0, 1.5)
                print(f"[LOAD [WARNING ⚠️]] 503 recibido. Reintentando en {sleep_secs:.1f}s (intento {attempt}/{max_tries})…", flush=True)
                time.sleep(sleep_secs)
            except Exception as e:
                raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en BigQuery: {e}")

    def _escribir_gcs(p: dict, d: pd.DataFrame) -> None:
        from google.cloud import storage
        print("\n[LOAD [START ▶️]] Iniciando subida a Google Cloud Storage…", flush=True)

        bucket = p.get("GCS_target_table_bucket_name")
        path   = p.get("GCS_target_table_file_path")
        if not bucket or not path:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'GCS_target_table_bucket_name' o 'GCS_target_table_file_path'.")

        mode = p.get("GCS_target_table_overwrite_or_append", "overwrite").lower()
        project_id_str = _resolve_project_id_str(p)
        scopes_gcs = ["https://www.googleapis.com/auth/devstorage.read_write"]
        creds = _ini_authenticate_API(p, project_id_str, scopes=scopes_gcs)
        client  = storage.Client(credentials=creds, project=project_id_str)
        blob    = client.bucket(bucket).blob(path)
        import os
        _, ext  = os.path.splitext(path); ext = ext.lower()

        try:
            # Si append, descargar y concatenar
            if mode == "append" and blob.exists(client):
                data = blob.download_as_bytes()
                if ext == '.csv':
                    d = pd.concat([pd.read_csv(io.BytesIO(data)), d], ignore_index=True)
                elif ext == '.tsv':
                    d = pd.concat([pd.read_csv(io.BytesIO(data), sep='\t'), d], ignore_index=True)
                elif ext in {'.xls', '.xlsx'}:
                    d = pd.concat([pd.read_excel(io.BytesIO(data)), d], ignore_index=True)
                else:
                    raise RuntimeError(f"Extensión '{ext}' no soportada en modo append.")

            # Serializar y subir
            if ext in {'.xls', '.xlsx'}:
                buf = io.BytesIO(); d.to_excel(buf, index=False, engine='openpyxl')
                blob.upload_from_string(buf.getvalue(),
                                        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            elif ext == '.csv':
                blob.upload_from_string(d.to_csv(index=False).encode('utf-8'), content_type='text/csv')
            elif ext == '.tsv':
                blob.upload_from_string(d.to_csv(sep='\t', index=False).encode('utf-8'),
                                        content_type='text/tab-separated-values')
            else:
                raise RuntimeError(f"Extensión '{ext}' no soportada para GCS.")

            print("[LOAD [SUCCESS ✅]] Archivo subido exitosamente a GCS.", flush=True)
            print(f"[METRICS [INFO ℹ️]] Destino final: gs://{bucket}/{path}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en GCS: {e}")

    # ─────────────────────────────── DESPACHADOR ───────────────────────────────
    try:
        if _es_target_archivo(params):
            _escribir_archivo(params, df)
        elif _es_target_gsheet(params):
            _escribir_google_sheet(params, df)
        elif _es_target_gbq(params):
            _escribir_gbq(params, df)
        elif _es_target_gcs(params):
            _escribir_gcs(params, df)
        else:
            raise ValueError(
                "[VALIDATION [ERROR ❌]] No se detectó un destino válido. "
                "Defina 'file_target_table_path', "
                "o ('spreadsheet_target_table_id' y 'spreadsheet_target_table_worksheet_name'), "
                "o 'GBQ_target_table_name', "
                "o ('GCS_target_table_bucket_name' y 'GCS_target_table_file_path')."
            )
    except Exception as e:
        print(f"\n🔹🔹🔹 [END [FAILED ❌]] Proceso finalizado con errores: {e} 🔹🔹🔹\n", flush=True)
        raise

    print("\n🔹🔹🔹 [END [FINISHED ✅]] Escritura completada exitosamente. 🔹🔹🔹\n", flush=True)

