from common.dpm_GCP_ini import _ini_authenticate_API
    
import pandas as pd

# __________________________________________________________________________________________________________________________________________________________
# HS_sensitive_data_to_df
# __________________________________________________________________________________________________________________________________________________________
def HS_sensitive_data_to_df(config: dict) -> pd.DataFrame:
    """
    Extrae contactos de HubSpot por ventanas de createdate y devuelve un DataFrame
    con campos NO sensibles (vía /search) y SENSIBLES (vía /batch/read).

    Implementa:
      - Validación de parámetros.
      - Resolución de esquema incluyendo propiedades sensibles con `dataSensitivity`.
      - Paginación por ventanas con split dinámico para esquivar el cap de ~10k resultados.
      - Reintentos con backoff para 429/5xx y throttling configurable en batch/read.
      - Filtro opcional: devolver solo filas con al menos un valor sensible no vacío.

    Parámetros (config)
    -------------------
    - HS_datawarehouse_sensitive_acces_token (str): Token (Private App) con acceso a contactos y Sensitive Data.
    - HS_fields_no_sensitive_names_list (list[str]): Propiedades NO sensibles (se fuerzan "createdate", "lastmodifieddate", "hs_object_id").
    - HS_fields_sensitive_names_list (list[str]): Propiedades sensibles a recuperar con batch/read.
    - HS_only_records_with_any_sensitive_value_bool (bool): Si True, filtra filas sin valores sensibles.
    - HS_contact_filter_createdate (dict): {"from":"YYYY-MM-DD","to":"YYYY-MM-DD","mode":"between|after|since|before|until"}.
    - HS_batch_read_chunk_size_int (int, opcional): Tamaño de lote para batch/read (1..100; por defecto 100).
    - HS_batch_read_throttle_ms_int (int, opcional): Pausa (ms) entre llamadas batch/read (por defecto 0).

    Retorna
    -------
    pandas.DataFrame: Columnas: id, createdate, lastmodifieddate, hs_object_id, [no_sensibles...], [sensibles...]

    Raises
    ------
    ValueError: Validación de parámetros, 401/403, fechas inválidas, o sin propiedades válidas tras validar el esquema.
    """
    import time
    from datetime import datetime, timezone, timedelta
    from typing import Any, Dict, List, Optional, Tuple
    
    import requests

    print("🔹🔹🔹 [START ▶️] Extracción HubSpot (sensibles) → DataFrame 🔹🔹🔹", flush=True)

    # ===============================
    # 1) VALIDACIÓN DE PARÁMETROS
    # ===============================
    def _validate_params_dic(cfg: dict) -> None:
        token_str = cfg.get("HS_datawarehouse_sensitive_acces_token") or cfg.get("HS_access_token")
        if not token_str or not isinstance(token_str, str):
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'HS_datawarehouse_sensitive_acces_token' (str) en config.")

        if "HS_contact_filter_createdate" not in cfg or not isinstance(cfg["HS_contact_filter_createdate"], dict):
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'HS_contact_filter_createdate' (dict) en config.")

        f_dic = cfg["HS_contact_filter_createdate"]
        from_str = f_dic.get("from")
        to_str   = f_dic.get("to")
        mode_str = (f_dic.get("mode") or "between").lower()

        if mode_str not in ("between", "after", "since", "before", "until"):
            raise ValueError("[VALIDATION [ERROR ❌]] 'mode' debe ser 'between' | 'after/since' | 'before/until'.")

        def _parse_date(d: Optional[str]) -> Optional[datetime]:
            if not d:
                return None
            return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        from_dt = _parse_date(from_str)
        to_dt   = _parse_date(to_str)

        if mode_str == "between":
            if not from_dt or not to_dt:
                raise ValueError("[VALIDATION [ERROR ❌]] 'from' y 'to' son obligatorios para mode='between'.")
            if from_dt > to_dt:
                raise ValueError("[VALIDATION [ERROR ❌]] Rango de fechas inválido: 'from' > 'to'.")

    _validate_params_dic(config)

    # Normalizaciones
    HS_token_str: str = config.get("HS_datawarehouse_sensitive_acces_token") or config.get("HS_access_token")
    HS_no_sens_list: List[str] = list(config.get("HS_fields_no_sensitive_names_list", []) or [])
    HS_sens_list:    List[str] = list(config.get("HS_fields_sensitive_names_list", []) or [])
    HS_only_sens_bool: bool = bool(config.get("HS_only_records_with_any_sensitive_value_bool", False))

    # Nunca pedir 'id' como propiedad (es record ID top-level / hs_object_id)
    HS_no_sens_list = [p for p in HS_no_sens_list if p != "id"]

    # Forzamos propiedades base de auditoría para /search
    for forced in ("createdate", "lastmodifieddate", "hs_object_id"):
        if forced not in HS_no_sens_list:
            HS_no_sens_list.append(forced)

    # Batch/read tuning
    chunk_size_int = int(config.get("HS_batch_read_chunk_size_int", 100))
    throttle_ms_int = int(config.get("HS_batch_read_throttle_ms_int", 0))
    chunk_size_int = max(1, min(100, chunk_size_int))

    filter_dic: Dict[str, Any] = config["HS_contact_filter_createdate"]
    mode_str = (filter_dic.get("mode") or "between").lower()

    def _to_iso_start_of_day(date_str: str) -> str:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")

    def _to_iso_end_of_day(date_str: str) -> str:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        dt_eod = dt + timedelta(days=1) - timedelta(milliseconds=1)
        return dt_eod.isoformat().replace("+00:00", "Z")

    from_iso_str: Optional[str] = filter_dic.get("from")
    to_iso_str:   Optional[str] = filter_dic.get("to")
    if from_iso_str:
        from_iso_str = _to_iso_start_of_day(from_iso_str)
    if to_iso_str:
        to_iso_str = _to_iso_end_of_day(to_iso_str)

    print(f"[VALIDATION [SUCCESS ✅]] Parámetros validados. Modo fecha: '{mode_str}'.", flush=True)

    # ===============================
    # 2) HTTP + RETRIES
    # ===============================
    base_url_str = "https://api.hubapi.com"
    session = requests.Session()
    headers_dic = {"Authorization": f"Bearer {HS_token_str}", "Content-Type": "application/json"}

    def _request_with_retries(method: str, url: str, *, json: Optional[dict] = None, params: Optional[dict] = None) -> requests.Response:
        backoffs_sec = [1, 2, 4, 8, 16]
        last_resp: Optional[requests.Response] = None
        for attempt_int, delay in enumerate([0] + backoffs_sec):
            if delay:
                time.sleep(delay)
            resp = session.request(method, url, headers=headers_dic, json=json, params=params, timeout=60)
            last_resp = resp
            if resp.status_code in (401, 403):
                raise ValueError(f"[AUTHENTICATION [ERROR ❌]] {resp.status_code} - Verifica token y Sensitive Data. Detalle: {resp.text}")
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                if attempt_int < len(backoffs_sec):
                    print(f"[NETWORK [WARNING ⚠️]] {resp.status_code} transitorio. Reintento en {delay}s…", flush=True)
                    continue
            return resp
        return last_resp  # type: ignore

    # ===============================
    # 3) ESQUEMA: INCLUYE SENSIBLES
    # ===============================
    def _get_contact_properties_names_set() -> set:
        """
        Lista nombres de propiedades de contacto:
          - no sensibles (por defecto),
          - sensibles (dataSensitivity=sensitive),
          - altamente sensibles (dataSensitivity=highly_sensitive) si aplica.
        """
        url = f"{base_url_str}/crm/v3/properties/contacts"

        def _page(level: Optional[str], after: Optional[str]) -> dict:
            params = {"archived": "false"}
            if level:
                params["dataSensitivity"] = level
            if after:
                params["after"] = after
            resp = _request_with_retries("GET", url, params=params)
            resp.raise_for_status()
            return resp.json() or {}

        names = set()
        for level in (None, "sensitive", "highly_sensitive"):
            after = None
            while True:
                try:
                    data = _page(level, after)
                except Exception as e:
                    print(f"[SCHEMA [WARNING ⚠️]] No se pudo listar propiedades con dataSensitivity='{level}': {e}", flush=True)
                    break
                for p in data.get("results", []) or []:
                    n = p.get("name")
                    if n:
                        names.add(n)
                paging = data.get("paging", {})
                after = (paging.get("next") or {}).get("after")
                if not after:
                    break
        return names

    print("[SCHEMA [START ▶️]] Descargando lista de propiedades (incluye sensibles)…", flush=True)
    try:
        props_available_set = _get_contact_properties_names_set()
    except Exception as e:
        raise ValueError(f"[SCHEMA [ERROR ❌]] No se pudo obtener el schema de propiedades: {e}")

    orig_no_sens_set = set(HS_no_sens_list)
    orig_sens_set    = set(HS_sens_list)

    HS_no_sens_list = [p for p in HS_no_sens_list if p in props_available_set]
    HS_sens_list    = [p for p in HS_sens_list    if p in props_available_set]

    dropped_no_sens = sorted(list(orig_no_sens_set - set(HS_no_sens_list)))
    dropped_sens    = sorted(list(orig_sens_set    - set(HS_sens_list)))

    if dropped_no_sens:
        print(f"[SCHEMA [WARNING ⚠️]] Propiedades NO sensibles desconocidas y omitidas: {dropped_no_sens}", flush=True)
    if dropped_sens:
        print(f"[SCHEMA [WARNING ⚠️]] Propiedades SENSIBLES desconocidas y omitidas: {dropped_sens}", flush=True)

    if len(HS_no_sens_list) == 0 and len(HS_sens_list) == 0:
        raise ValueError("[VALIDATION [ERROR ❌]] No hay propiedades válidas para recuperar tras validar el schema.")

    print(f"[SCHEMA [SUCCESS ✅]] Propiedades válidas. NO sensibles: {len(HS_no_sens_list)} | Sensibles: {len(HS_sens_list)}", flush=True)

    # ===============================
    # 4) SEARCH por ventanas (createdate)
    # ===============================
    search_url_str = f"{base_url_str}/crm/v3/objects/contacts/search"

    def _build_filters_for_window(win_from_iso: Optional[str], win_to_iso: Optional[str]) -> List[Dict[str, Any]]:
        filters: List[Dict[str, Any]] = []
        if mode_str == "between":
            filters.append({"propertyName": "createdate", "operator": "GTE", "value": win_from_iso})
            filters.append({"propertyName": "createdate", "operator": "LTE", "value": win_to_iso})
        elif mode_str in ("after", "since"):
            filters.append({"propertyName": "createdate", "operator": "GTE", "value": win_from_iso})
        elif mode_str in ("before", "until"):
            filters.append({"propertyName": "createdate", "operator": "LTE", "value": win_to_iso})
        return filters

    def _search_window(win_from_iso: Optional[str], win_to_iso: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Devuelve (lista_de_props_planas, last_createdAt_iso) paginando ASC por createdate.
        Si la ventana alcanza ~9.5k, se corta para repartir y evitar el cap de 10k.
        """
        hard_limit_int = 9500
        fetched_int = 0
        results_list: List[Dict[str, Any]] = []
        after_cursor: Optional[str] = None
        last_createdAt_iso: Optional[str] = None

        while True:
            body_dic = {
                "filterGroups": [{"filters": _build_filters_for_window(win_from_iso, win_to_iso)}],
                "properties": HS_no_sens_list,
                "limit": 100,
                "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}],
            }
            if after_cursor:
                body_dic["after"] = after_cursor

            resp = _request_with_retries("POST", search_url_str, json=body_dic)
            resp.raise_for_status()
            data = resp.json() or {}

            page_results = data.get("results", []) or []
            for item in page_results:
                cid = str(item.get("id"))
                props = item.get("properties") or {}
                created_at_iso = item.get("createdAt")
                # Normalizamos mínimos
                props["id"] = props.get("id") or cid
                if created_at_iso and "createdate" not in props:
                    props["createdate"] = created_at_iso
                results_list.append(props)
                last_createdAt_iso = created_at_iso

            fetched_int += len(page_results)
            paging = data.get("paging", {})
            after_cursor = (paging.get("next") or {}).get("after")

            if fetched_int >= hard_limit_int and after_cursor is not None:
                print(f"[SEARCH [WARNING ⚠️]] Ventana alcanzó {fetched_int} registros. Se dividirá en '{last_createdAt_iso}'.", flush=True)
                break

            if not after_cursor:
                break

        return results_list, last_createdAt_iso

    # Cola de ventanas inicial
    windows_queue: List[Tuple[Optional[str], Optional[str]]] = []
    if mode_str == "between":
        windows_queue.append((from_iso_str, to_iso_str))
    elif mode_str in ("after", "since"):
        windows_queue.append((from_iso_str, None))
    elif mode_str in ("before", "until"):
        windows_queue.append((None, to_iso_str))

    all_contacts_basic_list: List[Dict[str, Any]] = []
    total_win_int = 0

    while windows_queue:
        win_from_iso, win_to_iso = windows_queue.pop(0)
        total_win_int += 1
        print(f"[SEARCH [START ▶️]] Ventana #{total_win_int} | from={win_from_iso} | to={win_to_iso}", flush=True)

        page_rows_list, last_createdAt_iso = _search_window(win_from_iso, win_to_iso)
        all_contacts_basic_list.extend(page_rows_list)

        if last_createdAt_iso:
            dt = datetime.fromisoformat(last_createdAt_iso.replace("Z", "+00:00"))
            dt_plus = dt + timedelta(milliseconds=1)
            next_from_iso = dt_plus.isoformat().replace("+00:00", "Z")
            if mode_str == "between":
                if win_to_iso and next_from_iso <= win_to_iso:
                    windows_queue.insert(0, (next_from_iso, win_to_iso))
            elif mode_str in ("after", "since"):
                windows_queue.insert(0, (next_from_iso, None))

        print(f"[SEARCH [INFO ℹ️]] Ventana #{total_win_int} | Acumulados: {len(all_contacts_basic_list)} registros.", flush=True)

    if not all_contacts_basic_list:
        print("[SEARCH [SUCCESS ✅]] No se encontraron contactos en el rango solicitado.", flush=True)
        return pd.DataFrame(columns=list(set(["id", "createdate", "lastmodifieddate", "hs_object_id"] + HS_no_sens_list + HS_sens_list)))

    print(f"[SEARCH [SUCCESS ✅]] Total contactos básicos: {len(all_contacts_basic_list)}", flush=True)

    # ===============================
    # 5) batch/read de SENSIBLES
    # ===============================
    base_df = pd.DataFrame(all_contacts_basic_list).drop_duplicates(subset=["id"]).reset_index(drop=True)
    all_ids_list = base_df["id"].astype(str).tolist()

    if HS_sens_list:
        batch_url_str = f"{base_url_str}/crm/v3/objects/contacts/batch/read"

        def _chunked(seq: List[str], size: int) -> List[List[str]]:
            return [seq[i:i + size] for i in range(0, len(seq), size)]

        sens_frames_list: List[pd.DataFrame] = []
        chunks = _chunked(all_ids_list, chunk_size_int)
        total_chunks = len(chunks)
        for idx, chunk_ids in enumerate(chunks, start=1):
            body = {"properties": HS_sens_list, "inputs": [{"id": str(cid)} for cid in chunk_ids]}
            try:
                resp = _request_with_retries("POST", batch_url_str, json=body)
                resp.raise_for_status()
            except Exception as e:
                raise ValueError(f"[SENSITIVE [ERROR ❌]] batch/read falló en el chunk {idx}/{total_chunks}: {e}")

            data = resp.json() or {}
            items = data.get("results", []) or []
            rows = []
            for it in items:
                cid = str(it.get("id"))
                props = it.get("properties") or {}
                props["id"] = cid
                rows.append(props)
            sens_df = pd.DataFrame(rows)
            sens_frames_list.append(sens_df)

            print(f"[SENSITIVE [INFO ℹ️]] Chunk {idx}/{total_chunks} | Registros: {len(sens_df)}", flush=True)

            if throttle_ms_int > 0 and idx < total_chunks:
                time.sleep(throttle_ms_int / 1000.0)

        sens_df_full = (
            pd.concat(sens_frames_list, ignore_index=True).drop_duplicates(subset=["id"])
            if sens_frames_list
            else pd.DataFrame(columns=["id"] + HS_sens_list)
        )
        df_final = base_df.merge(sens_df_full, on="id", how="left")
    else:
        print("[SENSITIVE [INFO ℹ️]] No se solicitaron propiedades sensibles. Saltando batch/read.", flush=True)
        df_final = base_df

    # ===============================
    # 6) LIMPIEZA + FILTRO OPCIONAL
    # ===============================
    final_cols = ["id", "createdate", "lastmodifieddate", "hs_object_id"]
    final_cols += [c for c in base_df.columns if c not in final_cols]
    final_cols += [c for c in HS_sens_list if c not in final_cols]
    df_final = df_final.reindex(columns=[c for c in final_cols if c in df_final.columns])

    if HS_sens_list and HS_only_sens_bool:
        def _row_has_any_sensitive(row) -> bool:
            for c in HS_sens_list:
                v = row.get(c)
                if v is None:
                    continue
                if isinstance(v, str):
                    if v.strip() != "":
                        return True
                else:
                    if bool(v):
                        return True
            return False

        before = len(df_final)
        df_final = df_final[df_final.apply(_row_has_any_sensitive, axis=1)].reset_index(drop=True)
        print(f"[TRANSFORMATION [INFO ℹ️]] Filtro 'solo con sensibles' aplicado. {before} → {len(df_final)} registros.", flush=True)
    elif not HS_sens_list and HS_only_sens_bool:
        print("[TRANSFORMATION [WARNING ⚠️]] 'HS_only_records_with_any_sensitive_value_bool' es True pero no hay columnas sensibles. No se filtrará.", flush=True)

    # ===============================
    # 7) MÉTRICAS
    # ===============================
    print("🔹🔹🔹 [METRICS [INFO 📊]] Resumen de Ejecución 🔹🔹🔹", flush=True)
    print(f"[METRICS [INFO 📊]] Registros: {len(df_final)} | Columnas: {len(df_final.columns)}", flush=True)
    print(f"[METRICS [INFO 📊]] Columnas sensibles: {len(HS_sens_list)} -> {HS_sens_list}", flush=True)
    print("[END [FINISHED ✅]] Extracción completada.", flush=True)

    return df_final
