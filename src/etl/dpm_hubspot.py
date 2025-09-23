from common.dpm_GCP_ini import _ini_authenticate_API
    
import pandas as pd

# __________________________________________________________________________________________________________________________________________________________
# HS_sensitive_data_to_df
# __________________________________________________________________________________________________________________________________________________________
def HS_sensitive_data_to_df(config: dict) -> pd.DataFrame:
    """
    Extrae contactos de HubSpot por ventanas de createdate y devuelve un DataFrame
    EXCLUSIVAMENTE con las columnas pedidas en:
      - HS_fields_no_sensitive_names_list (DEBE incluir "id")
      - HS_fields_sensitive_names_list

    Valida que todas las propiedades existan en HubSpot (excepto 'id', que es top-level).
    Si alguna no existe, lanza ValueError especificando cuáles.

    Parámetros (config)
    -------------------
    - HS_datawarehouse_sensitive_acces_token (str): Token (Private App) con acceso a contactos y Sensitive Data.
    - HS_fields_no_sensitive_names_list (list[str]): Propiedades NO sensibles a recuperar (DEBE incluir "id").
    - HS_fields_sensitive_names_list (list[str]): Propiedades sensibles a recuperar con batch/read.
    - HS_only_records_with_any_sensitive_value_bool (bool): Si True, filtra filas sin valores sensibles.
    - HS_contact_filter_createdate (dict): {"from":"YYYY-MM-DD","to":"YYYY-MM-DD","mode":"between|after|since|before|until"}.
    - HS_batch_read_chunk_size_int (int, opcional): Tamaño de lote para batch/read (1..100; por defecto 100).
    - HS_batch_read_throttle_ms_int (int, opcional): Pausa (ms) entre llamadas batch/read (por defecto 0).

    Retorna
    -------
    pandas.DataFrame con columnas EXACTAMENTE = NO_sensibles_solicitadas + sensibles_solicitadas

    Raises
    ------
    ValueError: Validación de parámetros, 401/403, fechas inválidas, propiedades inexistentes en HubSpot,
                o imposibilidad de hacer join.
    """
    import time
    from datetime import datetime, timezone, timedelta
    from typing import Any, Dict, List, Optional, Tuple
    import requests
    import pandas as pd

    print("🔹🔹🔹 [START ▶️] Extracción HubSpot (sensibles) → DataFrame 🔹🔹🔹", flush=True)

    # ===============================
    # 0) Helpers
    # ===============================
    def _ensure_only_and_order(df: pd.DataFrame, ordered_cols_list: List[str]) -> pd.DataFrame:
        """Crea columnas faltantes como NaN, elimina sobrantes y reordena."""
        for c in ordered_cols_list:
            if c not in df.columns:
                df[c] = pd.NA
        return df[ordered_cols_list]

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

        no_sens_list = list(cfg.get("HS_fields_no_sensitive_names_list", []) or [])
        if "id" not in no_sens_list:
            raise ValueError("[VALIDATION [ERROR ❌]] Debes incluir 'id' en 'HS_fields_no_sensitive_names_list' para poder operar.")

    _validate_params_dic(config)

    # Normalizaciones
    HS_token_str: str = config.get("HS_datawarehouse_sensitive_acces_token") or config.get("HS_access_token")
    HS_no_sens_list: List[str] = list(config.get("HS_fields_no_sensitive_names_list", []) or [])
    HS_sens_list:    List[str] = list(config.get("HS_fields_sensitive_names_list", []) or [])
    HS_only_sens_bool: bool = bool(config.get("HS_only_records_with_any_sensitive_value_bool", False))

    # Tuning
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
        import requests as _rq
        backoffs_sec = [1, 2, 4, 8, 16]
        last_resp: Optional[_rq.Response] = None
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
    # 3) ESQUEMA + VALIDACIÓN DE PROPIEDADES
    # ===============================
    def _get_contact_properties_names_set() -> set:
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

    print("[SCHEMA [START ▶️]] Validando propiedades solicitadas…", flush=True)
    try:
        props_available_set = _get_contact_properties_names_set()
    except Exception as e:
        raise ValueError(f"[SCHEMA [ERROR ❌]] No se pudo obtener el schema de propiedades: {e}")

    missing_no_sens = [p for p in HS_no_sens_list if p != "id" and p not in props_available_set]
    missing_sens    = [p for p in HS_sens_list    if p not in props_available_set]
    if missing_no_sens or missing_sens:
        msg_parts = []
        if missing_no_sens:
            msg_parts.append(f"NO sensibles inexistentes: {missing_no_sens}")
        if missing_sens:
            msg_parts.append(f"SENSIBLES inexistentes: {missing_sens}")
        raise ValueError("[SCHEMA [ERROR ❌]] " + " | ".join(msg_parts))

    print("[SCHEMA [SUCCESS ✅]] Propiedades válidas NO sensibles:", len(HS_no_sens_list), flush=True)
    if HS_no_sens_list:
        print("\n".join(sorted(HS_no_sens_list)), flush=True)
    print("[SCHEMA [SUCCESS ✅]] Propiedades válidas sensibles:", len(HS_sens_list), flush=True)
    if HS_sens_list:
        print("\n".join(sorted(HS_sens_list)), flush=True)

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

    # Propiedades a pedir en /search (solo NO sensibles solicitadas)
    search_properties_list = list(HS_no_sens_list)

    def _search_window(win_from_iso: Optional[str], win_to_iso: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        hard_limit_int = 9500
        fetched_int = 0
        results_list: List[Dict[str, Any]] = []
        after_cursor: Optional[str] = None
        last_createdAt_iso: Optional[str] = None

        while True:
            body_dic = {
                "filterGroups": [{"filters": _build_filters_for_window(win_from_iso, win_to_iso)}],
                "properties": search_properties_list,
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

                # Rellenos desde top-level SOLO si se solicitaron
                if "id" in HS_no_sens_list:
                    props["id"] = props.get("id") or cid
                if "createdate" in HS_no_sens_list and "createdate" not in props:
                    top_created = item.get("createdAt")
                    if top_created:
                        props["createdate"] = top_created
                if "lastmodifieddate" in HS_no_sens_list and "lastmodifieddate" not in props:
                    top_updated = item.get("updatedAt")
                    if top_updated:
                        props["lastmodifieddate"] = top_updated
                if "hs_object_id" in HS_no_sens_list:
                    props["hs_object_id"] = props.get("hs_object_id") or cid

                results_list.append(props)
                last_createdAt_iso = item.get("createdAt")

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
                if to_iso_str and next_from_iso <= to_iso_str:
                    windows_queue.insert(0, (next_from_iso, to_iso_str))
            elif mode_str in ("after", "since"):
                windows_queue.insert(0, (next_from_iso, None))

        print(f"[SEARCH [INFO ℹ️]] Ventana #{total_win_int} | Acumulados: {len(all_contacts_basic_list)} registros.", flush=True)

    if not all_contacts_basic_list:
        print("[SEARCH [SUCCESS ✅]] No se encontraron contactos en el rango solicitado.", flush=True)
        final_cols = list(dict.fromkeys(HS_no_sens_list + HS_sens_list))
        return pd.DataFrame(columns=final_cols)

    print(f"[SEARCH [SUCCESS ✅]] Total contactos básicos: {len(all_contacts_basic_list)}", flush=True)

    # ===============================
    # 5) batch/read de SENSIBLES
    # ===============================
    # Base SOLO con columnas NO sensibles pedidas (ni una más)
    base_df = pd.DataFrame(all_contacts_basic_list).reset_index(drop=True)
    base_df = _ensure_only_and_order(base_df, HS_no_sens_list)

    # IDs para batch/read
    if "id" in base_df.columns:
        all_ids_list = base_df["id"].astype(str).tolist()
    elif "hs_object_id" in base_df.columns:
        all_ids_list = base_df["hs_object_id"].astype(str).tolist()
    else:
        raise ValueError("[PROCESS ERROR ❌] No se pudo construir la lista de IDs para batch/read.")

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
                # Forzamos claves de join SOLO si el usuario las pidió en no-sensibles
                if "id" in HS_no_sens_list:
                    props["id"] = cid
                if "hs_object_id" in HS_no_sens_list and "hs_object_id" not in props:
                    props["hs_object_id"] = cid
                rows.append(props)

            # Frame de sensibles conteniendo SOLO las columnas sensibles pedidas (+ id/hs_object_id si corresponde para join)
            needed_for_join = [c for c in ["id", "hs_object_id"] if c in HS_no_sens_list]
            sens_df = pd.DataFrame(rows)
            sens_df = _ensure_only_and_order(sens_df, needed_for_join + HS_sens_list)
            sens_frames_list.append(sens_df)

            print(f"[SENSITIVE [INFO ℹ️]] Chunk {idx}/{total_chunks} | Registros: {len(sens_df)}", flush=True)

            if throttle_ms_int > 0 and idx < total_chunks:
                time.sleep(throttle_ms_int / 1000.0)

        sens_df_full = (
            pd.concat(sens_frames_list, ignore_index=True).drop_duplicates()
            if sens_frames_list
            else pd.DataFrame(columns=(["id"] if "id" in HS_no_sens_list else []) + HS_sens_list)
        )

        # Merge usando SOLO claves pedidas por el usuario
        if "id" in HS_no_sens_list and "id" in sens_df_full.columns:
            df_final = base_df.merge(sens_df_full, on="id", how="left")
        elif "hs_object_id" in HS_no_sens_list and "hs_object_id" in sens_df_full.columns:
            df_final = base_df.merge(sens_df_full, on="hs_object_id", how="left")
        else:
            # Si no se pidieron claves de join, no podemos asociar sensibles
            df_final = base_df.copy()
            if HS_sens_list:
                raise ValueError("[PROCESS ERROR ❌] No se pudieron asociar datos sensibles: falta clave de join ('id' o 'hs_object_id') en HS_fields_no_sensitive_names_list.")
    else:
        print("[SENSITIVE [INFO ℹ️]] No se solicitaron propiedades sensibles. Saltando batch/read.", flush=True)
        df_final = base_df

    # ===============================
    # 6) SELECCIÓN FINAL ESTRICTA + FILTRO OPCIONAL
    # ===============================
    final_cols = list(dict.fromkeys(HS_no_sens_list + HS_sens_list))
    df_final = _ensure_only_and_order(df_final, final_cols)  # <— SOLO y EXACTAMENTE estas columnas

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
    print(f"[METRICS [INFO 📊]] Columnas finales (orden): {final_cols}", flush=True)
    print("[END [FINISHED ✅]] Extracción completada.", flush=True)

    return df_final
