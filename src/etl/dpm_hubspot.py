from __future__ import annotations

import time
from datetime import datetime, timezone
from os import getenv
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from google.cloud import secretmanager

from common.dpm_GCP_ini import _ini_authenticate_API   )


# __________________________________________________________________________________________________________________________________________________________
# HS_sensitive_data_to_df
# __________________________________________________________________________________________________________________________________________________________
def HS_sensitive_data_to_df(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Recupera contactos de HubSpot devolviendo SOLO campos No Sensibles + Sensibles.
    - Autentica en GCP con `_ini_authenticate_API`.
    - Obtiene el token de HubSpot desde Secret Manager (`GCP_secret_name`).
    - Pagina el Search de contactos con filtro por `createdate`.
    - (Opcional) Devuelve sólo contactos con algún valor sensible.

    config (claves requeridas):
      ini_environment_identificated : str   # 'LOCAL' | 'COLAB' | <gcp-project-id>
      json_keyfile_local            : str   # si LOCAL
      json_keyfile_colab            : str   # si COLAB
      json_keyfile_GCP_secret_id    : str   # si GCP (si tu helper lo usa)
      GCP_secret_name               : str   # secreto con token HubSpot
      HS_fields_no_sensitive_names_list : list[str]
      HS_fields_sensitive_names_list     : list[str]
      HS_contact_filter_createdate       : dict {from,to,mode}

    config (opcionales):
      HS_only_records_with_any_sensitive_value_bool : bool
      HS_debug_contact_id                           : int|str
      HS_return_partial_on_error_bool               : bool
    """

    # ───────────────── Helpers ─────────────────
    def _to_ms(value: Any) -> int:
        if isinstance(value, (int, float)):
            n = int(value)
            return n if n >= 10_000_000_000 else n * 1000
        if isinstance(value, str):
            s = value.strip()
            if s.isdigit():
                n = int(s)
                return n if n >= 10_000_000_000 else n * 1000
            s = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        raise ValueError(f"Valor de fecha no soportado: {type(value)!r}")

    def _hubspot_request(method: str, url: str, headers: Dict[str, str], **kwargs) -> Dict[str, Any]:
        MAX_RETRIES, BACKOFF_BASE, TIMEOUT = 5, 1.5, 60
        RETRYABLE = {429, 500, 502, 503, 504}
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.request(method, url, headers=headers, timeout=TIMEOUT, **kwargs)
                if resp.status_code in RETRYABLE:
                    ra = resp.headers.get("Retry-After")
                    wait = float(ra) if ra and str(ra).isdigit() else BACKOFF_BASE * (2 ** (attempt - 1))
                    print(f"[HUBSPOT API WARNING ⚠️] HTTP {resp.status_code} intento {attempt}/{MAX_RETRIES}. Reintento en {wait:.1f}s.", flush=True)
                    time.sleep(wait); continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                wait = BACKOFF_BASE * (2 ** (attempt - 1))
                print(f"[HUBSPOT API WARNING ⚠️] Error de red intento {attempt}/{MAX_RETRIES}: {exc}. Reintento en {wait:.1f}s.", flush=True)
                time.sleep(wait)
        raise requests.HTTPError(f"Fallo tras {MAX_RETRIES} intentos: {last_exc}")

    # ───────────────── Inicio ─────────────────
    print("🔹🔹🔹 [START ▶️] Extracción de campos sensibles desde HubSpot 🔹🔹🔹", flush=True)

    # Validación config
    required = [
        "ini_environment_identificated",
        "json_keyfile_local",
        "json_keyfile_colab",
        "json_keyfile_GCP_secret_id",
        "GCP_secret_name",
        "HS_fields_no_sensitive_names_list",
        "HS_fields_sensitive_names_list",
        "HS_contact_filter_createdate",
    ]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"[VALIDATION [ERROR ❌]] Faltan claves obligatorias: {missing}")

    if not isinstance(config["HS_fields_no_sensitive_names_list"], list) or not isinstance(
        config["HS_fields_sensitive_names_list"], list
    ):
        raise ValueError("[VALIDATION [ERROR ❌]] HS_fields_* deben ser listas.")

    # Filtro fechas
    date_dic = config["HS_contact_filter_createdate"]
    if not isinstance(date_dic, dict):
        raise ValueError("[VALIDATION [ERROR ❌]] 'HS_contact_filter_createdate' debe ser dict.")
    mode = str(date_dic.get("mode", "between")).lower()
    mode = {"between": "between", "after": "after", "since": "after", "before": "before", "until": "before"}.get(mode, "between")

    ts_from: Optional[int] = None
    ts_to: Optional[int] = None
    if mode in ("between", "after"):
        if date_dic.get("from") is None:
            raise ValueError("[VALIDATION [ERROR ❌]] Se requiere 'from' para el modo seleccionado.")
        ts_from = _to_ms(date_dic["from"])
    if mode in ("between", "before"):
        if date_dic.get("to") is None:
            raise ValueError("[VALIDATION [ERROR ❌]] Se requiere 'to' para el modo seleccionado.")
        ts_to = _to_ms(date_dic["to"])

    only_sensitive = bool(config.get("HS_only_records_with_any_sensitive_value_bool", False))
    debug_contact_id = config.get("HS_debug_contact_id")
    partial_on_error = bool(config.get("HS_return_partial_on_error_bool", False))

    # ───────────────── Auth GCP + Secret Manager ─────────────────
    env_id = config["ini_environment_identificated"]
    try:
        auth_cfg = {
            "ini_environment_identificated": env_id,
            "json_keyfile_local": config.get("json_keyfile_local"),
            "json_keyfile_colab": config.get("json_keyfile_colab"),
            "json_keyfile_GCP_secret_id": config.get("json_keyfile_GCP_secret_id"),
        }
        credentials = _ini_authenticate_API(auth_cfg, env_id)
    except Exception as e:
        raise ValueError(f"[AUTHENTICATION [ERROR ❌]] _ini_authenticate_API: {e}")

    # Deducir project_id REAL de las credenciales / entorno; fallback a env_id
    project_id = (
        getattr(credentials, "project_id", None)
        or getenv("GOOGLE_CLOUD_PROJECT")
        or getenv("GCLOUD_PROJECT")
        or getenv("GCP_PROJECT")
        or env_id
    )

    try:
        sm = secretmanager.SecretManagerServiceClient(credentials=credentials)
        secret_path = f"projects/{project_id}/secrets/{config['GCP_secret_name']}/versions/latest"
        hs_token = sm.access_secret_version(name=secret_path).payload.data.decode("utf-8")
        print(f"[SECRET MANAGER SUCCESS ✅] Token de HubSpot recuperado desde proyecto '{project_id}'.", flush=True)
    except Exception as e:
        raise ValueError(f"[SECRET MANAGER [ERROR ❌]] No se pudo leer el secreto '{config['GCP_secret_name']}' en '{project_id}': {e}")

    # ───────────────── Propiedades a solicitar ─────────────────
    props_no: List[str] = config["HS_fields_no_sensitive_names_list"]
    props_sens: List[str] = config["HS_fields_sensitive_names_list"]
    filter_property = "createdate"
    all_props: List[str] = list(dict.fromkeys(props_no + props_sens + [filter_property]))

    # ───────────────── HubSpot API setup ─────────────────
    HS_SEARCH_URL = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    HS_CONTACT_URL = "https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    hs_headers = {"Authorization": f"Bearer {hs_token}", "Content-Type": "application/json"}

    # Debug: único contacto
    if debug_contact_id not in (None, ""):
        params = {"properties": ",".join(all_props)}
        data = _hubspot_request("GET", HS_CONTACT_URL.format(contact_id=str(debug_contact_id)), hs_headers, params=params)
        props = data.get("properties", {}) or {}
        row = {k: props.get(k) for k in all_props}
        row["contact_id"] = data.get("id")
        df_debug = pd.DataFrame([row])
        cols = ["contact_id", "createdate"] + [p for p in all_props if p not in ("contact_id", "createdate")]
        df_debug = df_debug.reindex(columns=cols)
        print("[DATAFRAME SUCCESS ✅] Contacto recuperado en modo depuración.", flush=True)
        print("[END FINISHED ✅] Proceso completado.", flush=True)
        return df_debug

    # ───────────────── Paginación con ventanas (evitar ~10k) ─────────────────
    print("[HUBSPOT API START ▶️] Solicitando contactos con filtros especificados.", flush=True)
    PAGE_LIMIT = 100           # fijo
    WINDOW_LIMIT = 9_500       # umbral por ventana
    INITIAL_DAYS = 90          # tamaño inicial ventana
    MIN_DAYS = 1
    DAY_MS = 24 * 3600 * 1000

    def _build_payload(start_ms: int, end_ms: int, after: Optional[str]) -> Dict[str, Any]:
        filters = []
        if ts_from is not None:
            filters.append({"propertyName": filter_property, "operator": "GTE", "value": str(start_ms)})
        if ts_to is not None:
            filters.append({"propertyName": filter_property, "operator": "LTE", "value": str(end_ms)})
        payload = {
            "filterGroups": [{"filters": filters}],
            "properties": all_props,
            "limit": PAGE_LIMIT,
            "sorts": [{"propertyName": filter_property, "direction": "ASCENDING"}],
        }
        if after:
            payload["after"] = after
        return payload

    api_calls = 0
    results_all: List[Dict[str, Any]] = []

    def _fetch_window(start_ms: int, end_ms: int, depth: int = 0) -> List[Dict[str, Any]]:
        nonlocal api_calls
        indent = "  " * depth
        print(f"{indent}[CHUNK START ▶️] Ventana {start_ms} – {end_ms}", flush=True)
        after = None
        acc: List[Dict[str, Any]] = []
        while True:
            try:
                data = _hubspot_request("POST", HS_SEARCH_URL, hs_headers, json=_build_payload(start_ms, end_ms, after))
            except Exception as e:
                if partial_on_error and acc:
                    print(f"{indent}[CHUNK FAILED ⚠️] Ventana parcial devuelta por error: {e}", flush=True)
                    break
                raise
            api_calls += 1
            page = data.get("results", [])
            acc.extend(page)
            paging = data.get("paging", {})
            after = paging.get("next", {}).get("after")
            if len(acc) >= WINDOW_LIMIT and after:
                if (end_ms - start_ms) <= MIN_DAYS * DAY_MS:
                    print(f"{indent}[CHUNK WARNING ⚠️] Límite superado en ventana mínima; devuelvo parcial.", flush=True)
                    break
                mid = start_ms + (end_ms - start_ms) // 2
                print(f"{indent}[CHUNK SPLIT ⚠️] Partiendo ventana.", flush=True)
                left = _fetch_window(start_ms, mid, depth + 1)
                right = _fetch_window(mid + 1, end_ms, depth + 1)
                return left + right
            if not after:
                break
            time.sleep(0.25)
        print(f"{indent}[CHUNK FINISHED ✅] Registros: {len(acc)}", flush=True)
        return acc

    overall_start = ts_from if ts_from is not None else 0
    overall_end = ts_to if ts_to is not None else int(time.time() * 1000)

    if overall_end - overall_start <= INITIAL_DAYS * DAY_MS:
        results_all.extend(_fetch_window(overall_start, overall_end, 0))
    else:
        cur = overall_start
        while cur <= overall_end:
            end = min(overall_end, cur + INITIAL_DAYS * DAY_MS - 1)
            results_all.extend(_fetch_window(cur, end, 0))
            cur = end + 1

    print(f"[HUBSPOT API FINISHED ✅] Contactos recuperados: {len(results_all)}", flush=True)

    # ───────────────── Filtrado opcional por sensibles ─────────────────
    if only_sensitive:
        before = len(results_all)
        sens_set = set(props_sens)
        results_all = [
            c for c in results_all
            if any((c.get("properties", {}) or {}).get(f) not in (None, "", [], {}) for f in sens_set)
        ]
        print(f"[FILTER INFO ℹ️] Filtrando sin datos sensibles: {before} -> {len(results_all)}", flush=True)

    # ───────────────── DataFrame ─────────────────
    rows: List[Dict[str, Any]] = []
    for c in results_all:
        props = c.get("properties", {}) or {}
        row = {k: props.get(k) for k in all_props}
        row["contact_id"] = c.get("id")
        rows.append(row)

    df = pd.DataFrame(rows)
    col_order = ["contact_id", "createdate"] + [p for p in all_props if p not in ("contact_id", "createdate")]
    if not df.empty:
        df = df.reindex(columns=col_order)

    print("[DATAFRAME SUCCESS ✅] DataFrame construido.", flush=True)
    print("🔹🔹🔹 [METRICS 📊] Resumen de Ejecución 🔹🔹🔹", flush=True)
    print(f"[METRICS INFO ℹ️] Filas devueltas: {len(df)}", flush=True)
    print(f"[METRICS INFO ℹ️] Llamadas a la API: {api_calls}", flush=True)
    print(f"[METRICS INFO ℹ️] Entorno(auth): {env_id}  | Proyecto(secrets): {project_id}", flush=True)
    print("[END FINISHED ✅] Proceso completado.", flush=True)
    return df
