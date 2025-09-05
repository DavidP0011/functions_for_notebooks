from __future__ import annotations

from common.dpm_GCP_ini import _ini_authenticate_API

# __________________________________________________________________________________________________________________________________________________________
# HS_sensitive_data_to_df
# __________________________________________________________________________________________________________________________________________________________
def HS_sensitive_data_to_df(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Recupera contactos de HubSpot devolviendo SOLO campos no sensibles y sensibles.

    Esta función se encarga de:
      - Autenticar en Google Cloud Platform a partir de la configuración
        proporcionada, pudiendo usar credenciales locales, de Colab o
        obtenidas de Secret Manager.
      - Recuperar el token de acceso a la API de HubSpot desde Secret
        Manager. Si la librería ``google‑cloud‑secret‑manager`` no está
        instalada, se intentará instalar de forma dinámica en entornos
        locales o Colab Enterprise mediante ``ini_google_secret_manager_instalation``.
      - Construir y enviar consultas paginadas a la API de HubSpot según
        filtros de fecha, devolviendo todos los contactos que cumplan los
        criterios.
      - Filtrar opcionalmente para incluir solo registros con algún valor
        sensible.
      - Devolver los datos en un ``pandas.DataFrame`` ordenado por
        ``createdate``.

    Args:
        config (dict): Diccionario de configuración con las claves obligatorias:
            * ``ini_environment_identificated`` (str): Identificador del entorno.
            * ``json_keyfile_local`` (str): Ruta al JSON de credenciales para entorno LOCAL.
            * ``json_keyfile_colab`` (str): Ruta al JSON de credenciales para entorno COLAB.
            * ``json_keyfile_GCP_secret_id`` (str): ID del secreto con credenciales en GCP.
            * ``GCP_secret_name`` (str): Nombre del secreto con el token de HubSpot.
            * ``HS_fields_no_sensitive_names_list`` (list[str]): Propiedades no sensibles.
            * ``HS_fields_sensitive_names_list`` (list[str]): Propiedades sensibles.
            * ``HS_contact_filter_createdate`` (dict): Filtro de fechas con claves
              ``from``, ``to`` y ``mode`` (between/after/before).
        config opcionales:
            * ``HS_only_records_with_any_sensitive_value_bool`` (bool): Si es
              True, solo devuelve contactos con algún valor sensible.
            * ``HS_debug_contact_id`` (int|str): Si se proporciona, se obtendrá
              exclusivamente ese contacto para depuración.
            * ``HS_return_partial_on_error_bool`` (bool): Si es True, al
              producirse un error se devuelve el DataFrame parcialmente
              construido en lugar de lanzar una excepción.

    Returns:
        pandas.DataFrame: Un DataFrame con las propiedades solicitadas.

    Raises:
        ValueError: Si faltan claves obligatorias en ``config`` o si los
            formatos de datos son incorrectos.
        ImportError: Si no se logra instalar o importar la librería de
            Secret Manager necesaria para recuperar el token de HubSpot.
    """

    import time
    from datetime import datetime, timezone
    from os import getenv
    from typing import Any, Dict, List, Optional

    import pandas as pd
    import requests
    import os
    import json
    from google.oauth2 import service_account
    # ───────────────── Importación dinámica del cliente de Secret Manager ─────────────────
    try:
        # Se intenta importar el cliente de Secret Manager directamente.
        from google.cloud import secretmanager  # type: ignore
    except Exception:
        # Si falla, se detecta el entorno y se instala la librería apropiada.
        from common.dpm_GCP_ini import (
            ini_environment_identification,
            ini_google_secret_manager_instalation,
        )
        env_detected = ini_environment_identification()
        print(
            f"[DEPENDENCIA [INFO ℹ️]] Módulo 'secretmanager' no disponible. "
            f"Intentando instalar 'google‑cloud‑secret‑manager' para el entorno '{env_detected}'...",
            flush=True,
        )
        # Se intenta la instalación y posterior importación.
        try:
            ini_google_secret_manager_instalation({"entorno_identificado_str": env_detected})
            from google.cloud import secretmanager  # type: ignore  # retry import
        except Exception as install_exc:
            raise ImportError(
                f"[DEPENDENCIA [ERROR ❌]] No se pudo instalar o importar 'google‑cloud‑secret‑manager': {install_exc}"
            )

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
                    print(
                        f"[HUBSPOT API WARNING ⚠️] HTTP {resp.status_code} intento {attempt}/{MAX_RETRIES}. "
                        f"Reintento en {wait:.1f}s.",
                        flush=True,
                    )
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                wait = BACKOFF_BASE * (2 ** (attempt - 1))
                print(
                    f"[HUBSPOT API WARNING ⚠️] Error de red intento {attempt}/{MAX_RETRIES}: {exc}. "
                    f"Reintento en {wait:.1f}s.",
                    flush=True,
                )
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
    mode = {"between": "between", "after": "after", "since": "after", "before": "before", "until": "before"}.get(
        mode, "between"
    )

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
    # La autenticación se realiza siguiendo la lógica de otras funciones del
    # repositorio: si existe la variable de entorno GOOGLE_CLOUD_PROJECT se
    # entiende que estamos en GCP (o Vertex) y se utilizan credenciales
    # predeterminadas para acceder al Secret Manager y obtener un JSON con las
    # credenciales del servicio; de lo contrario, se usan credenciales
    # proporcionadas en un archivo local/Colab.
    gcp_project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    credentials = None  # tipo: Optional[service_account.Credentials]
    project_id = None
    try:
        if gcp_project:
            # Entorno GCP: obtener credenciales del servicio desde Secret Manager
            secret_id_credentials = config.get("json_keyfile_GCP_secret_id")
            if not secret_id_credentials:
                raise ValueError(
                    "[AUTHENTICATION [ERROR ❌]] En entornos GCP se debe proporcionar 'json_keyfile_GCP_secret_id' para obtener las credenciales."
                )
            # Usamos el cliente de Secret Manager con credenciales predeterminadas
            sm_default = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{gcp_project}/secrets/{secret_id_credentials}/versions/latest"
            response = sm_default.access_secret_version(name=secret_name)
            secret_str = response.payload.data.decode("UTF-8")
            secret_info = json.loads(secret_str)
            credentials = service_account.Credentials.from_service_account_info(secret_info)
            project_id = credentials.project_id or gcp_project
        else:
            # Entorno no GCP: usar archivo JSON de credenciales
            # Se da preferencia a json_keyfile_colab sobre json_keyfile_local
            json_path = config.get("json_keyfile_colab") or config.get("json_keyfile_local")
            if not json_path:
                raise ValueError(
                    "[AUTHENTICATION [ERROR ❌]] En entornos no GCP se debe proporcionar 'json_keyfile_colab' o 'json_keyfile_local'."
                )
            credentials = service_account.Credentials.from_service_account_file(json_path)
            project_id = credentials.project_id or config.get("ini_environment_identificated")
        # Verificación final de project_id
        if not project_id:
            raise ValueError("[AUTHENTICATION [ERROR ❌]] No se pudo determinar un project_id válido.")
    except Exception as auth_exc:
        raise ValueError(f"[AUTHENTICATION [ERROR ❌]] Error al obtener credenciales: {auth_exc}")

    # Obtener el token de HubSpot desde Secret Manager usando las credenciales obtenidas
    try:
        sm = secretmanager.SecretManagerServiceClient(credentials=credentials)
        secret_path = f"projects/{project_id}/secrets/{config['GCP_secret_name']}/versions/latest"
        hs_token = sm.access_secret_version(name=secret_path).payload.data.decode("utf-8")
        print(
            f"[SECRET MANAGER SUCCESS ✅] Token de HubSpot recuperado desde proyecto '{project_id}'.",
            flush=True,
        )
    except Exception as e:
        raise ValueError(
            f"[SECRET MANAGER [ERROR ❌]] No se pudo leer el secreto '{config['GCP_secret_name']}' en '{project_id}': {e}"
        )

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
        data = _hubspot_request(
            "GET",
            HS_CONTACT_URL.format(contact_id=str(debug_contact_id)),
            hs_headers,
            params=params,
        )
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
    PAGE_LIMIT = 100  # fijo
    WINDOW_LIMIT = 9_500  # umbral por ventana
    INITIAL_DAYS = 90  # tamaño inicial ventana
    MIN_DAYS = 1
    DAY_MS = 24 * 3600 * 1000

    def _build_payload(start_ms: int, end_ms: int, after: Optional[str]) -> Dict[str, Any]:
        filters: List[Dict[str, str]] = []
        if ts_from is not None:
            filters.append({"propertyName": filter_property, "operator": "GTE", "value": str(start_ms)})
        if ts_to is not None:
            filters.append({"propertyName": filter_property, "operator": "LTE", "value": str(end_ms)})
        payload: Dict[str, Any] = {
            "properties": all_props,
            "limit": PAGE_LIMIT,
            "filterGroups": [{"filters": filters}],
        }
        if after:
            payload["after"] = after
        return payload

    # Lista acumuladora para las filas de contactos
    rows: List[Dict[str, Any]] = []

    # Ventaneo: recorre el rango de fechas solicitado en ventanas adaptativas
    # para respetar el límite de resultados de la API (~10k por solicitud).
    start_ms = ts_from if ts_from is not None else -float("inf")
    end_ms = ts_to if ts_to is not None else int(time.time() * 1000)
    days_window = INITIAL_DAYS

    while start_ms < end_ms:
        window_end_ms = min(start_ms + days_window * DAY_MS, end_ms)
        after: Optional[str] = None
        total_window_results = 0
        while True:
            payload = _build_payload(start_ms, window_end_ms, after)
            data = _hubspot_request("POST", HS_SEARCH_URL, hs_headers, json=payload)
            results = data.get("results", [])
            paging = data.get("paging", {})
            total_window_results += len(results)
            for item in results:
                props = item.get("properties", {}) or {}
                row = {k: props.get(k) for k in all_props}
                row["contact_id"] = item.get("id")
                # Filtrar si se requiere un valor sensible
                if only_sensitive and not any(row.get(p) for p in props_sens):
                    continue
                rows.append(row)
            after = paging.get("next", {}).get("after") if paging else None
            if not after:
                break
        # Ajustar el tamaño de la ventana según los resultados obtenidos
        if total_window_results >= WINDOW_LIMIT and days_window > MIN_DAYS:
            days_window = max(MIN_DAYS, days_window // 2)
        elif total_window_results < WINDOW_LIMIT // 2 and (start_ms + days_window * DAY_MS) < end_ms:
            days_window = min(INITIAL_DAYS, days_window * 2)
        # Avanzar al siguiente intervalo
        start_ms = window_end_ms + 1

    # Construcción del DataFrame final
    df = pd.DataFrame(rows)
    # Ordenar por fecha de creación y reorganizar columnas
    if not df.empty:
        cols = ["contact_id", filter_property] + [p for p in all_props if p not in ("contact_id", filter_property)]
        df = df.reindex(columns=cols)

    print("[END FINISHED ✅] Proceso completado.", flush=True)
    return df
