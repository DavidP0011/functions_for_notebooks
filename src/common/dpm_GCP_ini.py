# __________________________________________________________________________________________________________________________________________________________
# _ini_authenticate_API
# __________________________________________________________________________________________________________________________________________________________
def _ini_authenticate_API(p: dict, project_id_str: str, scopes: list):
    """
    Devuelve credenciales con los SCOPES solicitados, priorizando:
    1) Secret Manager (json_keyfile_GCP_secret_id)
    2) Keyfile local/colab si existe en disco
    3) ADC (default()) solo si soporta scopes adecuados
    """
    import os, json
    from google.oauth2.service_account import Credentials as SACreds
    from google.auth import default as default_auth
    from google.auth.transport.requests import Request

    env_str = (p.get("ini_environment_identificated") or "").upper().strip()
    key_local_path_str = (p.get("json_keyfile_local") or "").strip()
    key_colab_path_str = (p.get("json_keyfile_colab") or "").strip()
    secret_id_str = (p.get("json_keyfile_GCP_secret_id") or "").strip()

    # 1) Intentar SECRET MANAGER (recomendado en GCP / COLAB_ENTERPRISE)
    if secret_id_str:
        try:
            from google.cloud import secretmanager
            sm_client = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/{project_id_str}/secrets/{secret_id_str}/versions/latest"
            payload_bytes = sm_client.access_secret_version(name=secret_name).payload.data
            sa_info_dic = json.loads(payload_bytes.decode("utf-8"))
            creds = SACreds.from_service_account_info(sa_info_dic, scopes=scopes)
            print("[AUTHENTICATION [SUCCESS ✅]] Credenciales desde Secret Manager con scopes de Sheets/Drive.", flush=True)
            return creds
        except Exception as e:
            print(f"[AUTHENTICATION [WARNING ⚠️]] Secret Manager no disponible o secreto inválido: {e}", flush=True)

    # 2) Keyfile en disco si existe
    for candidate in [key_colab_path_str, key_local_path_str]:
        if candidate and os.path.exists(candidate):
            try:
                creds = SACreds.from_service_account_file(candidate, scopes=scopes)
                print(f"[AUTHENTICATION [SUCCESS ✅]] Credenciales desde keyfile: {candidate}", flush=True)
                return creds
            except Exception as e:
                print(f"[AUTHENTICATION [WARNING ⚠️]] Keyfile inválido ({candidate}): {e}", flush=True)

    # 3) ADC (solo si realmente aporta scopes válidos)
    creds, _ = default_auth()  # Ojo: aquí NO siempre vienen los scopes que pides
    try:
        # Si las credenciales requieren scopes, intentalo:
        if getattr(creds, "requires_scopes", False):
            creds = creds.with_scopes(scopes)
        # Refrescar para validar
        creds.refresh(Request())
    except Exception as e:
        print(f"[AUTHENTICATION [WARNING ⚠️]] ADC no refresca con scopes deseados: {e}", flush=True)

    # Heurística: si es Compute/GCE o Workbench y no soporta scopes de Sheets → mensaje claro
    # No todos los tipos implementan has_scopes; comprobamos best-effort:
    missing_scopes_list = [s for s in scopes if not getattr(creds, "has_scopes", lambda _: True)([s])]
    if missing_scopes_list:
        raise RuntimeError(
            "[AUTHENTICATION [ERROR ❌]] El token ADC no incluye scopes de Sheets/Drive. "
            "En Colab Enterprise o GCP usa 'json_keyfile_GCP_secret_id' (Service Account en Secret Manager) "
            "o proporciona un keyfile en 'json_keyfile_colab'/'json_keyfile_local'."
        )

    print("[AUTHENTICATION [SUCCESS ✅]] ADC válido con scopes requeridos.", flush=True)
    return creds















# __________________________________________________________________________________________________________________________________________________________
# ini_environment_identification
# __________________________________________________________________________________________________________________________________________________________
def ini_environment_identification() -> str:
    """
    Detecta el entorno de ejecución original basado en variables de entorno y módulos disponibles.

    La función utiliza la siguiente lógica:
      - Si la variable de entorno 'VERTEX_PRODUCT' tiene el valor 'COLAB_ENTERPRISE', se asume que se está ejecutando en Colab Enterprise y se devuelve ese valor original.
      - Si la variable de entorno 'GOOGLE_CLOUD_PROJECT' existe, se asume que se está ejecutando en GCP y se devuelve su valor original.
      - Si se puede importar el módulo 'google.colab', se asume que se está ejecutando en Colab (estándar) y se devuelve 'COLAB'.
      - Si ninguna de las condiciones anteriores se cumple, se asume que el entorno es Local y se devuelve 'LOCAL'.

    Returns:
        str: Cadena que representa el entorno de ejecución original. Los posibles valores son:
             - 'COLAB_ENTERPRISE'
             - El valor de la variable 'GOOGLE_CLOUD_PROJECT' (ej.: 'mi-proyecto')
             - 'COLAB'
             - 'LOCAL'
    """
    import os

    # ────────────────────────────── DETECCIÓN DEL ENTORNO ──────────────────────────────
    # Verificar si se está en Colab Enterprise / VERTEX_PRODUCT
    if os.environ.get('VERTEX_PRODUCT') == 'COLAB_ENTERPRISE':
        return os.environ.get('VERTEX_PRODUCT')
    
    # Verificar si se está en un entorno GCP (Google Cloud Platform)
    if os.environ.get('GOOGLE_CLOUD_PROJECT'):
        return os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    # Verificar si se está en Colab estándar
    try:
        import google.colab  # type: ignore
        return 'COLAB'
    except ImportError:
        pass

    # Por defecto, se asume que se está en un entorno local
    return 'LOCAL'














# __________________________________________________________________________________________________________________________________________________________
# ini_google_drive_instalation
# __________________________________________________________________________________________________________________________________________________________
def ini_google_drive_instalation(params: dict) -> None:
    """
    Monta Google Drive en función del entorno de ejecución especificado en params.

    Args:
        params (dict):
            - entorno_identificado_str (str): Valor que indica el entorno de ejecución.
              Los posibles valores pueden ser:
                * 'VERTEX_PRODUCT'
                * 'COLAB'
                * Cualquier otro valor que indique un entorno diferente (por ejemplo, el nombre de un proyecto GCP o 'LOCAL').

    Returns:
        None

    Raises:
        ValueError: Si falta la key 'entorno_identificado_str' en params.
    """
    entorno_identificado_str = params.get('entorno_identificado_str')
    if not entorno_identificado_str:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'entorno_identificado_str' en params.")

    # Montar Google Drive si el entorno es Colab (estándar o Enterprise)
    if entorno_identificado_str in ['COLAB']:
        try:
            from google.colab import drive
            drive.mount('/content/drive')
            print("[INFO ℹ️] Google Drive montado correctamente.", flush=True)
        except ImportError as e:
            print(f"[ERROR ❌] No se pudo importar google.colab para montar Google Drive: {e}", flush=True)
    else:
        print(f"[INFO ℹ️] El entorno '{entorno_identificado_str}' no requiere montaje de Google Drive.", flush=True)
















# _______________________________________________________________________________________________
# ini_google_secret_manager_instalation
# _______________________________________________________________________________________________
def ini_GCP_get_secret_manager(config: dict) -> dict:
    """
    Instala (si falta) google-cloud-secret-manager, selecciona credenciales según entorno
    y devuelve los secretos solicitados.

    Parámetros (config) — OBLIGATORIOS
    ----------------------------------
    - ini_environment_identificated (str): "LOCAL" | "COLAB" | "COLAB_ENTERPRISE" | "GCP"
    - GCP_json_keyfile_local (str): Ruta al JSON si entorno=LOCAL
    - GCP_json_keyfile_colab (str): Ruta al JSON si entorno=COLAB o COLAB_ENTERPRISE
    - GCP_json_keyfile_GCP_secret_id (str): ID corto o recurso completo de Secret Manager con el JSON de SA (solo GCP)
    - GBQ_project_id (str): "animum-dev-datawarehouse" (usado como project_id por defecto para IDs cortos)
    - GCP_secrets_requests_list (list): Lista de secretos a obtener (str o dict, ver más abajo)

    Opcionales
    ----------
    - as_bytes_bool (bool): False por defecto (devuelve str UTF-8)
    - max_retries_int (int): 3
    - retry_backoff_secs_float (float): 1.0
    - error_if_missing_bool (bool): True
    - pip_upgrade_bool (bool): False
    - pip_force_reinstall_bool (bool): False
    - pip_package_version_str (str|None): None
    - pip_verbose_bool (bool): False

    Formatos aceptados en GCP_secrets_requests_list:
      * "MY_SECRET"  → usa GBQ_project_id + versión 'latest'
      * {"secret_id_str": "ID", "alias_str": "out"}  → idem con alias
      * {"resource_name_str": "projects/<pid>/secrets/<id>/versions/<v>", "alias_str": "out"}
      * {"secret_id_str":"ID","project_id_str":"<pid>","version_str":"<v>","alias_str":"out"}

    Return
    ------
    dict: {alias/secret_id: valor_utf8_o_bytes}

    Raises
    ------
    ValueError: Validación de claves, instalación/importe de librería o acceso a secretos.
    """
    # =============================== Imports locales ===============================
    import os, sys, time, json, tempfile, subprocess
    from typing import Any, Dict, List
    from importlib import import_module, metadata as importlib_metadata

    # ---------------- Utilidades de log ----------------
    def _log(msg: str) -> None:
        print(msg, flush=True)

    # ---------------- Dependencia ----------------
    def _try_import_secret_manager() -> bool:
        try:
            import_module("google.cloud.secretmanager")
            import_module("google.api_core.exceptions")
            return True
        except Exception:
            return False

    def _ensure_dependency_installed(cfg: dict) -> None:
        pip_upgrade_bool = bool(cfg.get("pip_upgrade_bool", False))
        pip_force_reinstall_bool = bool(cfg.get("pip_force_reinstall_bool", False))
        pip_package_version_str = cfg.get("pip_package_version_str")
        pip_verbose_bool = bool(cfg.get("pip_verbose_bool", False))

        if _try_import_secret_manager() and not (pip_upgrade_bool or pip_force_reinstall_bool):
            try:
                ver = importlib_metadata.version("google-cloud-secret-manager")
            except importlib_metadata.PackageNotFoundError:
                ver = "unknown"
            _log(f"[DEPENDENCY SUCCESS ✅] google-cloud-secret-manager disponible. Versión: {ver}")
            return

        pkg = "google-cloud-secret-manager" + (f"=={pip_package_version_str}" if pip_package_version_str else "")
        cmd = [sys.executable, "-m", "pip", "install"]
        if not pip_verbose_bool: cmd.append("-q")
        if pip_upgrade_bool: cmd.append("--upgrade")
        if pip_force_reinstall_bool: cmd.append("--force-reinstall")
        cmd.append(pkg)

        _log(f"[DEPENDENCY START ▶️] pip {' '.join(cmd)}")
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as e:
            raise ValueError(f"[PROCESS ERROR ❌] Falló 'pip install {pkg}': {e}") from e

        if not _try_import_secret_manager():
            raise ValueError("[PROCESS ERROR ❌] Instalado pero no importable: google.cloud.secretmanager")

        try:
            ver = importlib_metadata.version("google-cloud-secret-manager")
        except importlib_metadata.PackageNotFoundError:
            ver = "unknown"
        _log(f"[DEPENDENCY SUCCESS ✅] Librería lista. Versión: {ver}")

    # ---------------- Helpers recursos secretos ----------------
    def _is_full_resource(name: str) -> bool:
        return isinstance(name, str) and name.startswith("projects/") and "/secrets/" in name

    def _normalize_resource(name: str, default_version: str = "latest") -> str:
        return name if "/versions/" in name else f"{name}/versions/{default_version}"

    def _resource_from_parts(project_id: str, secret_id: str, version: str = "latest") -> str:
        return f"projects/{project_id}/secrets/{secret_id}/versions/{version or 'latest'}"

    # ---------------- Validaciones base ----------------
    env = config.get("ini_environment_identificated")
    if not env:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta 'ini_environment_identificated'.")

    # Claves obligatorias de rutas / id SA
    req_keys = ["GCP_json_keyfile_local", "GCP_json_keyfile_colab", "GCP_json_keyfile_GCP_secret_id", "GBQ_project_id", "GCP_secrets_requests_list"]
    missing = [k for k in req_keys if k not in config]
    if missing:
        raise ValueError(f"[VALIDATION [ERROR ❌]] Faltan claves obligatorias: {missing}")

    project_id = config.get("GBQ_project_id")  # Usaremos este como default project
    secrets_requests = config.get("GCP_secrets_requests_list")
    if not isinstance(secrets_requests, list) or not secrets_requests:
        raise ValueError("[VALIDATION [ERROR ❌]] 'GCP_secrets_requests_list' debe ser lista no vacía.")

    as_bytes_bool = bool(config.get("as_bytes_bool", False))
    max_retries = int(config.get("max_retries_int", 3))
    backoff_base = float(config.get("retry_backoff_secs_float", 1.0))
    error_if_missing = bool(config.get("error_if_missing_bool", True))

    _log("🔹🔹🔹 [START ▶️] Secret Manager: inicialización + obtención 🔹🔹🔹")
    _log(f"[INFO ℹ️] Entorno: {env} | Proyecto por defecto: {project_id}")

    # ---------------- Dependencia ----------------
    _ensure_dependency_installed(config)
    from google.cloud import secretmanager
    from google.api_core import exceptions as gax

    # ---------------- Selección de credenciales por entorno ----------------
    def _set_adc_from_path(path: str):
        if not path or not os.path.exists(path):
            raise ValueError(f"[VALIDATION [ERROR ❌]] Ruta credenciales inválida o inexistente: {path}")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        _log(f"[AUTHENTICATION [INFO] ℹ️] GOOGLE_APPLICATION_CREDENTIALS set → {path}")

    def _bootstrap_sa_from_secret_id(secret_id_or_resource: str, default_project: str) -> str:
        """
        Lee el JSON de SA desde Secret Manager (usando ADC del entorno GCP),
        escribe a un archivo temporal y devuelve su ruta.
        """
        # Resolver recurso completo
        if _is_full_resource(secret_id_or_resource):
            resource = _normalize_resource(secret_id_or_resource)
        else:
            if not default_project:
                raise ValueError("[VALIDATION [ERROR ❌]] Falta 'GBQ_project_id' para resolver el secret_id del SA en GCP.")
            resource = _resource_from_parts(default_project, secret_id_or_resource, "latest")

        # Cliente temporal con ADC del entorno
        tmp_client = secretmanager.SecretManagerServiceClient()
        resp = tmp_client.access_secret_version(name=resource)
        sa_bytes = resp.payload.data

        # Persistir a archivo temporal
        try:
            json.loads(sa_bytes.decode("utf-8"))  # validación rápida
        except Exception as e:
            raise ValueError(f"[VALIDATION [ERROR ❌]] El secreto de SA no es JSON válido: {e}") from e

        fd, tmp_path = tempfile.mkstemp(prefix="sa_", suffix=".json")
        with os.fdopen(fd, "wb") as f:
            f.write(sa_bytes)
        _log(f"[AUTHENTICATION [INFO] ℹ️] SA JSON recuperado de Secret Manager → {resource}")
        return tmp_path

    if env == "LOCAL":
        _set_adc_from_path(config.get("GCP_json_keyfile_local"))
    elif env in ("COLAB", "COLAB_ENTERPRISE"):
        _set_adc_from_path(config.get("GCP_json_keyfile_colab"))
    elif env == "GCP":
        sa_secret_id = config.get("GCP_json_keyfile_GCP_secret_id")
        if sa_secret_id:  # Opcional: usar SA desde Secret Manager
            tmp_sa_path = _bootstrap_sa_from_secret_id(sa_secret_id, project_id)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp_sa_path
            _log(f"[AUTHENTICATION [SUCCESS ✅]] ADC configurado desde secreto → {tmp_sa_path}")
        else:
            _log("[AUTHENTICATION [INFO] ℹ️] Usando ADC del entorno GCP (sin ruta explícita).")
    else:
        _log(f"[AUTHENTICATION [WARNING ⚠️]] Entorno '{env}' no reconocido. Intentando ADC por defecto…")

    # Cliente definitivo
    try:
        sm_client = secretmanager.SecretManagerServiceClient()
        _log("[AUTHENTICATION [SUCCESS ✅]] Cliente de Secret Manager listo.")
    except Exception as e:
        raise ValueError(f"[AUTHENTICATION [ERROR ❌]] No se pudo crear el cliente: {e}") from e

    # ---------------- Parseo de items ----------------
    def _out_key(item: Any, fallback: str) -> str:
        return item.get("alias_str") if isinstance(item, dict) and item.get("alias_str") else fallback

    def _parse_item(item: Any) -> Dict[str, str]:
        if isinstance(item, str):
            if _is_full_resource(item):
                resource = _normalize_resource(item)
                try:
                    sid = resource.split("/secrets/")[1].split("/")[0]
                except Exception:
                    sid = resource
                return {"resource": resource, "key": sid}
            else:
                if not project_id:
                    raise ValueError("[VALIDATION [ERROR ❌]] Falta 'GBQ_project_id' para ID corto.")
                return {"resource": _resource_from_parts(project_id, item, "latest"), "key": item}

        if isinstance(item, dict):
            if item.get("resource_name_str"):
                resource = _normalize_resource(item["resource_name_str"])
                try:
                    sid = resource.split("/secrets/")[1].split("/")[0]
                except Exception:
                    sid = resource
                return {"resource": resource, "key": _out_key(item, sid)}

            sid = item.get("secret_id_str")
            if not sid:
                raise ValueError("[VALIDATION [ERROR ❌]] Falta 'secret_id_str' o 'resource_name_str' en item.")

            pid = item.get("project_id_str") or project_id
            if not pid:
                raise ValueError("[VALIDATION [ERROR ❌]] Falta 'project_id_str' y no hay 'GBQ_project_id'.")

            ver = item.get("version_str", "latest")
            resource = _resource_from_parts(pid, sid, ver)
            return {"resource": resource, "key": _out_key(item, sid)}

        raise ValueError("[VALIDATION [ERROR ❌]] Cada item debe ser str o dict.")

    parsed = []
    for idx, it in enumerate(secrets_requests, 1):
        p = _parse_item(it)
        parsed.append(p)
        _log(f"[PARSER [SUCCESS ✅]] Item #{idx} → {p['resource']} (key='{p['key']}')")

    # ---------------- Acceso a secretos ----------------
    results: Dict[str, Any] = {}
    failures: List[Dict[str, str]] = []
    _log("[EXTRACTION [START ▶️]] Accediendo a versiones de secretos…")

    for i, p in enumerate(parsed, 1):
        name = p["resource"]
        out_key = p["key"]
        last_err = None

        for attempt in range(1, max_retries + 1):
            try:
                resp = sm_client.access_secret_version(name=name)
                data_bytes = resp.payload.data
                results[out_key] = data_bytes if as_bytes_bool else data_bytes.decode("utf-8")
                _log(f"[EXTRACTION [SUCCESS ✅]] #{i} '{out_key}' obtenido (len={len(data_bytes)})")
                last_err = None
                break
            except (gax.DeadlineExceeded, gax.ServiceUnavailable, gax.ResourceExhausted, gax.InternalServerError) as e:
                last_err = e
                sleep_s = backoff_base * (2 ** (attempt - 1))
                _log(f"[EXTRACTION [WARNING ⚠️]] #{i} intento {attempt}/{max_retries}: {e}. Reintento en {sleep_s:0.2f}s…")
                time.sleep(sleep_s)
            except gax.NotFound as e:
                last_err = e
                _log(f"[EXTRACTION [ERROR ❌]] #{i} NotFound: {name}")
                break
            except Exception as e:
                last_err = e
                _log(f"[EXTRACTION [ERROR ❌]] #{i} Error no controlado: {e}")
                break

        if last_err is not None:
            failures.append({"key": out_key, "name": name, "error": str(last_err)})

    _log(f"[METRICS [INFO 📊]] Intentados: {len(parsed)} | OK: {len(results)} | KO: {len(failures)}")

    if failures and error_if_missing:
        lines = [f"- {f['key']} ← {f['name']} | {f['error']}" for f in failures[:5]]
        more = "" if len(failures) <= 5 else f" (+{len(failures)-5} más)"
        raise ValueError("[PROCESS ERROR ❌] No fue posible obtener algunos secretos:\n" + "\n".join(lines) + more)

    _log("[END [FINISHED ✅]]")
    return results


