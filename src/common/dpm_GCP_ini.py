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
def ini_google_secret_manager_instalation(params: dict) -> None:
    """
    Instala la librería google‑cloud‑secret‑manager en función del entorno de ejecución.

    Args:
        params (dict):
            - entorno_identificado_str (str): Valor que indica el entorno de ejecución.
              Los posibles valores pueden ser:
                * 'LOCAL'
                * 'COLAB'
                * 'COLAB_ENTERPRISE'
                * Cualquier otro valor que indique un entorno diferente (por ejemplo, el nombre de un proyecto GCP).

    Returns:
        None

    Raises:
        ValueError: Si falta la key 'entorno_identificado_str' en params.
    """
    import subprocess
    import sys

    entorno_identificado_str = params.get('entorno_identificado_str')
    if not entorno_identificado_str:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'entorno_identificado_str' en params.")

    # Instalar Secret Manager sólo en entornos donde no suele estar disponible
    if entorno_identificado_str in ['LOCAL', 'COLAB_ENTERPRISE']:
        try:
            print(f"[INFO ℹ️] Instalando google‑cloud‑secret‑manager en entorno '{entorno_identificado_str}'...", flush=True)
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "google-cloud-secret-manager"])
            # Verificamos que se pueda importar
            import google.cloud.secretmanager  # noqa: F401
            print("[INFO ✅] Librería google‑cloud‑secret‑manager instalada y disponible.", flush=True)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR ❌] Error al instalar google‑cloud‑secret‑manager: {e}", flush=True)
        except ImportError as e:
            print(f"[ERROR ❌] La instalación se realizó, pero no se pudo importar la librería: {e}", flush=True)
    else:
        print(f"[INFO ℹ️] El entorno '{entorno_identificado_str}' ya gestiona la librería de Secret Manager; no es necesaria la instalación.", flush=True)

