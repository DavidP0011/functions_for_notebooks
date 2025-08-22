# __________________________________________________________________________________________________________________________________________________________
# df_to_whisper_transcribe_to_spreadsheet
# __________________________________________________________________________________________________________________________________________________________
def df_to_whisper_transcribe_to_spreadsheet(config: dict) -> None:
    """
    Transcribe archivos de vídeo usando un modelo Whisper y escribe los resultados en una hoja de cálculo de Google Sheets.

    Args:
        config (dict): Diccionario de configuración que debe incluir:
            - source_files_path_table_df (pd.DataFrame): DataFrame con al menos la columna especificada en 'field_name_for_file_path'.
            - target_files_path_table_spreadsheet_url (str): URL de la hoja de cálculo destino.
            - target_files_path_table_spreadsheet_worksheet (str): Nombre de la worksheet destino.
            - field_name_for_file_path (str): Nombre de la columna con la ruta del vídeo.
            - whisper_model_size (str): Tamaño del modelo Whisper (ej. "small", "medium", etc.).
            - whisper_language (str, opcional): Idioma de la transcripción (default: "en").
            - ini_environment_identificated (str): Identificador del entorno ("LOCAL", "COLAB", "COLAB_ENTERPRISE" o un project_id).
            - json_keyfile_local (str): Ruta del archivo JSON de credenciales para entorno LOCAL.
            - json_keyfile_colab (str): Ruta del archivo JSON de credenciales para entorno COLAB.
            - json_keyfile_GCP_secret_id (str): ID del secreto para entornos GCP.

    Returns:
        None

    Raises:
        ValueError: Si falta algún parámetro obligatorio o si el DataFrame fuente está vacío o no contiene la columna requerida.
        Exception: Para otros errores inesperados.
    """
    import os
    import time
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    from datetime import datetime
    import whisper
    import torch

    def _trocear_texto(texto: str, max_chars: int = 50000, max_partes: int = 10) -> list:
        """
        Trocea un texto en partes de longitud <= max_chars y retorna una lista de longitud max_partes.
        """
        trozos = [texto[i:i + max_chars] for i in range(0, len(texto), max_chars)]
        trozos = trozos[:max_partes]
        if len(trozos) < max_partes:
            trozos += [""] * (max_partes - len(trozos))
        return trozos

    def _process_transcription() -> None:
        # Validación inicial de parámetros
        required_keys = [
            "source_files_path_table_df",
            "target_files_path_table_spreadsheet_url",
            "target_files_path_table_spreadsheet_worksheet",
            "field_name_for_file_path",
            "whisper_model_size"
        ]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"[VALIDATION [ERROR ❌]] Falta el parámetro obligatorio: '{key}'.")
            # Para el DataFrame se evita evaluar su veracidad de forma ambigua
            if key != "source_files_path_table_df" and not config.get(key):
                raise ValueError(f"[VALIDATION [ERROR ❌]] El parámetro '{key}' está vacío.")

        # Extraer parámetros
        source_df = config["source_files_path_table_df"]
        if source_df is None:
            raise ValueError("[VALIDATION [ERROR ❌]] El DataFrame fuente no puede ser None.")
        if source_df.empty:
            raise ValueError("[VALIDATION [ERROR ❌]] El DataFrame fuente está vacío.")
        field_name = config["field_name_for_file_path"]
        if field_name not in source_df.columns:
            raise ValueError(f"[VALIDATION [ERROR ❌]] La columna '{field_name}' no existe en el DataFrame fuente.")

        target_spreadsheet_url = config["target_files_path_table_spreadsheet_url"]
        target_worksheet_name = config["target_files_path_table_spreadsheet_worksheet"]
        whisper_model_size = config["whisper_model_size"]
        whisper_language = config.get("whisper_language", "en")

        # Configurar credenciales según el entorno
        ini_env = config.get("ini_environment_identificated")
        if ini_env == "LOCAL":
            credentials_path = config.get("json_keyfile_local")
        elif ini_env == "COLAB":
            credentials_path = config.get("json_keyfile_colab")
        else:
            credentials_path = config.get("json_keyfile_GCP_secret_id")
        if not credentials_path:
            raise ValueError("[VALIDATION [ERROR ❌]] Credenciales no definidas para el entorno especificado.")

        # Autenticación con Google Sheets
        print("🔹🔹🔹 [START ▶️] Autenticando con Google Sheets 🔹🔹🔹", flush=True)
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        print("[AUTH SUCCESS ✅] Autenticación exitosa.", flush=True)

        # Preparar la hoja destino
        print(f"🔹🔹🔹 [START ▶️] Preparando hoja destino: {target_worksheet_name} 🔹🔹🔹", flush=True)
        spreadsheet_dest = client.open_by_url(target_spreadsheet_url)
        try:
            destination_sheet = spreadsheet_dest.worksheet(target_worksheet_name)
        except gspread.WorksheetNotFound:
            print("[INFO ℹ️] Hoja destino no existe. Creándola...", flush=True)
            destination_sheet = spreadsheet_dest.add_worksheet(title=target_worksheet_name, rows=1000, cols=30)
        destination_sheet.clear()
        dest_header = (
            ["file_path", "transcription_date", "transcription_duration", "whisper_model", "GPU_model"] +
            [f"transcription_part_{i}" for i in range(1, 11)] +
            [f"transcription_seg_part_{i}" for i in range(1, 11)]
        )
        destination_sheet.update("A1", [dest_header])
        print("[SHEET SUCCESS ✅] Hoja destino preparada y encabezados definidos.", flush=True)

        # Cargar modelo Whisper
        print(f"🔹🔹🔹 [START ▶️] Cargando modelo Whisper '{whisper_model_size}' 🔹🔹🔹", flush=True)
        model = whisper.load_model(whisper_model_size)
        gpu_model = torch.cuda.get_device_name(0) if torch.cuda.is_available() else "No"
        print("[MODEL SUCCESS ✅] Modelo Whisper cargado.", flush=True)

        # Convertir DataFrame a lista de diccionarios
        source_data = source_df.to_dict(orient="records")
        total_rows = len(source_data)
        print(f"[DATA INFO ℹ️] DataFrame fuente cargado. Total filas: {total_rows}", flush=True)

        # Procesar cada fila
        for idx, row_data in enumerate(source_data, start=1):
            video_path_value = row_data.get(field_name, "")
            if not video_path_value:
                continue

            print(f"[PROCESSING 🔄] ({idx}/{total_rows}) Transcribiendo: {video_path_value} (idioma='{whisper_language}')", flush=True)
            start_time_proc = time.time()
            try:
                result = model.transcribe(video_path_value, language=whisper_language)
                transcription_full = result.get("text", "")
                transcription_segments_full = "".join(
                    [f"[{seg['start']:.2f}s - {seg['end']:.2f}s]: {seg['text']}\n" for seg in result.get("segments", [])]
                )
            except Exception as e:
                print(f"[ERROR ❌] Error al transcribir {video_path_value}: {e}", flush=True)
                continue

            duration = round(time.time() - start_time_proc, 2)
            transcription_parts = _trocear_texto(transcription_full)
            transcription_seg_parts = _trocear_texto(transcription_segments_full)
            transcription_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row_to_insert = (
                [video_path_value, transcription_date, duration, whisper_model_size, gpu_model] +
                transcription_parts + transcription_seg_parts
            )

            try:
                destination_sheet.append_row(row_to_insert, value_input_option="USER_ENTERED")
                print(f"[WRITE SUCCESS ✅] Fila {idx} escrita correctamente.", flush=True)
            except Exception as e:
                print(f"[ERROR ❌] Error al escribir la fila {idx}: {e}", flush=True)
                continue

        print("🔹🔹🔹 [FINISHED ✅] Proceso de transcripción completado.", flush=True)

    # Ejecutar el proceso de transcripción con manejo de errores
    try:
        _process_transcription()
        print("Proceso completado con éxito.", flush=True)
    except ValueError as ve:
        print(f"Error en los parámetros: {ve}", flush=True)
    except Exception as e:
        print(f"Error inesperado: {e}", flush=True)
