# __________________________________________________________________________________________________________________________________________________________
# files_path_collect_df
# __________________________________________________________________________________________________________________________________________________________
def files_path_collect_df(config: dict) -> "pd.DataFrame":
    """
    Busca archivos de video en una ruta (local o Google Drive), extrae sus propiedades usando ffprobe
    y devuelve un DataFrame con los resultados. 

    Args:
        config (dict):
            - video_files_root_path (str): Ruta raíz donde buscar archivos de video.
            - video_files_target_search_folder (list): Lista de subcarpetas de interés.
            - video_files_target_search_extension (list): Lista de extensiones de archivo (e.g., [".mp4"]).
            - ini_environment_identificated (str): Entorno de ejecución.
            - json_keyfile_local (str): Ruta del JSON de credenciales LOCAL.
            - json_keyfile_colab (str): Ruta del JSON de credenciales COLAB.
            - json_keyfile_GCP_secret_id (str): ID de secreto GCP.

    Returns:
        pd.DataFrame: DataFrame con la información y metadatos de los videos.

    Raises:
        ValueError: Si falta algún parámetro obligatorio o no se encuentran archivos.
        NotImplementedError: Si la ruta es de Google Drive.
        Exception: Para errores inesperados.
    """
    import os
    import subprocess
    import json
    import pandas as pd
    from datetime import datetime
    from time import time
    from typing import Optional
    try:
        from IPython.display import clear_output
    except ImportError:
        def clear_output(wait=False):
            os.system('cls' if os.name == 'nt' else 'clear')

    # Contador de líneas impresas
    line_counter = 0
    max_lines = 200
    def safe_print(*args, **kwargs):
        nonlocal line_counter
        if line_counter >= max_lines:
            clear_output(wait=True)
            line_counter = 0
        print(*args, **kwargs)
        line_counter += 1

    try:
        safe_print("\n[PROCESS START ▶️] Iniciando recolección de archivos.", flush=True)
        start_time = time()

        # Validación parámetros
        video_root_path: Optional[str] = config.get("video_files_root_path")
        if not video_root_path:
            raise ValueError("Falta 'video_files_root_path' en config.")
        video_folders = config.get("video_files_target_search_folder", [])
        video_exts = config.get("video_files_target_search_extension", [])
        if not video_exts:
            raise ValueError("Falta 'video_files_target_search_extension' o está vacío.")
        env_ident: Optional[str] = config.get("ini_environment_identificated")
        if not env_ident:
            raise ValueError("Falta 'ini_environment_identificated' en config.")
        if env_ident == "LOCAL":
            keyfile = config.get("json_keyfile_local")
        elif env_ident == "COLAB":
            keyfile = config.get("json_keyfile_colab")
        else:
            keyfile = config.get("json_keyfile_GCP_secret_id")
        if not keyfile:
            raise ValueError("Falta clave de credenciales para el entorno.")
        safe_print("[VALIDATION SUCCESS ✅] Parámetros validados.", flush=True)

        # Ruta Google Drive no soportada
        if video_root_path.startswith("https://"):
            safe_print("[WARNING ⚠️] Google Drive no implementado.", flush=True)
            raise NotImplementedError("Extracción desde Google Drive no está implementada.")

        # Búsqueda de archivos
        def _find_files(root, folders, exts):
            results = []
            found = 0
            for cur_root, dirs, files in os.walk(root):
                base = os.path.basename(cur_root)
                if folders and base not in folders:
                    continue
                for f in files:
                    _, ext = os.path.splitext(f)
                    if ext.lower() in (e.lower() for e in exts):
                        path = os.path.join(cur_root, f)
                        results.append({"video_file_path": path, "video_file_name": f})
                        found += 1
                        safe_print(f"[FOUND] {found}: {f}", flush=True)
            if not results:
                return pd.DataFrame()
            return pd.DataFrame(results)

        safe_print(f"[SEARCH ▶️] Ruta: {video_root_path} | Exts: {video_exts}", flush=True)
        df_paths = _find_files(video_root_path, video_folders, video_exts)
        if df_paths.empty:
            raise ValueError("No se encontraron archivos.")
        safe_print(f"[FOUND ✅] {len(df_paths)} archivos.", flush=True)

        # Extracción metadatos
        total = len(df_paths)
        rows = []
        safe_print("[EXTRACT ▶️] Extrayendo metadatos.", flush=True)
        for idx, path in enumerate(df_paths['video_file_path'], 1):
            meta = {}
            try:
                size_mb = os.path.getsize(path) // (1024*1024)
                proc = subprocess.run(
                    ['ffprobe','-v','error','-print_format','json','-show_streams','-show_format', path],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
                info = json.loads(proc.stdout)
                # valores por defecto
                meta = {
                    'file_name': os.path.basename(path),
                    'file_path': path,
                    'file_creation_date': datetime.fromtimestamp(os.path.getctime(path)),
                    'file_last_modified_date': datetime.fromtimestamp(os.path.getmtime(path)),
                    'file_scrap_date': datetime.now(),
                    'file_size_mb': size_mb,
                    'duration_hms':'00:00:00','duration_ms':0,
                    'video_codec':None,'video_bitrate_kbps':0,'video_fps':0,'video_resolution':'unknown',
                    'audio_codec':None,'audio_bitrate_kbps':0,'audio_channels':0,'audio_sample_rate_hz':0
                }
                for st in info.get('streams',[]):
                    if st.get('codec_type')=='video':
                        meta['video_codec']=st.get('codec_name')
                        meta['video_bitrate_kbps']=int(st.get('bit_rate',0))//1000
                        w,h=st.get('width'),st.get('height')
                        meta['video_resolution']=f"{w}x{h}" if w and h else 'unknown'
                        n,d=map(int,st.get('r_frame_rate','0/1').split('/'))
                        meta['video_fps']=n/d if d else 0
                    elif st.get('codec_type')=='audio':
                        meta['audio_codec']=st.get('codec_name')
                        meta['audio_bitrate_kbps']=int(st.get('bit_rate',0))//1000
                        meta['audio_channels']=st.get('channels',0)
                        meta['audio_sample_rate_hz']=int(st.get('sample_rate',0))
                fmt = info.get('format',{})
                dur = float(fmt.get('duration', 0))
                meta['duration_ms']=int(dur*1000)
                h=int(dur)//3600; m=(int(dur)%3600)//60; s=int(dur)%60
                meta['duration_hms']=f"{h:02d}:{m:02d}:{s:02d}"
                safe_print(f"[META] {meta['file_name']} | {meta['duration_hms']} | {meta['video_resolution']}", flush=True)
            except Exception as e:
                safe_print(f"[ERROR] {os.path.basename(path)}: {e}", flush=True)
            rows.append(meta)
            # progreso
            pct = idx/total*100
            bar = '['+'='*int(pct//5)+' '*(20-int(pct//5))+']'
            safe_print(f"[PROG] {bar} {pct:.1f}% ({idx}/{total})", flush=True)

        safe_print("[EXTRACT ✅] Metadatos extraídos.", flush=True)
        df = pd.DataFrame(rows)[[
            'file_name','file_path','file_creation_date','file_last_modified_date','file_scrap_date',
            'file_size_mb','duration_hms','duration_ms','video_codec','video_bitrate_kbps',
            'video_fps','video_resolution','audio_codec','audio_bitrate_kbps',
            'audio_channels','audio_sample_rate_hz'
        ]]
        for c in ['file_creation_date','file_last_modified_date','file_scrap_date']:
            df[c]=pd.to_datetime(df[c], errors='coerce')
        df.to_csv('video_files_backup.csv', index=False)
        safe_print(f"[BACKUP] Guardado en video_files_backup.csv", flush=True)
        elapsed = time() - start_time
        safe_print(f"[END ✅] {len(df)} videos en {elapsed:.2f}s", flush=True)
        return df
    except ValueError as ve:
        safe_print(f"[FAIL] {ve}", flush=True)
        return None
    except Exception as e:
        safe_print(f"[FAIL] Inesperado: {e}", flush=True)
        return None
