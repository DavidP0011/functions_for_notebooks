from typing import Dict


# @title pdf_merge_intercalated_pages_file()
def pdf_merge_intercalated_pages_file(config: dict) -> None:
    """
    Intercala páginas de dos archivos PDF (impares y pares) y genera un archivo PDF combinado.
    Además, reporta métricas del proceso al finalizar.

    Args:
        config (dict): Diccionario de configuración con los siguientes parámetros:
            - impares_path (str): Ruta del archivo PDF con páginas impares.
            - pares_path (str): Ruta del archivo PDF con páginas pares.
            - output_path (str): Ruta de salida para el archivo PDF combinado.

    Returns:
        None

    Raises:
        ValueError: Si falta algún parámetro obligatorio o si los PDFs no tienen la misma cantidad de páginas.
        Exception: Si ocurre un error al escribir el archivo PDF resultante.
    """
    from PyPDF2 import PdfReader, PdfWriter
    import time

    start_time_float = time.time()

    # Validación de parámetros
    impares_path_str = config.get("impares_path")
    pares_path_str = config.get("pares_path")
    output_path_str = config.get("output_path")

    if not impares_path_str:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta 'impares_path' en config.")
    if not pares_path_str:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta 'pares_path' en config.")
    if not output_path_str:
        raise ValueError("[VALIDATION [ERROR ❌]] Falta 'output_path' en config.")

    print("🔹🔹🔹 [START ▶️] Proceso de intercalado de PDFs 🔹🔹🔹", flush=True)

    # Cargar los PDFs
    try:
        reader_impares = PdfReader(impares_path_str)
        reader_pares = PdfReader(pares_path_str)
        print("[FILE LOAD SUCCESS ✅] Archivos PDF cargados correctamente.", flush=True)
    except Exception as e:
        raise ValueError(f"[FILE LOAD ERROR ❌] Error al cargar los archivos PDF: {e}")

    # Verificar que ambos PDFs tengan la misma cantidad de páginas
    num_impares_int = len(reader_impares.pages)
    num_pares_int = len(reader_pares.pages)

    if num_impares_int != num_pares_int:
        raise ValueError("[VALIDATION [ERROR ❌]] El número de páginas en los archivos no coincide.")

    # Revertir las páginas del PDF de pares
    pares_paginas_list = list(reader_pares.pages)[::-1]

    # Crear el PDF combinado
    writer_pdf = PdfWriter()

    print(f"[PROCESSING 🔄] Intercalando {num_impares_int} páginas de cada archivo.", flush=True)
    for idx in range(num_impares_int):
        writer_pdf.add_page(reader_impares.pages[idx])
        writer_pdf.add_page(pares_paginas_list[idx])

    # Guardar el archivo resultante
    try:
        with open(output_path_str, "wb") as output_file:
            writer_pdf.write(output_file)
        print(f"[FILE WRITE SUCCESS ✅] PDF intercalado guardado en: {output_path_str}", flush=True)
    except Exception as e:
        raise Exception(f"[FILE WRITE ERROR ❌] Error al guardar el archivo PDF: {e}")

    elapsed_time_sec_float = time.time() - start_time_float

    # 🔹🔹🔹 MÉTRICAS 🔹🔹🔹
    print("\n🔹🔹🔹 [METRICS 📊] Resumen del proceso 🔹🔹🔹", flush=True)
    print("[METRICS INFO ℹ️] Total de páginas por PDF de entrada:", num_impares_int, flush=True)
    print("[METRICS INFO ℹ️] Total de páginas finales combinadas:", num_impares_int * 2, flush=True)
    print("[METRICS INFO ℹ️] Archivo de salida:", output_path_str, flush=True)
    print(f"[METRICS INFO ℹ️] Tiempo total de ejecución: {elapsed_time_sec_float:.2f} segundos", flush=True)

    print("🔹🔹🔹 [PDF MERGE FINISHED ✅] Proceso completado exitosamente. 🔹🔹🔹", flush=True)
