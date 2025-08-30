from fastapi import APIRouter, UploadFile, File, status, HTTPException
import tempfile
import polars as pl
from pathlib import Path
from utils.logger import get_logger
import logging

router = APIRouter(
    prefix="",
    tags=["upload-parquet"],
    responses={400: {"message": "BAD REQUEST"}},
)

logger: logging = get_logger("upload-parquet")
carpeta_db: Path = Path("data")
ruta_db: Path = carpeta_db / "base.parquet"


@router.post("/upload-parquet/")
async def upload_parquet(file: UploadFile = File(...)) -> dict:
    """_summary_
    upload_parquet: función para el endpoint que se encarga de recibir un archivo parquet con la
                    data actualizada de los empleos

    Args:
        file (UploadFile, optional): Archivo recibido desde el exterior

    Returns:
        dict: Filas y columnas de la data recibida en el archivo Parquet
    """
    # Guardar temporalmente el archivo
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            contents = await file.read()
            tmp.write(contents)
            tmp_path = tmp.name
        logger.info("Carga de data exitosa")
        # Leer con Polars
        df = pl.read_parquet(tmp_path)
        df.write_parquet(ruta_db)
        logger.info("Se guardo exitosamente la data")
    except TimeoutError:
        logger.error("No pudo cargar la data")
        raise HTTPException(status_code=400, detail="No pudo cargar la data")
    return {"rows": df.shape[0], "columns": df.shape[1]}
