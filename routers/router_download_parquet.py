from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
from utils.logger import get_logger
import logging

router = APIRouter(
    prefix="",
    tags=["download-parquet"],
    responses={400: {"message": "BAD REQUEST"}},
)

logger: logging = get_logger("download-parquet")
carpeta_db: Path = Path("data")
ruta_db: Path = carpeta_db / "base.parquet"


@router.get("/download-parquet/")
async def download_parquet() -> FileResponse:
    """_summary_
    download_parquet: función para el endpoint que sirve para que se descargue el archivo parquet con la data

    Raises:
        HTTPException: En caso de que ocurra algún error en Servidor

    Returns:
        FileResponse: Archivo Parquet con la data actualizada de los empleos
    """
    if ruta_db.exists():
        try:
            logger.info("Solicitud de descarga recibida")
            return FileResponse(
                path=str(ruta_db),
                media_type="application/octet-stream",
                filename="db.parquet"
            )
        except TimeoutError:
            logger.error("No logro enviar parquet")
    raise HTTPException(status_code=404, detail="Archivo no encontrado")
