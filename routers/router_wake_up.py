from fastapi import APIRouter, HTTPException
from zoneinfo import ZoneInfo
from datetime import datetime, time, timedelta, date
import os
from utils.logger import get_logger
from utils.utils import borrar_archivos
import logging

router = APIRouter(
    prefix="",
    tags=["wake-up"],
    responses={400: {"message": "BAD REQUEST"}},
)

logger: logging = get_logger("wake-up")
today: date = datetime.now().date()
fecha_obsolecencia: date = today - timedelta(days=15)
hora_local: int = datetime.now(ZoneInfo("America/Caracas")).hour
logs: list = os.listdir("logs")
archivos_borrar: list = []


@router.get("/wake-up/")
async def wake_up() -> dict:
    """_summary_
    wake_up: Función para el endpoint que tiene como finalidad el ser llamada para mantener la Api en
             el servidor activa. Considerando que está desplegada en un servicio gratuito, al no tener actividad
             continua cae en reposo y es posible que falle cuande sea solicitada por la app del dashboard.
             Aparte una(1) vez al dia elimina los logs con N dias de obsolecencia.

    Raises:
        HTTPException: En caso de fallar comunicacion con servidor u otro fallo

    Returns:
        dict: Confirmación de actividad de la API
    """
    try:
        if hora_local == 15:
            for archivo in logs:
                fecha_str = archivo.split("_")[2].replace(".log", "").strip()
                try:
                    if datetime.strptime(fecha_str, "%Y-%m-%d").date() <= fecha_obsolecencia:
                        archivos_borrar.append(archivo)
                except ValueError:
                    logger.error("ERROR 500 el archivo tipo log tiene un nombre no previsto")
                    raise HTTPException(status_code=500, detail="ERROR en servidor")

            if len(archivos_borrar) > 0:
                borrar_archivos(archivos=archivos_borrar, carpeta="logs", logger=logger)
        logger.info("Despertar Ok")
        return {"Wake-Up": True}
    except TimeoutError:
        logger.error("Algo ocurrio durante proceso Wake-Up")
    raise HTTPException(status_code=404, detail="No encontro respuesta")
