import os
import logging


def limpiar_terminal():
    """
        limpiar_terminal: como su nombre lo indica, limpia la terminal
    """
    os.system('cls' if os.name == 'nt' else 'clear')


def borrar_archivos(archivos: list, carpeta: str, logger: logging) -> None:
    """_summary_

    Args:
        archivos (list): Lista de archivos a borra
        carpeta (str): Carpeta donde se encuentran ubicados
        logger (logging): Logger de la app para seguimiento
    """
    for archivo in archivos:
        ruta = os.path.join(carpeta, archivo)
        try:
            os.remove(ruta)
            logger.info(f"Archivo de log {ruta} eliminado")
        except TimeoutError:
            logger.error(f"Ocurrio error tratando de borrar {ruta}")
