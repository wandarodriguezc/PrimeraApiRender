from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseScoreTitulo

titulos = pl.read_parquet("data/api_03_04_titulos.parquet")

router = APIRouter(
    prefix="/score_titulo",
    tags=["score_titulo"],
    responses={404: {"message": "NOT FOUND"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def score_titulo(titulo_filmacion: str) -> BaseScoreTitulo:
    """votos_titulo: Funcion del tipo GET en el endpoint: '/votos_titulo'

    Planteamiento: Se ingresa el título de una filmación esperando como respuesta
                   el título, el año de estreno y el score.
                   Ejemplo de retorno: La película X fue estrenada en el año X con
                                       un score/popularidad de X
    A tomar en Cuenta:
                   La consigna solicitada en el ejemplo es cubierta con el campo `message` de la
                   Clase `BaseScoreTitulo` se anexan los campos `title`, `year` y `popularity` por
                   separado tambien para cubrir el escenario de que la api sea consumida por otra
                   app y de esta manera se le facilite la lectura de los datos

    Args:
        titulo_filmacion (str): String recibido en la peticion que representa el
                                titulo de la pelicula a consultar

    Raises:
        HTTPException: 404 NOT FOUND
                       Dado que el Valor recibido por la Api puede contener algun titulo
                       de pelicula (valido o no valido) pero que no esté en la Data que la
                       api maneja, de no encontrar un match simplemente NOT FOUND

    Returns:
        BaseScoreTitulo: class BaseScoreTitulo(BaseModel):
                            title: str
                            year: int
                            popularity: float
                            message: str
    """
    # data filtrada comparando el campo `title` con el recibido elevado a MAYUSCULAS
    mask = titulos["title"] == titulo_filmacion.strip().upper()
    try:
        popularity = titulos.filter(mask)["popularity"][0]
        year = titulos.filter(mask)["release_year"][0]
        message = f"La Película {titulo_filmacion} fue estrenada en el año {year} cuenta con un score/popularidad de {popularity}"
    except IndexError:
        raise HTTPException(status_code=404, detail="NOT FOUND")

    return BaseScoreTitulo(
        title=titulo_filmacion,
        year=year,
        popularity=popularity,
        message=message,
    )
