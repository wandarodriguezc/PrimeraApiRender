from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseVotosTitulo

score_titulo = pl.read_parquet("data/api_03_04_titulos.parquet")

router = APIRouter(
    prefix="/votos_titulo",
    tags=["votos_titulo"],
    responses={404: {"message": "NOT FOUND"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def votos_titulos(titulo_filmacion: str) -> BaseVotosTitulo:
    """votos_titulo: Funcion del tipo GET en el endpoint: '/votos_titulo'

    Planteamiento: Se ingresa el título de una filmación esperando como respuesta el título,
                   la cantidad de votos y el valor promedio de las votaciones. La misma variable
                   deberá de contar con al menos 2000 valoraciones, caso contrario, debemos contar
                   con un mensaje avisando que no cumple esta condición y que por ende, no se
                   devuelve ningun valor.
                   Ejemplo de retorno: La película X fue estrenada en el año X. La misma cuenta con
                                       un total de X valoraciones, con un promedio de X
    A tomar en Cuenta:
                   La consigna solicitada en el ejemplo es cubierta con el campo `message` de la
                   Clase `BaseVotosTitulo` se anexan los campos `title`, `year`, `vote_count` y
                   `vote_average` por separado tambien para cubrir el escenario de que la api sea
                    consumida por otra app y de esta manera se le facilite la lectura de los datos
                    Los Campos individuales son Opcionales con un valor por defecto de `None` dentro
                    de la Clase BaseVotosTitulos, ya que están condicionados a que las votaciones de
                    la pelicula consultada supere las 2000 valoraciones

    Args:
        titulo_filmacion (str): String recibido en la peticion que representa el
                                titulo de la pelicula a consultar

    Raises:
        HTTPException: 404 NOT FOUND
                       Dado que el Valor recibido por la Api puede contener algun titulo
                       de pelicula (valido o no valido) pero que no esté en la Data que la
                       api maneja, de no encontrar un match simplemente NOT FOUND

    Returns:
        BaseVotosTitulo: class BaseVotosTitulo(BaseModel):
                            title: str | None = None
                            year: int | None = None
                            vote_count: int | None = None
                            vote_average: float | None = None
                            message: str
    """
    # data filtrada comparando el campo `title` con el recibido elevado a MAYUSCULAS
    mask = score_titulo["title"] == titulo_filmacion.strip().upper()
    try:
        vote_count = score_titulo.filter(mask)["vote_count"][0]
        vote_average = score_titulo.filter(mask)["vote_average"][0]
        year = score_titulo.filter(mask)["release_year"][0]
    except IndexError:
        raise HTTPException(status_code=404, detail="NOT FOUND")

    if vote_count >= 2000:
        message_1 = f"La Película {titulo_filmacion} fue estrenada en el año {year}. La misma cuenta con un total "
        message_2 = f"de {vote_count} valoraciones, con un promedio de {vote_average}"
        message = message_1 + message_2
    else:
        # Si las valoraciones no alcanzan las 2000, devuelve el mensaje y se hace None las variables individuales
        message = f"La Película {titulo_filmacion} no cuenta con 2000 valoraciones por ende no se muestra data"
        vote_count = None
        vote_average = None
        year = None

    return BaseVotosTitulo(
        title=titulo_filmacion,
        year=year,
        vote_count=vote_count,
        vote_average=vote_average,
        message=message,
    )
