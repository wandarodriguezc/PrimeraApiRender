from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseFilmacionesDia

filmaciones_dia = pl.read_parquet("data/api_02_cantidad_filmaciones_dia.parquet")


router = APIRouter(
    prefix="/filmaciones_dia",
    tags=["filmaciones_dia"],
    responses={400: {"message": "BAD REQUEST"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def cantidad_filmaciones_dia(dia: str) -> BaseFilmacionesDia:
    """cantidad_filmaciones_dia : Funcion del tipo GET en el endpoint: '/filmaciones_dia'

    Planteamiento: Se ingresa un día en idioma Español. Debe devolver la cantidad de
                   películas que fueron estrenadas en día consultado en la totalidad del dataset.
                   Ejemplo de retorno: X cantidad de películas fueron estrenadas en los días X
    A tomar en Cuenta:
                   La consigna solicitada en el ejemplo es cubierta con el campo `message` de la
                   Clase `BaseFilmacionesDia` se anexan los campos `day` y `number_movies` por
                   separado tambien para cubrir el escenario de que la api sea consumida por otra
                   app y de esta manera se le facilite la lectura de los datos


    Args:
        dia (str): representa el dia en español a consultar

    Raises:
        HTTPException: 400 BAD REQUEST
                       Dado que en la Data los dias estan completos y en Mayusculas y a la
                       hora de comparar contra el valor introducido se aplica la funcion
                       `upper()` la única opción de que se active la excepcion es porque el
                       valor recibido en la peticion tenga un error de escritura

    Returns:
        BaseFilmacionesDia: class BaseFilmacionesDia(BaseModel):
                                day: str
                                number_movies: int
                                message: str
    """
    # filtro la data comparando contra el string recibido elevado a MAYUSCULAS
    mask = filmaciones_dia["day"] == dia.upper()
    try:
        number_movies = filmaciones_dia.filter(mask)["cantidad_peliculas"][0]
    except IndexError:
        # Si entro en este except es porque el `dia` recibido está mal escrito
        raise HTTPException(status_code=400, detail="BAD REQUEST")

    message = f"{number_movies} cantidad de películas fueron estrenadas en los dias  {dia.upper()}"

    return BaseFilmacionesDia(
        day=dia.upper(), number_movies=number_movies, message=message
    )
