from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseFilmacionesMes

filmaciones_mes = pl.read_parquet("data/api_01_cantidad_filmaciones_mes.parquet")

schema: dict

router = APIRouter(
    prefix="/filmaciones_mes",
    tags=["filmaciones_mes"],
    responses={400: {"message": "BAD REQUEST"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def cantidad_filmaciones_mes(mes: str) -> BaseFilmacionesMes:
    """cantidad_filmaciones_mes : Funcion del tipo GET en el endpoint: '/filmaciones_mes'

    Planteamiento: Se ingresa un mes en idioma Español. Debe devolver la cantidad de películas
                   que fueron estrenadas en el mes consultado en la totalidad del dataset.
                   Ejemplo de retorno: X cantidad de películas fueron estrenadas en el mes de X
    A tomar en Cuenta:
                   La consigna solicitada en el ejemplo es cubierta con el campo `message` de la
                   Clase `BaseFilmacionesMes` se anexan los campos `month` y `number_movies` por
                   separado tambien para cubrir el escenario de que la api sea consumida por otra
                   app y de esta manera se le facilite la lectura de los datos

    Args:
        mes (str): representa el mes en español a consultar

    Raises:
        HTTPException: 400 BAD REQUEST
                       Dado que en la Data los meses estan completos y en Mayusculas y a la
                       hora de comparar contra el valor introducido se aplica la funcion
                       `upper()` la única opción de que se active la excepcion es porque el
                       valor recibido en la peticion tenga un error de escritura

    Returns:
        BaseFilmacionesMes: class BaseFilmacionesMes(BaseModel):
                                month: str
                                number_movies: int
                                message: str
    """
    # filtro la data comparando contra el string recibido elevado a MAYUSCULAS
    mask = filmaciones_mes["month"] == mes.strip().upper()
    try:
        number_movies = filmaciones_mes.filter(mask)["cantidad_peliculas"][0]
    except IndexError:
        # Si entro en este except es porque el `mes` recibido está mal escrito
        raise HTTPException(status_code=400, detail="BAD REQUEST")

    message = f"{number_movies} cantidad de películas fueron estrenadas en el mes de {mes.upper()}"
    return BaseFilmacionesMes(
        month=mes.upper(), number_movies=number_movies, message=message
    )
