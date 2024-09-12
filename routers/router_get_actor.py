from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseActor

get_actores = pl.read_parquet("data/api_05_get_actor.parquet")

router = APIRouter(
    prefix="/get_actor",
    tags=["get_actor"],
    responses={404: {"message": "NOT FOUND;"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def get_actor(nombre_actor: str) -> BaseActor:
    """get_actor: Funcion del tipo GET en el endpoint: '/get_actor'

    Planteamiento: Se ingresa el nombre de un actor que se encuentre dentro de un dataset
                   debiendo devolver el éxito del mismo medido a través del retorno. Además,
                   la cantidad de películas que en las que ha participado y el promedio de
                   retorno. La definición no deberá considerar directores.
                   Ejemplo de retorno: El actor X ha participado de X cantidad de filmaciones,
                   el mismo ha conseguido un retorno de X con un promedio de X por filmación
    A tomar en Cuenta:
                   La consigna solicitada en el ejemplo es cubierta con el campo `message` de la
                   Clase `BaseActor` se anexan los campos `actor`, `number_movies`, `total_return`
                   y `average_return` por separado tambien para cubrir el escenario de que la api
                   sea consumida por otra app y de esta manera se le facilite la lectura de los datos

    Args:
        nombre_actor (str): String recibido por la api que representa el nombre del actor
                            a consultar

    Raises:
        HTTPException: 404 NOT FOUND
                       Dado que el Valor recibido por la Api puede contener algun nombre
                       de persona (valido o no valido) pero que no esté en la Data que la
                       api maneja, de no encontrar un match simplemente NOT FOUND

    Returns:
        BaseActor: class BaseActor(BaseModel):
                        actor: str
                        number_movies: int
                        total_return: float
                        average_return: float
                        message: str
    """

    mask = get_actores["actor"] == nombre_actor.strip().upper()
    try:
        number_movies = get_actores.filter(mask)["number_of_movies"][0]
        total_return = get_actores.filter(mask)["total_return"][0]
        average_return = get_actores.filter(mask)["average_return"][0]
        message_1 = f"El actor {nombre_actor} ha participado de {number_movies} cantidad de filmaciones, el mismo"
        message_2 = f" ha conseguido un retorno de {total_return} con un promedio de {average_return} por filmación"
        message = message_1 + message_2
    except IndexError:
        # Si entro aca es porque no hubo match entre lo recibido por el endpoint y los Actores de la data
        raise HTTPException(status_code=404, detail="NOT FOUND")

    return BaseActor(
        actor=nombre_actor.upper(),
        number_movies=number_movies,
        total_return=total_return,
        average_return=average_return,
        message=message,
    )
