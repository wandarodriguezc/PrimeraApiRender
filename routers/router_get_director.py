from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseDirector

get_directores = pl.read_parquet("data/api_06_get_director.parquet")
directores = pl.read_parquet("data/directores.parquet")

# Este Dato viene con un Casteo tipo date. Tengo que pasarlo a String para evitar problemas con BaseModel
get_directores = get_directores.with_columns(
    pl.col("release_date").cast(pl.String).alias("release_date")
)


router = APIRouter(
    prefix="/get_director",
    tags=["get_director"],
    responses={404: {"message": "NOT FOUND"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def get_director(nombre_director: str) -> BaseDirector:
    """get_director: Funcion del tipo GET en el endpoint: '/get_director'

    Planteamiento: Se ingresa el nombre de un director que se encuentre dentro de un
                   dataset debiendo devolver el éxito del mismo medido a través del
                   retorno. Además, deberá devolver el nombre de cada película con la
                   fecha de lanzamiento, retorno individual, costo y ganancia de la misma.
    A tomar en Cuenta:
                   La clase BaseDirector tiene en su  estructura 2 campos para cubrir el
                   nombre del director y el total retornado.  Al no poder saber de anticipado
                   en cuantas Películas ha participado el Director consultado, el Tercer campo
                   de BaseDirector es una Lista.  Al ser esta lista una data que será usada dentro
                   de una respuesta a una solicitud, lo correcto es que los datos dentro de esa
                   lista tambien pertenezcan a alguna Clase que herede de BaseModel en este caso
                   de la clase BaseRequieredMovies, la cual transformará los datos de cada película
                   en la que participó el director a una estructura adecuada de transferencia de
                   informacion

    Args:
        nombre_director (str): String recibido por la api que representa el nombre del
                               director a consultar

    Raises:
        HTTPException: 404 NOT FOUND
                       Dado que el Valor recibido por la Api puede contener algun nombre
                       de persona (valido o no valido) pero que no esté en la Data que la
                       api maneja, de no encontrar un match simplemente NOT FOUND

    Returns:
        BaseDirector: class BaseDirector(BaseModel):
                         director: str
                         total_return: float
                         requiered_movies: list[BaseRequieredMovies]
                DONDE:
                     class BaseRequieredMovies(BaseModel):
                        id: int
                        title: str
                        release_date: str
                        revenue: float
                        budget: float
                        returned: float
    """
    try:
        # Obteniendo la lista de id de peliculas de la tabla directores
        mask = directores["director"] == nombre_director.upper()
        movies_list = directores.filter(mask)["movies"].to_list()[0]

        # Filtrando la tabla get_director y obteniendo la informacion
        mask = get_directores["id"].is_in(movies_list)
        requiered_movies = get_directores.filter(mask).to_dicts()
        total_return = get_directores.filter(mask)["returned"].sum()

    except IndexError:
        # Si entro aca es porque no hubo match entre lo recibido por el endpoint y los Directores de la data
        raise HTTPException(status_code=404, detail="NOT FOUND")

    return BaseDirector(
        director=nombre_director.upper(),
        total_return=total_return,
        requiered_movies=requiered_movies,
    )
