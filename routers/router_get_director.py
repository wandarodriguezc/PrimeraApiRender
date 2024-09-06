from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseDirector, BaseRequieredMovies

get_director = pl.read_parquet("data/get_director.parquet")
directores = pl.read_parquet("data/directores.parquet")

get_director = get_director.with_columns(
    pl.col("release_date").cast(pl.String).alias("release_date")
)


router = APIRouter(
    prefix="/get_director",
    tags=["get_director"],
    responses={404: {"message": "No encontrado;"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def cantidad_filmaciones_mes(nombre_director: str):
    try:
        # Obteniendo la lista de id de peliculas de la tabla directores
        mask = directores["director"] == nombre_director.upper()
        movies_list = directores.filter(mask)["movies"].to_list()[0]

        # Filtrando la tabla get_director y obteniendo la informacion
        mask = get_director["id"].is_in(movies_list)
        requiered_movies = get_director.filter(mask).to_dicts()
        total_return = get_director.filter(mask)["returned"].sum()

    except IndexError:
        raise HTTPException(status_code=404, detail="INCORRECTO")

    return BaseDirector(
        director=nombre_director.upper(),
        total_return=total_return,
        requiered_movies=requiered_movies,
    )
