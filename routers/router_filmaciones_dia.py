from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseFilmacionesDia

filmaciones_dia = pl.read_parquet("data/cantidad_filmaciones_dia.parquet")

router = APIRouter(
    prefix="/filmaciones_dia",
    tags=["filmaciones_dia"],
    responses={404: {"message": "No encontrado;"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def cantidad_filmaciones_mes(dia: str):
    mask = filmaciones_dia["day"] == dia.upper()
    try:
        number_movies = filmaciones_dia.filter(mask)["cantidad_peliculas"][0]
    except IndexError:
        raise HTTPException(status_code=404, detail="INCORRECTO")

    message = f"{number_movies} películas fueron estrenadas el dia  {dia.upper()}"
    return BaseFilmacionesDia(
        day=dia.upper(), number_movies=number_movies, message=message
    )
