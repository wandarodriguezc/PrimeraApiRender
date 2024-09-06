from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseFilmacionesMes

# from schemas.schemas import schema_filmaciones_mes

filmaciones_mes = pl.read_parquet("data/cantidad_filmaciones_mes.parquet")

schema: dict

router = APIRouter(
    prefix="/filmaciones_mes",
    tags=["filmaciones_mes"],
    responses={404: {"message": "No encontrado;"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def cantidad_filmaciones_mes(mes: str):
    mask = filmaciones_mes["month"] == mes.upper()
    try:
        number_movies = filmaciones_mes.filter(mask)["cantidad_peliculas"][0]
    except IndexError:
        raise HTTPException(status_code=404, detail="INCORRECTO")

    message = f"{number_movies} películas fueron estrenadas el mes de {mes.upper()}"
    return BaseFilmacionesMes(
        month=mes.upper(), number_movies=number_movies, message=message
    )
