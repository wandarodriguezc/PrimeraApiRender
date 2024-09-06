from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseScoreTitulo

score_titulo = pl.read_parquet("data/score_titulo.parquet")

router = APIRouter(
    prefix="/score_titulo",
    tags=["score_titulo"],
    responses={404: {"message": "No encontrado;"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def cantidad_filmaciones_mes(titulo_filmacion: str):

    mask = score_titulo["title"] == titulo_filmacion
    try:
        vote_count = score_titulo.filter(mask)["vote_count"][0]
        vote_average = score_titulo.filter(mask)["vote_average"][0]
        year = score_titulo.filter(mask)["release_year"][0]
    except IndexError:
        raise HTTPException(status_code=404, detail="INCORRECTO")

    if vote_count >= 2000:
        message = f"""La Película {titulo_filmacion} fue estrenada el año {year} cuenta con un total de {vote_count} valoraciones, con un promedio de {vote_average}"""
    else:
        message = f"La Película {titulo_filmacion} no cuenta con 2000 valoraciones por ende no se muestra data"
        vote_count = None
        vote_average = None
        year = None

    return BaseScoreTitulo(
        title=titulo_filmacion,
        year=year,
        vote_count=vote_count,
        vote_average=vote_average,
        message=message,
    )
