from fastapi import APIRouter, status, HTTPException
import polars as pl
from models.base_models import BaseActor

get_actor = pl.read_parquet("data/get_actor.parquet")

router = APIRouter(
    prefix="/get_actor",
    tags=["get_actor"],
    responses={404: {"message": "No encontrado;"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def cantidad_filmaciones_mes(nombre_actor: str):

    mask = get_actor["actor"] == nombre_actor.upper()
    try:
        number_movies = get_actor.filter(mask)["number_of_movies"][0]
        total_return = get_actor.filter(mask)["total_return"][0]
        average_return = get_actor.filter(mask)["average_return"][0]
    except IndexError:
        raise HTTPException(status_code=404, detail="INCORRECTO")

    return BaseActor(
        actor=nombre_actor.upper(),
        number_movies=number_movies,
        total_return=total_return,
        average_return=average_return,
    )
