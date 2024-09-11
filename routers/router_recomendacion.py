from fastapi import APIRouter, status, HTTPException
import polars as pl
import joblib
from models.base_models import BaseRecomendaciones

movies = pl.read_parquet("data/movies_model_polars.parquet")
model = joblib.load('data/top_similarities.pkl')

router = APIRouter(
    prefix="/recomendacion",
    tags=["recomendacion"],
    responses={404: {"message": "NOT FOUND"}},
)


@router.get("/", status_code=status.HTTP_200_OK)
async def recomendacion(titulo: str) -> BaseRecomendaciones:
    """recomendacion: Funcion del tipo GET en el endpoint: '/recomendacion'

    Args:
        titulo (str): Cadena de String recibida por la api que representa 
                      el titulo a consultar

    Planteamineto: Se ingresa el nombre de una película y te recomienda las 
                   similares en una lista de 5 valores.

    Raises:
        HTTPException: 404 NOT FOUND
                       Dado que el Valor recibido por la Api puede contener algun titulo
                       de pelicula (valido o no valido) pero que no esté en la Data que la
                       api maneja, de no encontrar un match simplemente NOT FOUND

    Returns:
        BaseRecomendaciones: class BaseRecomendaciones(BaseModel):
                                 recomendaciones: list[BaseRecomendacion]

                        DONDE:
                             class BaseRecomendacion(BaseModel):
                                num: int
                                title: str   
    """
    try:
        # filtro por el string que recibe la api
        mask = movies['title'] == titulo.strip().upper()
        # ubico dentro del parqut el indice
        index = movies.filter(mask)['index'][0]
        # Con ayuda del modelo `model` se ubican las 5 Mejores coincidencias 
        best_scores = model[index]  #Recibe 5 tuplas en formato (12549, 0.9999982855818694) 
        movie_indices = [i[0] for i in best_scores]

        top_5 = []
        for i in range(len(movie_indices)):
            top_5_dict = {}
            mask = movies['index'] == movie_indices[i]
            top_5_dict['num'] = i+1
            top_5_dict['title'] = movies.filter(mask)['title'][0]
            top_5.append(top_5_dict)
    except IndexError:
        # Si entro en este except es porque el `titulo` recibido está mal escrito
        raise HTTPException(status_code=404, detail="NOT FOUND")

    return BaseRecomendaciones(recomendaciones=top_5)
