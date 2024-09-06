def schema_filmaciones_mes(filmaciones_mes: tuple) -> dict:

    return {
        "month": filmaciones_mes[0],
        "number_movies": filmaciones_mes[1],
        "message": filmaciones_mes[2],
    }
