from fastapi import FastAPI

from routers import router_filmaciones_mes
from routers import router_filmaciones_dia
from routers import router_votos_titulo
from routers import router_score_titulo
from routers import router_get_actor
from routers import router_get_director
from routers import router_recomendacion

app = FastAPI()

# ROUTERS:
app.include_router(router_filmaciones_mes.router)
app.include_router(router_filmaciones_dia.router)
app.include_router(router_score_titulo.router)
app.include_router(router_votos_titulo.router)
app.include_router(router_get_actor.router)
app.include_router(router_get_director.router)
app.include_router(router_recomendacion.router)


@app.get("/")
async def read_root():
    return {
        "Message": "Proyecto Labs-1 Aliskair Rodríguez Data PT-10 ",
        "Documentacion": "/docs",
    }
