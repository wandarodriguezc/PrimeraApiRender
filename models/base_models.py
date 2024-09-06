from pydantic import BaseModel


class BaseFilmacionesMes(BaseModel):
    month: str
    number_movies: int
    message: str


class BaseFilmacionesDia(BaseModel):
    day: str
    number_movies: int
    message: str


class BaseScoreTitulo(BaseModel):
    title: str | None = None
    year: int | None = None
    vote_count: int | None = None
    vote_average: float | None = None
    message: str


class BaseActor(BaseModel):
    actor: str
    number_movies: int
    total_return: float
    average_return: float


class BaseRequieredMovies(BaseModel):
    id: int
    title: str
    release_date: str
    revenue: float
    budget: float
    returned: float


class BaseDirector(BaseModel):
    director: str
    total_return: float
    requiered_movies: list[BaseRequieredMovies]
