from pydantic import BaseModel, validator
from pydantic.typing import Optional

attr_list = {"_id", "id", "keywords", "adult", "budget", "genres", "popularity", "title"}
frg

class Movie(BaseModel):
    title: Optional[str]
    genre: Optional[str]
    is_adult: Optional[bool]
    min_popularity: Optional[float]
    min_budget: Optional[int]

    @validator('min_budget')
    def split_str(cls, v):
        if v > 1000000000:
            raise ValueError('budget must be less than 1000000000')
        return v

    @validator('min_popularity')
    def split_str(cls, v):
        if v > 50:
            raise ValueError('popularity must be less than 70')
        return v
