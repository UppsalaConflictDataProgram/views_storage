from typing import List
from pydantic import BaseModel


class Listing(BaseModel):
    folders: List[str]
    files: List[str]
