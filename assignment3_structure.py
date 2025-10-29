from typing import TypedDict, NamedTuple
from dataclasses import dataclass
from pydantic import BaseModel

# TypedDict
class UserTypedDict(TypedDict):
    id: int
    name: str
    email: str
    age: int

# NamedTuple
class UserNamedTuple(NamedTuple):
    id: int
    name: str
    email: str
    age: int

# Dataclass
@dataclass
class UserDataClass:
    id: int
    name: str
    email: str
    age: int

# Pydantic Model
class UserPydantic(BaseModel):
    id: int
    name: str
    email: str
    age: int