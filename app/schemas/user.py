from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class UserBase(BaseModel):
    """
    Schema for reading user data.
    """
    id: int
    fullname: str 
    email: str
    is_guest: int
    registered_at: datetime
    created_at: datetime

    class Config:
        from_attributes = True  # Enable ORM mode to automatically map to SQLAlchemy model


class TokenBase(BaseModel):
    """
    Schema for responding with a token data
    """
    access_token: str
    token_type: str
    success: bool


class TokenData(BaseModel):
    """
    """
    id: Optional[int] = None
