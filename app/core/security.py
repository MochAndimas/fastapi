from fastapi import Request, HTTPException, status
from datetime import datetime, timedelta
from typing import Any
from jose import jwt
from passlib.context import CryptContext
from app.schemas.user import TokenData
from app.core.config import settings
from jose import jwt, JWTError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.user import UserToken

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain password against a hashed password."""
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(
    subject: str | Any, 
    expires_delta: timedelta = timedelta(
        minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
) -> str:
    """Create an access token with the given subject (usually the user ID)."""
    expire = datetime.now() + expires_delta
    to_encode = {
        "exp": expire.timestamp(), 
        "sub": str(subject), 
        "type": "access"}
    encoded_jwt = jwt.encode(
        to_encode, 
        settings.JWT_SECRET_KEY, 
        algorithm=settings.ALGORITHM)
    return encoded_jwt


def create_refresh_token(
    subject: str | Any, 
    expires_delta: timedelta = timedelta(
        days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
) -> str:
    """Create a refresh token with the given subject (usually the user ID)."""
    expire = datetime.now() + expires_delta
    to_encode = {
        "exp": expire.timestamp(), 
        "sub": str(subject), 
        "type": "refresh"}
    encoded_jwt = jwt.encode(
        to_encode, 
        settings.JWT_SECRET_KEY,
        algorithm=settings.ALGORITHM)
    return encoded_jwt


async def refresh_access_token(
        sqlite_session: AsyncSession, 
        refresh_token: str):
    """Decodes, validates the refresh token, and issues a new access token."""
    # decode the refresh token, validate it, and issue a new access token.
    payload = jwt.decode(
        refresh_token, 
        settings.JWT_SECRET_KEY, 
        algorithms=[settings.ALGORITHM])
    user_id = payload.get("sub")
    query_personal_token = select(UserToken).filter_by(user_id=user_id)
    personal_token_data = await sqlite_session.execute(query_personal_token)
    personal_token = personal_token_data.scalars().first()

    if not personal_token or payload.get("type") != "refresh":
        if personal_token.is_revoked:
            raise JWTError("Invalid refresh token")
    
    new_access_token = create_access_token(subject=user_id)
    personal_token.access_token = new_access_token
    personal_token.updated_at = datetime.now()
    await sqlite_session.commit()
    await sqlite_session.close()

    return new_access_token


async def verify_access_token(
        sqlite_session: AsyncSession, 
        token: str):
    """Decodes and validates the access token, returning the user ID if valid."""
    payload = jwt.decode(
        token, 
        settings.JWT_SECRET_KEY, 
        algorithms=[settings.ALGORITHM])
    id: str = payload.get("sub")
    query_personal_token = select(UserToken).filter_by(user_id=id)
    personal_token_data = await sqlite_session.execute(query_personal_token)
    personal_token = personal_token_data.scalars().first()
    
    if not personal_token or payload.get("type") != "access":  
        if personal_token.is_revoked:    
            raise JWTError("Invalid access token")
    
    return TokenData(id=personal_token.user_id)


async def verify_csrf_token(request: Request):
    """Validating the CSRF Token"""
    csrf_token_from_request = request.cookies.get("csrf_token")
    csrf_token_from_session = request.session.get("csrf_token") 
    
    if not csrf_token_from_request\
        or csrf_token_from_request\
            != csrf_token_from_session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid CSRF Token!"
        )
    return csrf_token_from_request

