import secrets
from fastapi import APIRouter, Depends, HTTPException, status, Response, Request
from fastapi.responses import JSONResponse
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils.user_utils import get_current_user, user_token, roles, logout
from app.core.security import verify_password, create_access_token, create_refresh_token, verify_csrf_token
from app.db.session import get_db, get_sqlite

from app.db.models.user import GooddreamerUserData
from app.schemas.user import TokenBase


router = APIRouter()

@router.post("/api/login/csrf-token")
async def get_csrf_token(
    response: Response, 
    request: Request,
    session: AsyncSession = Depends(get_db),
    creds: OAuth2PasswordRequestForm = Depends()):
    """
    This endpoint initializes the CSRF token by setting it as an HTTP-only cookie.
    """
    # Retrieve user from the database by email
    result = await session.execute(select(GooddreamerUserData).where(GooddreamerUserData.email == creds.username))
    user = result.scalar()
    user_role = roles(creds.username)
    
    if not user or not user_role or not verify_password(creds.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password!",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if "csrf_token" not in request.session:
        csrf_token = secrets.token_hex(16)
        request.session["csrf_token"] = csrf_token
    else:
        csrf_token = request.session["csrf_token"]

    # Set CSRF token as an HTTP-only cookie
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=True,
        secure=True  # Set to True in production
    )
    return {"message": "CSRF token initialized."}

@router.post("/api/login", response_model=TokenBase)
async def login_user(
    creds: OAuth2PasswordRequestForm = Depends(), 
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    csrf_token : str = Depends(verify_csrf_token)):
    """
    Authenticate a user and return an access token.
    """
    # Retrieve user from the database by email
    result = await session.execute(select(GooddreamerUserData).where(GooddreamerUserData.email == creds.username))
    user = result.scalar()
    user_role = roles(creds.username)
    
    if not user or not user_role or not verify_password(creds.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password!",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create JWT token and store it to sqlite
    personal_token = await user_token(
        session=sqlite_session, 
        user_id=user.id, 
        role=user_role,
        access_token=create_access_token(subject=user.id), 
        refresh_token=create_refresh_token(subject=user.id))
    
    # Return access token in JSON response and refresh token in headers
    response = JSONResponse(
        content={
            "access_token": personal_token.get("access_token"), 
            "token_type": "Bearer", 
            "role": user_role,
            "success": True})
    response.headers["Authentication"] = str(user.id)

    return response


@router.post("/api/logout")
async def logout_user(
    response: Response,
    session: AsyncSession = Depends(get_sqlite), 
    current_user: GooddreamerUserData = Depends(get_current_user)):
    """
    Logs out a user by clearing the refresh token cookie.
    """
    try:
        await logout(session=session, user_id=current_user.id)
    except Exception:
        response = response = JSONResponse(
            content={"message": "Something error, please try again!", "success": False},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    # Clear the refresh token by setting an expired cookie
    response = JSONResponse(
        content={"message": "Successfully logged out", "success": True},
        status_code=status.HTTP_200_OK
    )
    response.delete_cookie(
        key="refresh_token",
        httponly=True,  # Prevents JavaScript access
        secure=True,    # Ensures it's only sent over HTTPS
        samesite="strict",  # Adjust as needed)
    )
    return response
