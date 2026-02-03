import uuid
from datetime import datetime, timedelta
from fastapi.responses import JSONResponse
from fastapi import Depends, HTTPException, status, Request, Response
from fastapi.security import OAuth2PasswordBearer
from app.db.models.user import GooddreamerUserData, UserToken
from app.core.security import verify_access_token, refresh_access_token
from app.db.session import get_db, get_sqlite
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from jose.exceptions import ExpiredSignatureError, JWSSignatureError, JWTError


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")


def roles(email):
    """
    Retrieve the role of a given email.

    Args:
        email (str): The email address to look up.

    Returns:
        Optional[str]: The role associated with the email, or None if no role is found.
    """
    user_role = {
        'developer': ['dimas+1@gooddreamer.id'],
        'superadmin': ['abdi@gooddreamer.id', 'febri_growth@gooddreamer.id', 'rama@gooddreamer.id'],
        'growth': [
            'betharia@gooddreamer.id', 'ghea@gooddreamer.id', 'celi@gooddreamer.id', 
            'adelia@gooddreamer.id', 'eca@gooddreamer.id', 'audi@gooddreamer.id', 
            'dimas@gooddreamer.id'],
        'operation': ['sisi-finance@gooddreamer.id', 'naura@gooddreamer.id']
    }

    for role, emails in user_role.items():
        if email in emails:
            return role
    return False


async def get_user_by_id(
    id: int, session: AsyncSession = Depends(get_db)
) -> GooddreamerUserData | None:
    """Retrieve a user from the database by their email."""
    return (
        (await session.execute(select(GooddreamerUserData).filter_by(id=id)))
        .scalars()
        .first()
    )


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite)
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    # If the access token has expired, attempt to refresh it
    query_refresh_token = select(UserToken).filter_by(access_token=token)
    data_refresh_token = await sqlite_session.execute(query_refresh_token)
    user_token = data_refresh_token.scalars().first()
    if user_token is None:
        raise credentials_exception
    if user_token.is_revoked:
        raise credentials_exception

    try:
        # Try to verify the access token
        token_data = await verify_access_token(
            sqlite_session, token)
    except ExpiredSignatureError:
        # Verify and refresh the access token using the refresh token
        new_access_token = await refresh_access_token(
            sqlite_session, user_token.refresh_token)
        # Re-attempt to verify the new access token
        token_data = await verify_access_token(
            sqlite_session, new_access_token)
    except (JWSSignatureError, JWTError):
        raise credentials_exception
    # Fetch the user from the database
    user = await get_user_by_id(id=token_data.id, session=db)
    
    if user is None:
        raise credentials_exception
    return user


async def user_token(
        session: AsyncSession, 
        user_id: int, 
        role: str,
        access_token: str, 
        refresh_token: str):
    """
    Function store credentials to sqlite database
    """
    today = datetime.now()
    session_id = str(uuid.uuid4())  # Generate a unique session ID
    expiry = datetime.now() + timedelta(days=7)  # Example expiry timestamp
    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)
        
    async with session.begin():
        query = select(UserToken).filter_by(user_id=user_id)
        user_data = await session.execute(query)
        user = user_data.scalars().first()

        if user:
            user.session_id = session_id
            user.logged_in = True
            user.expiry = expiry
            user.is_revoked = False
            user.access_token = access_token
            user.refresh_token = refresh_token
            user.updated_at = today
        else:
            data = UserToken(
                session_id=session_id,
                user_id=user_id,
                page="home",
                logged_in=True,
                role=role,
                expiry=expiry,
                access_token=access_token,
                refresh_token=refresh_token,
                is_revoked=False,
                created_at=today,
                updated_at=today
            )
            session.add(data)
        await session.commit()
        await session.close()

        data = {
            "access_token": access_token,
            "refresh_token": refresh_token
        }
    
        return data
    

async def logout(session: AsyncSession, user_id):
    """Function to Log out user and revoke all token"""
    today = datetime.now()
    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)

    query = select(UserToken).filter_by(user_id=user_id)
    user_data = await session.execute(query)
    user = user_data.scalars().first()

    user.logged_in = False
    user.is_revoked = True
    user.expiry = today
    user.updated_at = today

    await session.commit()
    await session.close()

