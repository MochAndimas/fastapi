import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db, get_sqlite
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_user_activity import fetch_user_activity

from app.db.models.user import GooddreamerUserData
from app.schemas.user_activity import UserActivityData


router = APIRouter()

@router.get("/api/user-activity", response_model=UserActivityData)
async def user_activity(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    source: str = Query(..., description="The source of data you want to fetch ('app' or 'web')"),
    types: str = Query(..., description="The data types of user activity to fetch."),
    year: str = Query(..., description="The Year of data you want to fetch")
):
    try:
        from_date = datetime.datetime.strptime(f'{year}-01-01', '%Y-%m-%d').date()
        to_date = datetime.datetime.strptime(f'{year}-12-31', '%Y-%m-%d').date()
        user_activity = await fetch_user_activity(
            session=session, 
            sqlite_session=sqlite_session, 
            from_date=from_date, 
            to_date=to_date, 
            source=source, 
            types=types)
        if not user_activity:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return user_activity
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )