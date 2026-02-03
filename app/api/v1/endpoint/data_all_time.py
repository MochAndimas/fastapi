import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db, get_sqlite
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_data_all_time import fetch_data_all_time, fetch_data_all_time_chart

from app.db.models.user import GooddreamerUserData
from app.schemas.data_all_time import AllTimeData, AllTimeDataChart


router = APIRouter()

@router.get("/api/data-all-time", response_model=AllTimeData)
async def data_all_time(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    year: str = Query(..., description="The Year of data you want to fetch")
):
    try:
        from_date = datetime.datetime.strptime(f'{year}-01-01', '%Y-%m-%d').date()
        to_date = datetime.datetime.strptime(f'{year}-12-31', '%Y-%m-%d').date()
        data_all_time = await fetch_data_all_time(
            session=session, 
            sqlite_session=sqlite_session, 
            from_date=from_date, 
            to_date=to_date)
        if not data_all_time:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return data_all_time
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    

@router.get("/api/data-all-time/chart", response_model=AllTimeDataChart)
async def data_all_time_chart(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    year: str = Query(..., description="The Year of data you want to fetch")
):
    try:
        from_date = datetime.datetime.strptime(f'{year}-01-01', '%Y-%m-%d').date()
        to_date = datetime.datetime.strptime(f'{year}-12-31', '%Y-%m-%d').date()
        data_all_time = await fetch_data_all_time_chart(
            session=session, 
            sqlite_session=sqlite_session, 
            from_date=from_date, 
            to_date=to_date)
        if not data_all_time:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return data_all_time
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
