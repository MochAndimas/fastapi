import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_sqlite
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_sem import fetch_sem, fetch_sem_daily_growth, fetch_sem_chart

from app.db.models.user import GooddreamerUserData
from app.schemas.sem import SemData, SemChartData


router = APIRouter()

@router.get("/api/sem", response_model=SemData)
async def sem(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        sem = await fetch_sem(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not sem:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return sem
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    

@router.get("/api/sem/daily-growth", response_model=SemData)
async def sem_daily_growth(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        sem = await fetch_sem_daily_growth(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not sem:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return sem
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    

@router.get("/api/sem/chart", response_model=SemChartData)
async def sem_chart(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        sem = await fetch_sem_chart(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not sem:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return sem
    except ValueError as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
