from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db, get_sqlite
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_overiew import fetch_overview, fetch_overview_daily_growth, fetch_overview_chart

from app.db.models.user import GooddreamerUserData
from app.schemas.overview import OverviewData, OverviewDailyGrowthData, OverviewChartData


router = APIRouter()

@router.get("/api/overview", response_model=OverviewData)
async def overview(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),# Validating token
    from_date: date = Query(
        ..., description="The start date of data you want to fetch"),
    to_date: date = Query(
        ..., description="The end date of data you want to fetch")
):
    try:
        overview = await fetch_overview(
            session=session, 
            sqlite_session=sqlite_session, 
            from_date=from_date, 
            to_date=to_date)
        if not overview:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return overview
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    

@router.get("/api/overview/daily-growth", response_model=OverviewDailyGrowthData)
async def overview_daily_growth(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: date = Query(..., description="The start date of data you want to fetch"),
    to_date: date = Query(..., description="The end date of data you want to fetch")
):
    try:
        overview = await fetch_overview_daily_growth(
            session=session, 
            sqlite_session=sqlite_session, 
            from_date=from_date, 
            to_date=to_date)
        if not overview:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return overview
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    

@router.get("/api/overview/chart", response_model=OverviewChartData)
async def overview_chart(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: date = Query(..., description="The start date of data you want to fetch"),
    to_date: date = Query(..., description="The end date of data you want to fetch")
):
    try:
        overview = await fetch_overview_chart(
            session=session, 
            sqlite_session=sqlite_session, 
            from_date=from_date, 
            to_date=to_date)
        if not overview:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return overview
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
