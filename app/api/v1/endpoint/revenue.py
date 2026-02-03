import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db, get_sqlite
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_revenue import fetch_revenue, fetch_revenue_daily_growth, fetch_revenue_chart

from app.db.models.user import GooddreamerUserData
from app.schemas.revenue import RevenueData, RevenueChart


router = APIRouter()

@router.get("/api/revenue", response_model=RevenueData)
async def revenue_data(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    source: str = Query(..., description="The source of data you want to fetch ('app' or 'web')"),
    from_date: datetime.date = Query(..., description="The start date of data you want to fetch"),
    to_date: datetime.date = Query(..., description="The end date of data you want to fetch"),
):
    try:
        revenue_data = await fetch_revenue(session=session, sqlite_session=sqlite_session, from_date=from_date, to_date=to_date, source=source)
        if not revenue_data:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return revenue_data
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )


@router.get("/api/revenue/daily-growth", response_model=RevenueData)
async def revenue_daily_growth(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),
    source: str = Query(..., description="The source of data you want to fetch ('app' or 'web')"),
    from_date: datetime.date = Query(..., description="The start date of data you want to fetch"),
    to_date: datetime.date = Query(..., description="The end date of data you want to fetch")
):
    try:
        revenue_data = await fetch_revenue_daily_growth(session=session, sqlite_session=sqlite_session, from_date=from_date, to_date=to_date, source=source)
        if not revenue_data:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return revenue_data
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )

    
@router.get("/api/revenue/chart", response_model=RevenueChart)
async def revenue_chart(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),
    source: str = Query(..., description="The source of data you want to fetch ('app' or 'web')"),
    from_date: datetime.date = Query(..., description="The start date of data you want to fetch"),
    to_date: datetime.date = Query(..., description="The end date of data you want to fetch"),
    filters: str = Query("", description="To filter coin purchase details data you want to fetch ('paid', 'expired', or 'pending')")
):
    try:
        
        revenue_data = await fetch_revenue_chart(session=session, sqlite_session=sqlite_session, from_date=from_date, to_date=to_date, source=source, filters=filters)
        if not revenue_data:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        
        return revenue_data
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
