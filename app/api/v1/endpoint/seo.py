import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_sqlite
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_seo import fetch_seo, fetch_seo_daily_growth, fetch_seo_chart

from app.db.models.user import GooddreamerUserData
from app.schemas.seo import SeoData, SeoChartData


router = APIRouter()

@router.get("/api/seo", response_model=SeoData)
async def seo(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        seo = await fetch_seo(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not seo:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return seo
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )


@router.get("/api/seo/daily-growth", response_model=SeoData)
async def seo_daily_growth(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        seo = await fetch_seo_daily_growth(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not seo:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return seo
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    

@router.get("/api/seo/chart", response_model=SeoChartData)
async def seo_chart(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        seo = await fetch_seo_chart(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not seo:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return seo
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
