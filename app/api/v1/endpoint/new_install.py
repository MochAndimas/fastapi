import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_sqlite
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_new_install import fetch_new_install, fetch_new_install_daily_growth, fetch_new_install_chart

from app.db.models.user import GooddreamerUserData
from app.schemas.new_install import NewInstallData, NewInstallChartData


router = APIRouter()

@router.get("/api/new-install", response_model=NewInstallData)
async def new_install(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        new_install = await fetch_new_install(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not new_install:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return new_install
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    

@router.get("/api/new-install/daily-growth", response_model=NewInstallData)
async def new_install_daily_growth(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        new_install = await fetch_new_install_daily_growth(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not new_install:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return new_install
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    

@router.get("/api/new-install/chart", response_model=NewInstallChartData)
async def new_install_chart(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        new_install = await fetch_new_install_chart(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not new_install:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return new_install
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    