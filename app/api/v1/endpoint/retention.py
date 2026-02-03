import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_retention import fetch_retention

from app.db.models.user import GooddreamerUserData
from app.schemas.retention import RetentionData


router = APIRouter()

@router.get("/api/retention", response_model=RetentionData)
async def retention(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    event_name: str = Query(
        ..., 
        description="The Event Name of data you want to fetch ('User Read Chapter', 'User Buy Chapter With Coin', 'User Buy Chpater With AdsCoin', 'User Buy Chapter With Ads', 'User Buy Coin'"),
    data: str = Query(..., description="The data types to fetch ('float', or 'total_user')"),
    period: str = Query(..., description="The period of user activity to fetch. ('Daily' or 'Monthly')"),
    preset_date: str = Query(
        ..., 
        description="The preset date of data you want to fetch ('last_7_days', 'last_14_days', 'last_28_days', 'last_3_months', 'last_6_months', 'last_12_months'"),
    source: str = Query(..., description="The source of data you want to fetch ('app' or 'web')"),
):
    try:
        user_activity = await fetch_retention(
            session=session,
            event_name=event_name,
            data=data,
            period=period,
            preset_date=preset_date,
            source=source
            )
        if not user_activity:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return user_activity
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )