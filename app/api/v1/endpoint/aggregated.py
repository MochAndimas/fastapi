from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db, get_sqlite
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_aggregated import fetch_aggregated

from app.db.models.user import GooddreamerUserData
from app.schemas.aggregated import AggregatedData


router = APIRouter()

@router.get("/api/aggregated", response_model=AggregatedData)
async def aggregated(
    session: AsyncSession = Depends(get_db),
    sqlite_session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: date = Query(..., description="The start date of data you want to fetch"),
    to_date: date = Query(..., description="The end date of data you want to fetch")
):
    try:
        aggregated = await fetch_aggregated(
            session=session, 
            sqlite_session=sqlite_session, 
            from_date=from_date, 
            to_date=to_date)
        if not aggregated:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return aggregated
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )