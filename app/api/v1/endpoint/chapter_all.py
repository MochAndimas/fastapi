import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_chapter_all import fetch_chapter_all, fetch_chapter_daily_growth, fetch_chapter_all_chart

from app.db.models.user import GooddreamerUserData
from app.schemas.chapter_all import ChapterAllData, ChapterAllChart


router = APIRouter()

@router.get("/api/chapter", response_model=ChapterAllData)
async def chapter_data(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    source: str = Query(..., description="The source of data you want to fetch ('app' or 'web')"),
    from_date: datetime.date = Query(..., description="The start date of data you want to fetch"),
    to_date: datetime.date = Query(..., description="The end date of data you want to fetch"),
):
    try:
        data_chapter = await fetch_chapter_all(session=session, from_date=from_date, to_date=to_date, source=source)
        if not data_chapter:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return data_chapter
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    
@router.get("/api/chapter/daily-growth", response_model=ChapterAllData)
async def chapter_data_daily_growth(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    source: str = Query(..., description="The source of data you want to fetch ('app' or 'web')"),
    from_date: datetime.date = Query(..., description="The start date of data you want to fetch"),
    to_date: datetime.date = Query(..., description="The end date of data you want to fetch"),
):
    try:
        data_chapter = await fetch_chapter_daily_growth(session=session, from_date=from_date, to_date=to_date, source=source)
        if not data_chapter:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return data_chapter
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    
@router.get("/api/chapter/chart", response_model=ChapterAllChart)
async def chapter_data_chart(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    source: str = Query(..., description="The source of data you want to fetch ('app' or 'web')"),
    from_date: datetime.date = Query(..., description="The start date of data you want to fetch"),
    to_date: datetime.date = Query(..., description="The end date of data you want to fetch"),
    sort_by: str = Query("pembaca_chapter_unique", description="To sort by column data, from novel details tables"),
    ascendings: bool = Query(False, description="To sort by column data, ascendings or descendings")
):
    try:
        data_chapter = await fetch_chapter_all_chart(
            session=session, 
            from_date=from_date, 
            to_date=to_date, 
            sort_by=sort_by,
            ascendings=ascendings,
            source=source)
        if not data_chapter:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return data_chapter
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
