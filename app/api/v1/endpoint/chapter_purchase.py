import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_chapter_purchase import fetch_chapter_types

from app.db.models.user import GooddreamerUserData
from app.schemas.chapter_purchase import ChapterCoinData


router = APIRouter()

@router.get("/api/chapter-purchase", response_model=ChapterCoinData)
async def chapter_purchase_data(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    chapter_types: str = Query(..., description="The chapter types data you want to fetch"),
    source: str = Query(..., description="The source of data you want to fetch ('app' or 'web')"),
    from_date: datetime.date = Query(..., description="The start date of data you want to fetch"),
    to_date: datetime.date = Query(..., description="The end date of data you want to fetch"),
    sort_by: str = Query("pembeli_chapter_unique", description="To sort by column data, from novel details tables"),
    ascendings: bool = Query(False, description="To sort by column data, ascendings or descendings")
):
    try:
        data_chapter_coin = await fetch_chapter_types(
            session=session, 
            from_date=from_date, 
            to_date=to_date, 
            chapter_types=chapter_types, 
            sort_by=sort_by,
            ascendings=ascendings,
            source=source)
        if not data_chapter_coin:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return data_chapter_coin
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )