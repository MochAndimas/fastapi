import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_novel import fetch_novel, fetch_novel_details, fetch_novel_details_chart

from app.db.models.user import GooddreamerUserData
from app.schemas.novel import NovelData, NovelDetailsData, NovelDetailsChartData


router = APIRouter()

@router.get("/api/novel/novel-analytics", response_model=NovelData)
async def novel(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering."),
    novel_title: str = Query("", description="Filter by novel title (case-insensitive)"),
    category_novel: str = Query("", description="Filter by novel category (case-insensitive)"),
    sort_by: str = Query("reader_purchase_percentage", description="Column to sort by. Defaults to 'presentase_pembaca_ke_pembeli'"),
    ascending: bool = Query(True, description="Whether to sort in ascending order. Defaults to True.")
):
    try:
        sem = await fetch_novel(
            session=session,
            from_date=from_date,
            to_date=to_date,
            novel_title=novel_title,
            category_novel=category_novel,
            sort_by=sort_by,
            ascending=ascending
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
    

@router.get("/api/novel/novel-details", response_model=NovelDetailsData)
async def novel_details(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering."),
    novel_title: str = Query("", description="Filter by novel title (case-insensitive)")
):
    try:
        sem = await fetch_novel_details(
            session=session,
            from_date=from_date,
            to_date=to_date,
            novel_title=novel_title
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
    

@router.get("/api/novel/novel-details/chart", response_model=NovelDetailsChartData)
async def novel_details_chart(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering."),
    novel_title: str = Query("", description="Filter by novel title (case-insensitive)")
):
    try:
        sem = await fetch_novel_details_chart(
            session=session,
            from_date=from_date,
            to_date=to_date,
            novel_title=novel_title
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
