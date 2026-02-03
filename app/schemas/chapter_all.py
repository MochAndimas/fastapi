from pydantic import BaseModel
from typing import Dict


class ChapterAllData(BaseModel):
    """Schemas for fetch chapter data"""
    
    chapter_read_data: Dict[str, float]
    chapter_ads_data: Dict[str, float]
    chapter_coin_data: Dict[str, float]
    chapter_adscoin_data: Dict[str, float]
    overall_chapter_purchase: Dict[str, float]
    overall_oldnew_chapter_purchase: Dict[str, float]

    class Config:
        from_attributes = True


class ChapterAllChart(BaseModel):
    """Schemas for fetch chapter all chart data"""

    pembaca_pembeli_chapter_unique_chart: str
    pembaca_pembeli_chapter_count_chart: str
    pie_chart_old_new_chapter_purchase: str
    pie_chart_old_new_chapter_read: str
    pembaca_old_new_chart: str
    pembeli_old_new_chart: str
    pembaca_chapter_day: str
    pembeli_chapter_day: str
    pembaca_chapter_genre: str
    pembeli_chapter_genre: str
    pembaca_chapter_novel_table: str

    class Config:
        from_attributes = True
