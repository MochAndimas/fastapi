from pydantic import BaseModel
from typing import Dict, Any


class ChapterCoinData(BaseModel):
    """Schemas for fetch chapter data"""
    
    chapter_data: Dict[str, float]
    data_daily_growth: Dict[str, float]
    old_new_chart: str
    unique_count_chart: str
    chapter_by_day_chart: str
    chapter_by_category: str
    chapter_table: str

    class Config:
        from_attributes = True