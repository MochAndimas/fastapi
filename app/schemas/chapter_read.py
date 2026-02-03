from pydantic import BaseModel
from typing import Dict, Any


class ChapterReadData(BaseModel):
    """Schemas for fetch chapter read data"""
    
    chapter_read_data: Dict[str, float]
    daily_growth: Dict[str, dict]
    frequency_chart: str
    frequency_table: str
    chart_old_new: str
    chart_unique_count: str
    chart_day: str
    chart_category: str
    chart_table: str

    class Config:
        from_attributes = True