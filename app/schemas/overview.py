from pydantic import BaseModel
from typing import Dict


class OverviewData(BaseModel):
    """Schemas for fetch overview data"""

    app_register: float
    web_register: float
    total_register: float
    install: Dict[str, float]
    overall_revenue: Dict[str, float]
    cost: float
    app_stickieness: Dict[str, float]
    web_stickieness: Dict[str, float]
    app_revenue: Dict[str, float]
    app_chapter_read: Dict[str, float]
    app_chapter_coin: Dict[str, float]
    app_chapter_adscoin: Dict[str, float]
    app_chapter_ads: Dict[str, float]
    app_total_chapter_purchase: Dict[str, float]
    web_revenue: Dict[str, float]
    web_chapter_read: Dict[str, float]
    web_chapter_coin: Dict[str, float]
    web_chapter_adscoin: Dict[str, float]
    web_chapter_ads: Dict[str, float]
    web_total_chapter_purchase: Dict[str, float]

    class Config:
        from_attributes = True

    
class OverviewDailyGrowthData(BaseModel):
    """Schemas for fetch overview daily growth data"""

    app_register: float
    web_register: float
    total_register: float
    install: Dict[str, float]
    overall_revenue: Dict[str, float]
    cost: float
    app_stickieness: Dict[str, float]
    web_stickieness: Dict[str, float]
    app_revenue_data: Dict[str, float]
    app_chapter_data: Dict[str, dict]
    web_revenue_data: Dict[str, float]
    web_chapter_data: Dict[str, dict]

    class Config:
        from_attributes = True


class OverviewChartData(BaseModel):
    """Schemas for fetch overview chart data"""

    web_ga4_dau_mau: str
    ga4_dau_mau: str
    chart_install: str
    user_journey_chart: str
    web_user_journey_chart: str
    revenue_cost_periods: str
    revenue_cost_charts: str
    payment: str

    class Config:
        from_attributes = True
