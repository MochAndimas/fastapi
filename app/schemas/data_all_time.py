from pydantic import BaseModel
from typing import Dict, Any


class AllTimeData(BaseModel):
    """Schemas for fetch all time data"""

    app_pembelian_coin: Dict[str, float]
    app_return_first_coin: Dict[str, float]
    app_chapter_read: Dict[str, float]
    app_chapter_coin: Dict[str, float]
    app_chapter_adscoin: Dict[str, float]
    app_chapter_ads: Dict[str, float]
    app_total_chapter_purchase: Dict[str, float]
    web_pembelian_coin: Dict[str, float]
    web_return_first_coin: Dict[str, float]
    web_chapter_read: Dict[str, float]
    web_chapter_coin: Dict[str, float]
    web_chapter_adscoin: Dict[str, float]
    web_chapter_ads: Dict[str, float]
    web_total_chapter_purchase: Dict[str, float]
    all_revenue_data: float

    class Config:
        from_attributes = True


class AllTimeDataChart(BaseModel):
    """Schemas for fetch all time data chart"""

    pembaca_pembeli_chapter_unique_chart_all: str
    pembaca_pembeli_chapter_count_chart_all: str
    pembaca_chapter_novel_table: str
    web_pembaca_pembeli_chapter_unique_chart_all: str
    web_pembaca_pembeli_chapter_count_chart_all: str
    web_pembaca_chapter_novel_table: str
    first_ret_purchase: str
    coin_month: str
    persentase_koin_all_time: str
    rev_month: str
    users_unique_count_overall_chart: str
    revenue_chart: str
    web_first_ret_purchase: str
    web_coin_month: str
    web_persentase_koin_all_time: str
    web_rev_month: str
    web_users_unique_count_overall_chart: str
    adsense_revenue_all: str
    all_revenue_chart: str
