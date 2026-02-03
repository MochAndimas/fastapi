from pydantic import BaseModel
from datetime import datetime
from typing import Dict

class CoinBase(BaseModel):
    """Schemas for read coin transaction"""

    id: int
    user_id: int
    transaction_status: int
    transaction_coin_value: int
    created_at: datetime

    class Config:
        from_attributes = True

class RevenueData(BaseModel):
    """Schemas for fetch coin transaction data"""
    coin_data: Dict[str, float]
    returning_first_purchase: Dict[str, float]

    class Config:
        from_attributes = True


class RevenueChart(BaseModel):
    """Schemas for fetch coin dataframe"""
    total_transaksi_coin: str
    persentase_coin: str
    category_coin: str
    revenue_days: str
    coin_days: str
    coin_details: str
    old_new_user_pembeli_koin: str
    chart_returning_first_purchase: str
    unique_count_users_admob: str
    impression_revenue_chart: str
    ads_details: str
    frequency_distribution_ads_chart: str
    frequency_admob_table: str
    overall_revenue_chart: str

    class Config:
        from_attributes = True
