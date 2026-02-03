from pydantic import BaseModel
from typing import Dict


class RedeemCodeData(BaseModel):
    """Schemas for fetch user redeem code data"""
    
    metrics_data: Dict[str, float]
    metrics_daily_growth: Dict[str, float]
    redeemed_table: str
    codes_table: str

    class Config:
        from_attributes = True


class IllustrationData(BaseModel):
    """Schemas for fetch user illustration data"""

    metrics_data: Dict[str, float]
    metrics_daily_growth: Dict[str, float]
    transaction_table: str
    illustration_table: str

    class Config:
        from_attributes = True


class OfflineModeData(BaseModel):
    """Schemas for fetch user Offline mode data"""

    metrics_data: Dict[str, float]
    metrics_daily_growth: Dict[str, float]
    unique_bar_chart: str
    count_bar_chart: str

    class Config:
        from_attributes = True


class UpdateData(BaseModel):
    """Schemas for fetch user Offline mode data"""
    message: str
