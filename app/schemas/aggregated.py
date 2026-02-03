from pydantic import BaseModel
from typing import Dict, Any


class AggregatedData(BaseModel):
    """Schemas for fetch all aggregated data"""

    install_android: float
    install_ios: float
    total_install: float
    overall_register: float
    overall_pembaca: float
    overall_pembeli: float
    register_week: float
    app_pembelian_coin: Dict[str, float]
    app_chapter_read: Dict[str, float]
    app_chapter_coin: Dict[str, float]
    app_chapter_adscoin: Dict[str, float]
    app_chapter_ads: Dict[str, float]
    app_total_chapter_purchase: Dict[str, float]
    web_register_week: float
    web_pembelian_coin: Dict[str, float]
    web_chapter_read: Dict[str, float]
    web_chapter_coin: Dict[str, float]
    web_chapter_adscoin: Dict[str, float]
    web_chapter_ads: Dict[str, float]
    web_total_chapter_purchase: Dict[str, float]

    class Config:
        from_attributes = True