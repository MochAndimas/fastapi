from pydantic import BaseModel


class SeoData(BaseModel):
    """Schemas for fetch user SEO data"""
    
    sessions: float
    source: float
    total_user: float
    new_user: float
    bounce_rate: float
    periode: str = ""
    dr: float
    indexing: float
    backlinks: float
    ref_domain: float
    total_keywords: float = 0.0
    rank_1_3: float
    rank_4_10: float
    rank_11_30: float

    class Config:
        from_attributes = True


class SeoChartData(BaseModel):
    """Schemas for fetch user SEO chart data"""

    metrics_chart: str
    source_chart: str
    device_chart: str
    landing_page_organic: str
    landing_page_cpc: str
    ranking_chart: str
    web_traffic_chart: str

    class Config:
        from_attributes = True
