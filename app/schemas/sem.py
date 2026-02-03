from pydantic import BaseModel
from typing import Dict


class SemData(BaseModel):
    """Schemas for fetch user SEM data"""
    
    google_sem: Dict[str, float]
    google_gdn: Dict[str, float]
    facebook_gdn: Dict[str, float]

    class Config:
        from_attributes = True


class SemChartData(BaseModel):
    """Schemas for fetch user SEM chart data"""

    google_sem_spend_chart: str
    google_sem_metrics_chart: str
    google_sem_details_table: str
    google_gdn_spend_chart: str
    google_gdn_metrics_chart: str
    google_gdn_details_table: str
    facebook_gdn_spend_chart: str
    facebook_gdn_metrics_chart: str
    facebook_gdn_details_table: str
