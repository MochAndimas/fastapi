from pydantic import BaseModel
from typing import Dict


class NewInstallData(BaseModel):
    """Schemas for fetch user new install data"""
    
    install_all: Dict[str, float]
    google_performance: Dict[str, float]
    facebook_performance: Dict[str, float]
    tiktok_performance: Dict[str, float]
    asa_performance: Dict[str, float]

    class Config:
        from_attributes = True


class NewInstallChartData(BaseModel):
    """Schemas for fetch user new install data"""

    source_chart: str
    source_table: str
    fb_chart: str
    fb_table: str
    fb_install_chart: str
    ggl_chart: str
    ggl_table: str
    ggl_install_chart: str
    chart_tiktok_cost_installs: str
    chart_tiktok_installs: str
    table_tiktok_campaign: str
    chart_asa_cost_install: str
    chart_asa_install: str
    table_asa: str
    chart_aso: str

    class Config:
        from_attributes = True
    