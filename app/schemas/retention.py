from pydantic import BaseModel


class RetentionData(BaseModel):
    """Schemas for fetch user retention data"""
    
    retention_charts: str
    table_cohort: str

    class Config:
        from_attributes = True
