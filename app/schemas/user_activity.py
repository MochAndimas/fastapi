from pydantic import BaseModel


class UserActivityData(BaseModel):
    """Schemas for fetch user activity data"""
    
    chart_activity_time: str
    session_chart: str

    class Config:
        from_attributes = True
