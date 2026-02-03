from sqlalchemy import Column, Integer, String, Date, Float
from app.db.base import SqliteBase


class Currency(SqliteBase):
    __tablename__ = "currency"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    idr = Column(Integer, nullable=False)


class GoogleAdsData(SqliteBase):
    __tablename__ = 'googleads_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    campaign_id = Column(Integer, nullable=False)
    campaign_name = Column(String, nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    spend = Column(Float, nullable=False)
    conversions = Column(Float, nullable=False)


class FacebookAdsData(SqliteBase):
    __tablename__ = "facebookads_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_start = Column(Date, nullable=False)
    date_stop = Column(Date, nullable=False)
    campaign_name = Column(String, nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    spend = Column(Integer, nullable=False)
    unique_actions_mobile_app_install = Column(Integer, nullable=False)


class AsaData(SqliteBase):
    __tablename__ = "asa_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    campaign_name = Column(String, nullable=False)
    daily_budget = Column(Integer, nullable=False)
    local_spend = Column(Float, nullable=False)
    impressions = Column(Integer, nullable=False)
    taps = Column(Integer, nullable=False)
    installs = Column(Integer, nullable=False)
    new_downloads = Column(Integer, nullable=False)
    redownloads = Column(Integer, nullable=False)


class TiktokAdsData(SqliteBase):
    __tablename__ = "tiktokads_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    campaign_name = Column(String, nullable=False)
    spend = Column(Integer, nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    conversion = Column(Integer, nullable=False)


class Ga4EventData(SqliteBase):
    __tablename__ = "ga4event_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    platform = Column(String, nullable=False)
    event_name = Column(String, nullable=False)
    event_count = Column(Integer, nullable=False)
    total_user = Column(Integer, nullable=False)


class AdmobReportData(SqliteBase):
    __tablename__ = "admob_report_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    platform = Column(String, nullable=False)
    estimated_earnings = Column(Integer, nullable=False)
    impressions = Column(Integer, nullable=False)
    observed_ecpm = Column(Integer, nullable=False)
    impression_ctr = Column(Float, nullable=False)
    clicks = Column(Integer, nullable=False)
    ad_requests = Column(Integer, nullable=False)
    match_rate = Column(Float, nullable=False)
    match_requests = Column(Integer, nullable=False)


class AdsenseReportData(SqliteBase):
    __tablename__ = "adsense_report_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    platform_type_name = Column(String, nullable=False)
    ad_placement_name = Column(String, nullable=False)
    ad_format_code = Column(String, nullable=False)
    estimated_earnings = Column(Float, nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    ad_requests = Column(Integer, nullable=False)
    matched_ad_requests = Column(Integer, nullable=False)
    impressions_rpm = Column(Float, nullable=False)
    impressions_ctr = Column(Float, nullable=False)
    ad_requests_ctr = Column(Float, nullable=False)
    matched_ad_requests_ctr = Column(Float, nullable=False)


class Ga4SessionsData(SqliteBase):
    __tablename__ = "ga4_session_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    device_category = Column(String, nullable=False)
    platform = Column(String, nullable=False)
    user_engaged_duration = Column(Integer, nullable=False)


class Ga4AnalyticsData(SqliteBase):
    __tablename__ = "ga4_analytics_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    device_category = Column(String, nullable=False)
    platform = Column(String, nullable=False)
    source = Column(String, nullable=False)
    sessions = Column(Integer, nullable=False)
    new_user = Column(Integer, nullable=False)
    active_user = Column(Integer, nullable=False)
    total_user = Column(Integer, nullable=False)
    bounce_rate = Column(Float, nullable=False)
    avg_sesseion_duration = Column(Float, nullable=False)
    engaged_session = Column(Integer, nullable=False)
    user_enagged_duration = Column(Float, nullable=False)


class Ga4LandingPageData(SqliteBase):
    __tablename__ = "ga4_landing_page_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    landing_page = Column(String, nullable=False)
    source = Column(String, nullable=False)
    platform = Column(String, nullable=False)
    medium = Column(String, nullable=False)
    sessions = Column(Integer, nullable=False)


class Ga4ActiveUserData(SqliteBase):
    __tablename__ = "ga4_active_user_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    platform = Column(String, nullable=False)
    active_1day_users = Column(Integer, nullable=False)
    active_28day_users = Column(Integer, nullable=False)

