import datetime
from google.ads.googleads.client import GoogleAdsClient
from fastapi.responses import JSONResponse
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db, get_sqlite
from app.utils.user_utils import get_current_user
from app.api.v1.functions.fetch_feature_data import fetch_redeem_code, fetch_illustration_transaction, fetch_offline_mode
from app.utils.external_api import ga4_active_user, get_google_analytics_data, landing_page_ga4, google_sheet_api, usd_idr_to_csv
from app.utils.external_api import get_asa_campaign_report, tiktok_report_API, get_access_token, ga4_event_data
from app.utils.external_api import admob_report_api, adsense_reprot_api, fb_api, google_reporting, get_ga4_session

from app.db.models.user import GooddreamerUserData
from app.schemas.feature_data import RedeemCodeData, IllustrationData, OfflineModeData, UpdateData


router = APIRouter()

@router.get("/api/feature-data/redeem-code", response_model=RedeemCodeData)
async def redeem_code(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering."),
    codes: str = Query("", description="Voucher code to filter by (default is '')."),
    user_type: str = Query("", description="User type to filter by (default is '').")
):
    try:
        redeemed_code = await fetch_redeem_code(
            session=session,
            from_date=from_date,
            to_date=to_date,
            codes=codes,
            user_type=user_type
            )
        if not redeemed_code:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return redeemed_code
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    
@router.get("/api/feature-data/illustration-transaction", response_model=IllustrationData)
async def illustration_transaction(
    session: AsyncSession = Depends(get_db),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering."),
    source: str = Query("", description="Filter by source name."),
    novel_title: str = Query("", description="Filter by novel title."),
    illustration_id: int = Query(0, description="Filter by illustration ID.")
):
    try:
        illustration = await fetch_illustration_transaction(
            session=session,
            from_date=from_date,
            to_date=to_date,
            source=source,
            novel_title=novel_title,
            illustration_id=illustration_id
            )
        if not illustration:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return illustration
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
    

@router.get("/api/feature-data/offline-mode", response_model=OfflineModeData)
async def offline_mode(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    from_date: datetime.date = Query(..., description="Start date for data filtering."),
    to_date: datetime.date = Query(..., description="End date for data filtering.")
):
    try:
        offline = await fetch_offline_mode(
            session=session,
            from_date=from_date,
            to_date=to_date
            )
        if not offline:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date range."
            )    
        return offline
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )


@router.get("/api/feature-data/update-external-api", response_model=UpdateData)
async def udpate_data(
    session: AsyncSession = Depends(get_sqlite),
    current_user: GooddreamerUserData = Depends(get_current_user),  # Validates the token
    data: str = Query(..., description="The External data to update.")
):
    try:
        # Filter Date
        last_60days = datetime.datetime.today() - datetime.timedelta(60)
        start_time = datetime.datetime.today() - datetime.timedelta(60)
        end_time = datetime.datetime.today() - datetime.timedelta(1)
        start_date = start_time.date()
        end_date = end_time.date()
        message = ""
        
        if data == "all":
            ACCESS_TOKEN = get_access_token()
            await get_asa_campaign_report(session=session, start_date=start_date ,end_date=end_date, access_token=ACCESS_TOKEN, types="manual")
            await google_reporting(session=session, start_date=start_date, end_date=end_date, types="manual")
            await fb_api(session=session, start_date=start_date, end_date=end_date, types="manual")
            await tiktok_report_API(session=session, start_date=start_date, end_date=end_date, types="manual")
            await admob_report_api(session=session, start_date=start_date, end_date=end_date, types="manual")
            await adsense_reprot_api(session=session, from_date=start_date, to_date=end_date, types="manual")
            await ga4_event_data(session=session, start_date=start_date, end_date=end_date, types="manual")
            await usd_idr_to_csv(session=session, start_date=start_date, end_date=end_date, types="manual")
            await get_ga4_session(session=session, start_date=start_date, end_date=end_date, types="manual")
            await get_google_analytics_data(session=session, start_date=start_date, end_date=end_date, types="manual")
            await fb_api(session=session, start_date=start_date, end_date=end_date, campaign_name="FB-BA_UA-Traffic_Web-ID-AON", attribution_window=["7d_click", "1d_view"], types="manual")
            await landing_page_ga4(session=session, start_date=start_date, end_date=end_date, types="manual")
            await ga4_active_user(session=session, start_date=start_date, end_date=end_date, types="manual")
            await google_sheet_api(sheet_range='Indexing!A1:CZ5', file='indexing', types="manual")
            await google_sheet_api(sheet_range='Ranking!A1:CZ200', file='ranking', types="manual")
            await google_sheet_api(sheet_range='dau_mau_web!A1:D1000', file='dau_mau_web', types="manual")
            await google_sheet_api(sheet_range='play_console_install!A1:D1000', file='play_console_install', types="manual")
            await google_sheet_api(sheet_range='organic_play_console!A1:R1000', file='organic_play_console', types="manual")
            await google_sheet_api(sheet_range='apple_total_download!A1:C1000', file='apple_total_download', types="manual")
            await google_sheet_api(sheet_range='cost_revenue!A1:D1000', file='cost_revenue', types="manual")
            message = "Data Update Successfully!"
        elif data == "asa":
            ACCESS_TOKEN = get_access_token()
            message = await get_asa_campaign_report(session=session, start_date=start_date ,end_date=end_date, access_token=ACCESS_TOKEN, types="manual")
        elif data == "googleads":
            message = await google_reporting(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "facebook":
            message = await fb_api(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "facebook_gdn":
            message = await fb_api(session=session, start_date=start_date, end_date=end_date, campaign_name="FB-BA_UA-Traffic_Web-ID-AON", attribution_window=["7d_click", "1d_view"], types="manual")
        elif data == "tiktok":
            message = await tiktok_report_API(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "admob":
            message = await admob_report_api(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "adsense":
            message = await adsense_reprot_api(session=session, from_date=start_date, to_date=end_date, types="manual")
        elif data == "ga4_event":
            message = await ga4_event_data(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "currency":
            message = await usd_idr_to_csv(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "ga4_session":
            message = await get_ga4_session(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "ga4_analytics":
            message = await get_google_analytics_data(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "ga4_landing_page":
            message = await landing_page_ga4(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "ga4_active_users":
            message = await ga4_active_user(session=session, start_date=start_date, end_date=end_date, types="manual")
        elif data == "indexing":
            message = await google_sheet_api(sheet_range='Indexing!A1:CZ5', file='indexing', types="manual")
        elif data == "ranking":
            message = await google_sheet_api(sheet_range='Ranking!A1:CZ200', file='ranking', types="manual")
        elif data == "dau_mau_web":
            message = await google_sheet_api(sheet_range='dau_mau_web!A1:D1000', file='dau_mau_web', types="manual")
        elif data == "play_console_install":
            message = await google_sheet_api(sheet_range='play_console_install!A1:D1000', file='play_console_install', types="manual")
        elif data == "organic_play_console":
            message = await google_sheet_api(sheet_range='organic_play_console!A1:R1000', file='organic_play_console', types="manual")
        elif data == "apple_total_download":
            message = await google_sheet_api(sheet_range='apple_total_download!A1:C1000', file='apple_total_download', types="manual")
        elif data == "cost_revenue":
            message = await google_sheet_api(sheet_range='cost_revenue!A1:D1000', file='cost_revenue', types="manual")

        if not message:
            raise HTTPException(
                status_code=404, detail="Data Update is failed!"
            )    
        return JSONResponse(
            content={
                "message": message
            }
        )
    except ZeroDivisionError as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )
