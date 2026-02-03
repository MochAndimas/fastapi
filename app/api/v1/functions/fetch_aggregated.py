import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from app.utils.chapter_all_utils import DataChapter
from app.utils.revenue_utils import RevenueData
from app.utils.aggregated_utils import play_console_install, apple_total_download
from app.utils.aggregated_utils import overall_data, register
pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_aggregated(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date
):
    """
    Fetch aggregated data for a specified date range.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.

    Returns:
        Dict[str, Any]: A dictionary containiner aggregated data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")

        # Initialize data fetchers
        revenue_data = await RevenueData.load_data(
            session=session, 
            sqlite_session=sqlite_session, 
            from_date=from_date, 
            to_date=to_date,
            period="monthly")
        chapter_data = await DataChapter.load_data(
            session=session, 
            from_date=from_date,
            to_date=to_date,
            data="all",
            period="monthly")

        # App Data
        app_data = await asyncio.gather(
            revenue_data.revenue_data(from_date=from_date, to_date=to_date, metrics=["first_purchase", "return_purchase", "coin_unique"], source="app"),
            chapter_data.chapter_read(from_date=from_date, to_date=to_date, source="app"),
            chapter_data.chapter_coin(from_date=from_date, to_date=to_date, source="app"),
            chapter_data.chapter_adscoin(from_date=from_date, to_date=to_date, source="app"),
            chapter_data.chapter_ads(from_date=from_date, to_date=to_date, source="app"),
            register(session=session, from_date=from_date, to_date=to_date, source="app")
        )

        # Web Data
        web_data = await asyncio.gather(
            revenue_data.revenue_data(from_date=from_date, to_date=to_date, metrics=["first_purchase", "return_purchase", "coin_unique"], source="web"),
            chapter_data.chapter_read(from_date=from_date, to_date=to_date, source="web"),
            chapter_data.chapter_coin(from_date=from_date, to_date=to_date, source="web"),
            chapter_data.chapter_adscoin(from_date=from_date, to_date=to_date, source="web"),
            chapter_data.chapter_ads(from_date=from_date, to_date=to_date, source="web"),
            register(session=session, from_date=from_date, to_date=to_date, source="web")
        )
        
        app_total_chapter_purchase, web_total_chapter_purchase = await asyncio.gather(
            asyncio.to_thread(
                chapter_data.total_chapter_purchase,
                chapter_coin_data=app_data[2], 
                chapter_adscoin_data=app_data[3], 
                chapter_ads_data=app_data[4],
                metrics_1='chapter_unique',
                metrics_2='chapter_count'),
            asyncio.to_thread(
                chapter_data.total_chapter_purchase,
                chapter_coin_data=web_data[2], 
                chapter_adscoin_data=web_data[3], 
                chapter_ads_data=web_data[4],
                metrics_1='chapter_unique',
                metrics_2='chapter_count')
        )

        # Overall data
        data_overall = await asyncio.gather(
            overall_data(session=session, from_date=from_date, to_date=to_date, data="register"),
            overall_data(session=session, from_date=from_date, to_date=to_date, data="pembaca"),
            overall_data(session=session, from_date=from_date, to_date=to_date, data="pembeli"),
            play_console_install(from_date=from_date, to_date=to_date),
            apple_total_download(from_date=from_date, to_date=to_date)
        )

        # Compile the final data dictionary
        data = {
            "install_android": data_overall[3],
            "install_ios": data_overall[4],
            "total_install": data_overall[3] + data_overall[4],
            "overall_register": data_overall[0],
            "overall_pembaca": data_overall[1],
            "overall_pembeli": data_overall[2],
            "app_pembelian_coin": app_data[0],
            "app_chapter_read": app_data[1],
            "app_chapter_coin": app_data[2],
            "app_chapter_adscoin": app_data[3],
            "app_chapter_ads": app_data[4],
            "register_week": app_data[5],
            "app_total_chapter_purchase": app_total_chapter_purchase,
            "web_pembelian_coin": web_data[0],
            "web_chapter_read": web_data[1],
            "web_chapter_coin": web_data[2],
            "web_chapter_adscoin": web_data[3],
            "web_chapter_ads": web_data[4],
            "web_register_week": web_data[5],
            "web_total_chapter_purchase": web_total_chapter_purchase
        }
        logger.info(f"Data retrieved successfully!")

        return data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")
        raise

    except Exception as e:
        logger.error(f"An error occurred while fetching aggregated data: {e}")
        raise
    