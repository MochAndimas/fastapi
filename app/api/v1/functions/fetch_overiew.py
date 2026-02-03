import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from app.utils.chapter_all_utils import DataChapter, user_activity
from app.utils.revenue_utils import RevenueData
from app.utils.new_install_utils import InstallData, cost, dg_cost
from app.utils.aggregated_utils import register, dg_register
from app.utils.overview_utils import ga4_mau_dau_df, dau_mau_df, dg_stickiness
from app.utils.overview_utils import dau_mau_chart, ga4_mau_dau, install_chart
from app.utils.overview_utils import revenue_cost_periods_chart, revenue_cost_chart, payment_channel
pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_overview(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date
):
    """
    Fetch overview data for a specified date range.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.

    Returns:
        Dict[str, Any]: A dictionary containiner overview data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")

        # Initialize data fetchers
        data_chapter = await DataChapter.load_data(
            session=session,
            from_date=from_date, 
            to_date=to_date,
            period="daily",
            data="all"
        )
        data_revenue = await RevenueData.load_data(
            session=session,
            sqlite_session=sqlite_session,
            from_date=from_date,
            to_date=to_date,
            period="daily"
        )
        data_install = await InstallData.load_data(
            session=sqlite_session,
            from_date=from_date,
            to_date=to_date
        )
        
        # data overview
        data_overview = await asyncio.gather(
            register(session=session, from_date=from_date, to_date=to_date, source="app"),
            register(session=session, from_date=from_date, to_date=to_date, source="web"),
            register(session=session, from_date=from_date, to_date=to_date, source="all"),
            data_install.overall_install(from_date=from_date, to_date=to_date, metrics=["android_install", "apple_install", "total_install"]),
            data_revenue.revenue_data(from_date=from_date, to_date=to_date, source="all", metrics=["overall_revenue"]),
            cost(session=sqlite_session, from_date=from_date, to_date=to_date, data="scalar"),
            ga4_mau_dau_df(session=sqlite_session, from_date=from_date, to_date=to_date, data="stickiness", source="app"),
            dau_mau_df(from_date=from_date, to_date=to_date, data="stickiness", source="web")
        )

        # App User activity data
        app_data = await asyncio.gather(
            data_revenue.revenue_data(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_read(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_coin(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_adscoin(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_ads(from_date=from_date, to_date=to_date, source="app")
        )

        # Web User activity data
        web_data = await asyncio.gather(
            data_revenue.revenue_data(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_read(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_coin(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_adscoin(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_ads(from_date=from_date, to_date=to_date, source="web")
        )

        app_total_chapter_purchase, web_total_chapter_purchase = await asyncio.gather(
            asyncio.to_thread(
                data_chapter.total_chapter_purchase,
                chapter_coin_data=app_data[2], 
                chapter_adscoin_data=app_data[3], 
                chapter_ads_data=app_data[4],
                metrics_1='chapter_unique',
                metrics_2='chapter_count'),
            asyncio.to_thread(
                data_chapter.total_chapter_purchase,
                chapter_coin_data=web_data[2], 
                chapter_adscoin_data=web_data[3], 
                chapter_ads_data=web_data[4],
                metrics_1='chapter_unique',
                metrics_2='chapter_count')
            )

        # Compile the final data dictionary
        data = {
            "app_register": data_overview[0],
            "web_register": data_overview[1],
            "total_register": data_overview[2],
            "install": data_overview[3],
            "overall_revenue": data_overview[4],
            "cost": data_overview[5],
            "app_stickieness": data_overview[6],
            "web_stickieness": data_overview[7],
            "app_revenue": app_data[0],
            "app_chapter_read": app_data[1],
            "app_chapter_coin": app_data[2],
            "app_chapter_adscoin": app_data[3],
            "app_chapter_ads": app_data[4],
            "app_total_chapter_purchase": app_total_chapter_purchase,
            "web_revenue": web_data[0],
            "web_chapter_read": web_data[1],
            "web_chapter_coin": web_data[2],
            "web_chapter_adscoin": web_data[3],
            "web_chapter_ads": web_data[4],
            "web_total_chapter_purchase": web_total_chapter_purchase
        }
        logger.info(f"Data retrieved successfully!")

        return data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise ve

    except KeyError as ke:
        logger.error(f"An error occurred while fetching overview chart data: {ke}")
        raise ke
    
    except Exception as e:
        logger.error(f"An error occurred while fetching overview chart data: {e}")
        raise e


async def fetch_overview_daily_growth(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date
):
    """
    Fetch overview daily growth data for a specified date range.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.

    Returns:
        Dict[str, Any]: A dictionary containiner overview daily growth data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")
        
        # Calculate date range for the previous week
        delta = (to_date - from_date) + timedelta(days=1)
        fromdate_lastweek = from_date - delta

        # Initialize data fetchers
        data_chapter = await DataChapter.load_data(
            session=session,
            from_date=fromdate_lastweek, 
            to_date=to_date,
            period="daily",
            data="all"
        )
        data_revenue = await RevenueData.load_data(
            session=session,
            sqlite_session=sqlite_session,
            from_date=fromdate_lastweek,
            to_date=to_date,
            period="daily"
        )
        data_install = await InstallData.load_data(
            session=sqlite_session,
            from_date=fromdate_lastweek,
            to_date=to_date
        )
        
        # data overview
        data_overview = await asyncio.gather(
            dg_register(session=session, from_date=from_date, to_date=to_date, source="app"),
            dg_register(session=session, from_date=from_date, to_date=to_date, source="web"),
            dg_register(session=session, from_date=from_date, to_date=to_date, source="all"),
            data_install.daily_growth(from_date=from_date, to_date=to_date, metrics=["android_install", "apple_install", "total_install"]),
            data_revenue.daily_growth(from_date=from_date, to_date=to_date, source="all", metrics=["overall_revenue"]),
            dg_cost(session=sqlite_session, from_date=from_date, to_date=to_date, data="scalar"),
            dg_stickiness(session=sqlite_session, from_date=from_date, to_date=to_date, source="app", file="ga4"),
            dg_stickiness(session=sqlite_session, from_date=from_date, to_date=to_date, source="web", file="moe")
        )
        
        # App User activity data
        app_data = await asyncio.gather(
            data_revenue.daily_growth(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.daily_growth(from_date=from_date, to_date=to_date, source="app")
        )
        
        # Web User activity data
        web_data = await asyncio.gather(
            data_revenue.daily_growth(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.daily_growth(from_date=from_date, to_date=to_date, source="web")
        )
        
        # Compile the final data dictionary
        data = {
            "app_register": data_overview[0],
            "web_register": data_overview[1],
            "total_register": data_overview[2],
            "install": data_overview[3],
            "overall_revenue": data_overview[4],
            "cost": data_overview[5],
            "app_stickieness": data_overview[6],
            "web_stickieness": data_overview[7],
            "app_revenue_data": app_data[0],
            "app_chapter_data": app_data[1],
            "web_revenue_data": web_data[0],
            "web_chapter_data": web_data[1]
        }
        logger.info(f"Data retrieved successfully!")

        return data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise ve

    except KeyError as ke:
        logger.error(f"An error occurred while fetching overview chart data: {ke}")
        raise ke
    
    except Exception as e:
        logger.error(f"An error occurred while fetching overview chart data: {e}")
        raise e


async def fetch_overview_chart(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date
):
    """
    Fetch overview chart data for a specified date range.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.

    Returns:
        Dict[str, Any]: A dictionary containiner overview chart data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")

        # Initialize data fetchers
        data_chapter = await DataChapter.load_data(
            session=session,
            from_date=from_date, 
            to_date=to_date,
            period="daily",
            data="all"
        )
        data_revenue = await RevenueData.load_data(
            session=session,
            sqlite_session=sqlite_session,
            from_date=from_date,
            to_date=to_date,
            period="daily"
        )
        data_install = await InstallData.load_data(
            session=sqlite_session,
            from_date=from_date,
            to_date=to_date
        )
        
        # Install dataframe
        install_dataframe = await data_install.dataframe(from_date=from_date, to_date=to_date, group_by="date")

        # App User activity data
        app_dataframe = await asyncio.gather(
            data_revenue.revenue_dataframe(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_read_dataframe(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_coin_dataframe(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_adscoin_dataframe(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_ads_dataframe(from_date=from_date, to_date=to_date, source="app")
        )

        # Web User activity data
        web_dataframe = await asyncio.gather(
            data_revenue.revenue_dataframe(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_read_dataframe(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_coin_dataframe(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_adscoin_dataframe(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_ads_dataframe(from_date=from_date, to_date=to_date, source="web")
        )

        charts = await asyncio.gather(
            dau_mau_chart(from_date=from_date, to_date=to_date, source="web"),
            ga4_mau_dau(session=sqlite_session, from_date=from_date, to_date=to_date, source="app"),
            install_chart(data=install_dataframe),
            user_activity(
                from_date=from_date,
                to_date=to_date,
                chapter_read_data=app_dataframe[1],
                chapter_coin_data=app_dataframe[2],
                chapter_adscoin_data=app_dataframe[3],
                chapter_ads_data=app_dataframe[4],
                source="app"
            ),
            user_activity(
                from_date=from_date,
                to_date=to_date,
                chapter_read_data=web_dataframe[1],
                chapter_coin_data=web_dataframe[2],
                chapter_adscoin_data=web_dataframe[3],
                chapter_ads_data=web_dataframe[4],
                source="web"
            ),
            revenue_cost_periods_chart(
                session=sqlite_session,
                from_date=from_date, 
                to_date=to_date,
                app_coin_data=app_dataframe[0],
                web_coin_data=web_dataframe[0]
            ),
            revenue_cost_chart(),
            payment_channel(app_coin_data=app_dataframe[0], web_coin_data=web_dataframe[0])
        )

        # Compile the final data dictionary
        data = {
            "web_ga4_dau_mau": charts[0],
            "ga4_dau_mau": charts[1],
            "chart_install": charts[2],
            "user_journey_chart": charts[3],
            "web_user_journey_chart": charts[4],
            "revenue_cost_periods": charts[5],
            "revenue_cost_charts": charts[6],
            "payment": charts[7]
        }
        logger.info(f"Data retrieved successfully!")

        return data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise ve

    except KeyError as ke:
        logger.error(f"An error occurred while fetching overview chart data: {ke}")
        raise ke
    
    except Exception as e:
        logger.error(f"An error occurred while fetching overview chart data: {e}")
        raise e

