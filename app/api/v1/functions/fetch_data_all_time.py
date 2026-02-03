import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from app.utils.chapter_all_utils import DataChapter, pembaca_pembeli_chapter_unique, pembaca_chapter_table
from app.utils.revenue_utils import RevenueData, returning_first_purchase, chart_returning_first_purchase
from app.utils.revenue_utils import total_transaksi_coin, persentase_koin_gagal_sukses, revenue_days
from app.utils.revenue_utils import unique_count_users_admob, impression_revenue_chart, revenue_all_chart
pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_data_all_time(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date
):
    """
    Fetch data all time for a specified date range.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.

    Returns:
        Dict[str, Any]: A dictionary containiner data all time.
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
            period="monthly",
            data="all"
        )
        data_revenue = await RevenueData.load_data(
            session=session,
            sqlite_session=sqlite_session,
            from_date=from_date,
            to_date=to_date,
            period="monthly"
        )

        # App Data
        app_coin_data, app_return_first_coin, app_chapter_read_data, app_chapter_ads_data, app_chapter_coin_data, app_chapter_adscoin_data = await asyncio.gather(
            data_revenue.revenue_data(from_date=from_date, to_date=to_date, source="app"),
            returning_first_purchase(session=session, data="value", from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_read(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_ads(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_coin(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_adscoin(from_date=from_date, to_date=to_date, source="app")
        )

        # Web Data
        web_coin_data, web_return_first_coin, web_chapter_read_data, web_chapter_ads_data, web_chapter_coin_data, web_chapter_adscoin_data = await asyncio.gather(
            data_revenue.revenue_data(from_date=from_date, to_date=to_date, source="web"),
            returning_first_purchase(session=session, data="value", from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_read(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_ads(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_coin(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_adscoin(from_date=from_date, to_date=to_date, source="web")
        )
        
        all_revenue_data, app_overall_chapter_purchase_data, web_overall_chapter_purchase_data = await asyncio.gather(
                data_revenue.revenue_data(from_date=from_date, to_date=to_date, source="all"),
                asyncio.to_thread(
                    data_chapter.total_chapter_purchase,
                    chapter_coin_data=app_chapter_coin_data, 
                    chapter_adscoin_data=app_chapter_adscoin_data, 
                    chapter_ads_data=app_chapter_ads_data,
                    metrics_1='chapter_unique',
                    metrics_2='chapter_count'),
                asyncio.to_thread(
                    data_chapter.total_chapter_purchase,
                    chapter_coin_data=web_chapter_coin_data, 
                    chapter_adscoin_data=web_chapter_adscoin_data, 
                    chapter_ads_data=web_chapter_ads_data,
                    metrics_1='chapter_unique',
                    metrics_2='chapter_count')
            )

        # Compile the final data dictionary
        data = {
            "app_pembelian_coin": app_coin_data,
            "app_return_first_coin": app_return_first_coin,
            "app_chapter_read": app_chapter_read_data,
            "app_chapter_coin": app_chapter_coin_data,
            "app_chapter_adscoin": app_chapter_adscoin_data,
            "app_chapter_ads": app_chapter_ads_data,
            "app_total_chapter_purchase": app_overall_chapter_purchase_data,
            "web_pembelian_coin": web_coin_data,
            "web_return_first_coin": web_return_first_coin,
            "web_chapter_read": web_chapter_read_data,
            "web_chapter_coin": web_chapter_coin_data,
            "web_chapter_adscoin": web_chapter_adscoin_data,
            "web_chapter_ads": web_chapter_ads_data,
            "web_total_chapter_purchase": web_overall_chapter_purchase_data,
            "all_revenue_data": all_revenue_data["overall_revenue"]
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
        logger.error(f"An error occurred while fetching data all time: {e}")
        raise
    
 
async def fetch_data_all_time_chart(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date
):
    """
    Fetch data all time chart for a specified date range.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.

    Returns:
        Dict[str, Any]: A dictionary containiner data all time chart.
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
            period="monthly",
            data="all"
        )
        data_revenue = await RevenueData.load_data(
            session=session,
            sqlite_session=sqlite_session,
            from_date=from_date,
            to_date=to_date,
            period="monthly"
        )

        # App Data
        app_coin_data, all_coin_data, app_chapter_read_data, app_chapter_ads_data, app_chapter_coin_data, app_chapter_adscoin_data = await asyncio.gather(
            data_revenue.revenue_dataframe(from_date=from_date, to_date=to_date, source="app"),
            data_revenue.revenue_dataframe(from_date=from_date, to_date=to_date, source="all"),
            data_chapter.chapter_read_dataframe(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_ads_dataframe(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_coin_dataframe(from_date=from_date, to_date=to_date, source="app"),
            data_chapter.chapter_adscoin_dataframe(from_date=from_date, to_date=to_date, source="app")
        )

        # Web Data
        web_coin_data, web_chapter_read_data, web_chapter_ads_data, web_chapter_coin_data, web_chapter_adscoin_data = await asyncio.gather(
            data_revenue.revenue_dataframe(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_read_dataframe(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_ads_dataframe(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_coin_dataframe(from_date=from_date, to_date=to_date, source="web"),
            data_chapter.chapter_adscoin_dataframe(from_date=from_date, to_date=to_date, source="web")
        )

        # chart data
        chapter_chart = await asyncio.gather(
            # App chapter chart
            pembaca_pembeli_chapter_unique(
                chapter_coin_data=app_chapter_coin_data, 
                chapter_adscoin_data=app_chapter_adscoin_data, 
                chapter_ads_data=app_chapter_ads_data, 
                chapter_read_data=app_chapter_read_data,
                period="monthly", 
                data="unique",
                source="app"),
            pembaca_pembeli_chapter_unique(
                chapter_coin_data=app_chapter_coin_data, 
                chapter_adscoin_data=app_chapter_adscoin_data, 
                chapter_ads_data=app_chapter_ads_data, 
                chapter_read_data=app_chapter_read_data,
                period="monthly", 
                data="count",
                source="app"),
            pembaca_chapter_table(
                chapter_coin_data=app_chapter_coin_data, 
                chapter_adscoin_data=app_chapter_adscoin_data, 
                chapter_ads_data=app_chapter_ads_data, 
                chapter_read_data=app_chapter_read_data,
                source="app"
            ),
            # Web chapter chart
            pembaca_pembeli_chapter_unique(
                chapter_coin_data=web_chapter_coin_data, 
                chapter_adscoin_data=web_chapter_adscoin_data, 
                chapter_ads_data=web_chapter_ads_data, 
                chapter_read_data=web_chapter_read_data,
                period="monthly", 
                data="unique",
                source="web"),
            pembaca_pembeli_chapter_unique(
                chapter_coin_data=web_chapter_coin_data, 
                chapter_adscoin_data=web_chapter_adscoin_data, 
                chapter_ads_data=web_chapter_ads_data, 
                chapter_read_data=web_chapter_read_data,
                period="monthly", 
                data="count",
                source="web"),
            pembaca_chapter_table(
                chapter_coin_data=web_chapter_coin_data, 
                chapter_adscoin_data=web_chapter_adscoin_data, 
                chapter_ads_data=web_chapter_ads_data, 
                chapter_read_data=web_chapter_read_data,
                source="web"
            )
        )

        revenue_chart = await asyncio.gather(
            # App revenue chart
            chart_returning_first_purchase(session=session, from_date=from_date, to_date=to_date, source="app", date_format="%Y-%m-01"),
            total_transaksi_coin(to_date=to_date, revenue_data=app_coin_data, period="monthly"),
            persentase_koin_gagal_sukses(revenue_data=app_coin_data, period='all_time'),
            revenue_days(from_date=from_date, to_date=to_date, revenue_data=app_coin_data, chart_types="bar"),
            unique_count_users_admob(revenue_data=app_coin_data, periode="month"),
            impression_revenue_chart(revenue_data=app_coin_data, period="monthly"),
            # Web Revenue_chart 
            chart_returning_first_purchase(session=session, from_date=from_date, to_date=to_date, source="web", date_format="%Y-%m-01"),
            total_transaksi_coin(to_date=to_date, revenue_data=web_coin_data, period="monthly"),
            persentase_koin_gagal_sukses(revenue_data=web_coin_data, period='all_time'),
            revenue_days(from_date=from_date, to_date=to_date, revenue_data=web_coin_data, chart_types="bar"),
            unique_count_users_admob(revenue_data=web_coin_data, periode="month"),
            impression_revenue_chart(revenue_data=web_coin_data, period="monthly"),
            revenue_all_chart(revenue_data=all_coin_data, period="monthly")
        )
        
        # Compile the final data dictionary
        data = {
            "pembaca_pembeli_chapter_unique_chart_all": chapter_chart[0],
            "pembaca_pembeli_chapter_count_chart_all": chapter_chart[1],
            "pembaca_chapter_novel_table": chapter_chart[2],
            "web_pembaca_pembeli_chapter_unique_chart_all": chapter_chart[3],
            "web_pembaca_pembeli_chapter_count_chart_all": chapter_chart[4],
            "web_pembaca_chapter_novel_table": chapter_chart[5],
            "first_ret_purchase": revenue_chart[0],
            "coin_month": revenue_chart[1],
            "persentase_koin_all_time": revenue_chart[2],
            "rev_month": revenue_chart[3],
            "users_unique_count_overall_chart": revenue_chart[4],
            "revenue_chart": revenue_chart[5],
            "web_first_ret_purchase": revenue_chart[6],
            "web_coin_month": revenue_chart[7],
            "web_persentase_koin_all_time": revenue_chart[8],
            "web_rev_month": revenue_chart[9],
            "web_users_unique_count_overall_chart": revenue_chart[10],
            "adsense_revenue_all": revenue_chart[11],
            "all_revenue_chart": revenue_chart[12]
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
        logger.error(f"An error occurred while fetching data all time: {e}")
        raise
       
