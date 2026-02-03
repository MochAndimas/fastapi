import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from app.utils.revenue_utils import RevenueData, returning_first_purchase, dg_returning_first_purchase, total_transaksi_coin
from app.utils.revenue_utils import persentase_koin_gagal_sukses, category_coin, revenue_days, coin_days, transaksi_koin_details
from app.utils.revenue_utils import old_new_user_pembeli_koin_chart, chart_returning_first_purchase, unique_count_users_admob
from app.utils. revenue_utils import impression_revenue_chart, ads_details, frequency_distribution_admob_df, frequency_admob_table
from app.utils.revenue_utils import revenue_all_chart
pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_revenue(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date,
    source: str = "app"
):
    """
    Fetch revenue data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.
        source (str, optional): The source of the data (e.g., "app", "web", or "all"). Defaults to "app".

    Returns:
        Dict[str, Any]: A dictionary containing revenue data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")

        # Initialize data fetchers
        data_revenue = await RevenueData.load_data(
            session=session,
            sqlite_session=sqlite_session,
            from_date=from_date,
            to_date=to_date,
            period="daily"
        )
        
        coin_purchase_data, returning_first_purchase_data = await asyncio.gather(
            data_revenue.revenue_data(from_date=from_date, to_date=to_date, source=source),
            returning_first_purchase(session=session, data="value", from_date=from_date, to_date=to_date, source=source)
        )

        # Compile the final data dictionary
        revenue_data = {
            "coin_data": coin_purchase_data,
            "returning_first_purchase": returning_first_purchase_data
        }
        logger.info(f"Data retrieved successfully for source: {source}")

        return revenue_data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")

    except Exception as e:
        logger.error(f"An error occurred while fetching revenue data: {e}")
        raise


async def fetch_revenue_daily_growth(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date,
    source: str = "app"
):
    """
    Fetch revenue data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.
        source (str, optional): The source of the data (e.g., "app" or "web"). Defaults to "app".

    Returns:
        Dict[str, Any]: A dictionary containing revenue data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")
        # Calculate date range for the previous week
        delta = (to_date - from_date) + timedelta(days=1)
        fromdate_lastweek = from_date - delta

        # Initialize data fetchers
        data_revenue = await RevenueData.load_data(
            session=session,
            sqlite_session=sqlite_session,
            from_date=fromdate_lastweek,
            to_date=to_date,
            period="daily"
        )

        coin_purchase_data , returning_first_purchase_data = await asyncio.gather(
            data_revenue.daily_growth(from_date=from_date, to_date=to_date, source=source),
            dg_returning_first_purchase(session=session, from_date=from_date, to_date=to_date, source=source)
        )

        # Compile the final data dictionary
        revenue_data = {
            "coin_data": coin_purchase_data,
            "returning_first_purchase": returning_first_purchase_data
        }
        logger.info(f"Data retrieved successfully for source: {source}")

        return revenue_data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")

    except Exception as e:
        logger.error(f"An error occurred while fetching revenue data: {e}")
        raise


async def fetch_revenue_chart(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date,
    source: str = "app",
    filters: str = ""
):
    """
    Fetch revenue data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.
        source (str, optional): The source of the data (e.g., "app" or "web"). Defaults to "app".

    Returns:
        Dict[str, Any]: A dictionary containing revenue data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")

        # Initialize data fetchers
        data_revenue = await RevenueData.load_data(
            session=session,
            sqlite_session=sqlite_session,
            from_date=from_date,
            to_date=to_date,
            period="daily"
        )

        revenue_dataframe, frequency_distribution_ads_chart_charts, frequency_admob_table_charts, chart_returning_first_purchase_charts = await asyncio.gather(
            data_revenue.revenue_dataframe(from_date=from_date, to_date=to_date, source=source),
            frequency_distribution_admob_df(session=session, from_date=from_date, to_date=to_date, types="chart", source=source),
            frequency_admob_table(session=session,from_date=from_date, to_date=to_date, source=source),
            chart_returning_first_purchase(session=session, from_date=from_date, to_date=to_date, source=source)
        )

        revenue_chart = await asyncio.gather(
            total_transaksi_coin(to_date=to_date, revenue_data=revenue_dataframe),
            persentase_koin_gagal_sukses(revenue_data=revenue_dataframe, period="daily"),
            category_coin(revenue_data=revenue_dataframe),
            revenue_days(from_date=from_date, to_date=to_date, revenue_data=revenue_dataframe),
            coin_days(from_date=from_date, to_date=to_date, revenue_data=revenue_dataframe),
            transaksi_koin_details(revenue_data=revenue_dataframe, filters=filters),
            old_new_user_pembeli_koin_chart(from_date=from_date, to_date=to_date, revenue_data=revenue_dataframe),
            unique_count_users_admob(revenue_data=revenue_dataframe, periode="daily"),
            impression_revenue_chart(revenue_data=revenue_dataframe),
            ads_details(revenue_data=revenue_dataframe, source=source),
            revenue_all_chart(revenue_data=revenue_dataframe, period="daily")
        )

        # Compile the final data dictionary
        data = {
            "total_transaksi_coin": revenue_chart[0],
            "persentase_coin": revenue_chart[1],
            "category_coin": revenue_chart[2],
            "revenue_days": revenue_chart[3],
            "coin_days": revenue_chart[4],
            "coin_details": revenue_chart[5],
            "old_new_user_pembeli_koin": revenue_chart[6],
            "chart_returning_first_purchase": chart_returning_first_purchase_charts,
            "unique_count_users_admob": revenue_chart[7],
            "impression_revenue_chart": revenue_chart[8],
            "ads_details": revenue_chart[9],
            "frequency_distribution_ads_chart": frequency_distribution_ads_chart_charts,
            "frequency_admob_table": frequency_admob_table_charts,
            "overall_revenue_chart": revenue_chart[10]
        }
        logger.info(f"Data retrieved successfully for source: {source}")

        return data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")

    except Exception as e:
        logger.error(f"An error occurred while fetching revenue data: {e}")
        raise

