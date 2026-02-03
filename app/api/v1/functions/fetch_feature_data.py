import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from app.utils.feature_data_utils import RedeemCode, TransactionIllustration, GoogleEventData

pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_redeem_code(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
    codes: str = "", 
    user_type: str = ""
):
    """
    Fetch user redeem code data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.
        codes (str, optional): Voucher code to filter by (default is "").
        user_type (str, optional): User type to filter by (default is "").

    Returns:
        Dict[str, Any]: A dictionary containing redeem code data.
    """
    try:
        delta = (to_date - from_date) + timedelta(1)
        fromdate_lastweek = from_date - delta
        # Initialize data fetchers
        redeem_data = await RedeemCode.laod_data(
            session=session, 
            from_date=fromdate_lastweek, 
            to_date=to_date)

        metrics_data, metrics_daily_growth, redeemed_table, codes_table = await asyncio.gather(
            redeem_data.redeemed_details(types="redeemed_code", from_date=from_date, to_date=to_date, codes=codes, user_type=user_type),
            redeem_data.daily_growth(types="redeemed_code", from_date=from_date, to_date=to_date, codes=codes, user_type=user_type),
            redeem_data.chart_table(types="redeemed_code", from_date=from_date, to_date=to_date, codes=codes, user_type=user_type),
            redeem_data.chart_table(types="codes")
        )

        container = {
            "metrics_data" :metrics_data,
            "metrics_daily_growth": metrics_daily_growth,
            "redeemed_table": redeemed_table,
            "codes_table": codes_table
        }
        logger.info(f"Data retrieved successfully!")

        return container

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")
        raise

    except Exception as e:
        logger.error(f"An error occurred while fetching user redeem code data: {e}")
        raise


async def fetch_illustration_transaction(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
    source: str = '', 
    novel_title: str = '', 
    illustration_id: int = 0
):
    """
    Fetch user Illustration transaction data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.
        source: (Optional) Filter by source name.
        novel_title: (Optional) Filter by novel title.
        illustration_id: (Optional) Filter by illustration ID.

    Returns:
        Dict[str, Any]: A dictionary containing illustration transaction data.
    """
    try:
        delta = (to_date - from_date) + timedelta(1)
        fromdate_lastweek = from_date - delta
        # Initialize data fetchers
        illustration_data = await TransactionIllustration.laod_data(
            session=session, 
            from_date=fromdate_lastweek, 
            to_date=to_date)

        metrics_data, metrics_daily_growth, transaction_table, illustration_table = await asyncio.gather(
            illustration_data.transaction_details(types="illustration_details", data="metrics", from_date=from_date, to_date=to_date, source=source, novel_title=novel_title, illustration_id=illustration_id),
            illustration_data.daily_growth(types="illustration_details", from_date=from_date, to_date=to_date, source=source, novel_title=novel_title, illustration_id=illustration_id),
            illustration_data.table_chart(types="illustration_details", from_date=from_date, to_date=to_date, source=source, novel_title=novel_title, illustration_id=illustration_id),
            illustration_data.table_chart(types="illustration")
        )

        container = {
            "metrics_data" :metrics_data,
            "metrics_daily_growth": metrics_daily_growth,
            "transaction_table": transaction_table,
            "illustration_table": illustration_table
        }
        logger.info(f"Data retrieved successfully!")

        return container

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")
        raise

    except Exception as e:
        logger.error(f"An error occurred while fetching user illustration transaction data: {e}")
        raise


async def fetch_offline_mode(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date
):
    """
    Fetch user Offline Mode data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing ofline mode data.
    """
    try:
        delta = (to_date - from_date) + timedelta(1)
        fromdate_lastweek = from_date - delta
        
        # Initialize data fetchers
        offline_data = await GoogleEventData.laod_data(
            session=session, 
            from_date=fromdate_lastweek, 
            to_date=to_date)

        metrics_data, metrics_daily_growth, unique_bar_chart, count_bar_chart = await asyncio.gather(
            offline_data.get_data(data="metrics", from_date=from_date, to_date=to_date),
            offline_data.daily_growth(from_date=from_date, to_date=to_date),
            offline_data.bar_chart(from_date=from_date, to_date=to_date, types="Unique"),
            offline_data.bar_chart(from_date=from_date, to_date=to_date, types="Count")
        )

        container = {
            "metrics_data" :metrics_data,
            "metrics_daily_growth": metrics_daily_growth,
            "unique_bar_chart": unique_bar_chart,
            "count_bar_chart": count_bar_chart
        }
        logger.info(f"Data retrieved successfully!")

        return container

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")
        raise

    except Exception as e:
        logger.error(f"An error occurred while fetching user offline mode data: {e}")
        raise

