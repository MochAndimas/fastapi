import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from app.utils.novel_utils import novel_table, NovelDetails
pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_novel(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
    novel_title: str = "",
    category_novel: str = "",
    sort_by: str = "reader_purchase_percentage",
    ascending: bool = True
):
    """
    Fetch novel data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQL Database session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.
        novel_title (str, optional): Filter by novel title (case-insensitive). Defaults to ''.
        category_novel (str, optional): Filter by novel category (case-insensitive). Defaults to ''.
        sort_by (str, optional): Column to sort by. Defaults to 'presentase_pembaca_ke_pembeli'.
        ascending (bool, optional): Whether to sort in ascending order. Defaults to True.

    Returns:
        Dict[str, Any]: A dictionary containing novel data.
    """
    try:
        # Initialize data fetchers
        df_novel = await novel_table(
            session=session, 
            from_date=from_date, 
            to_date=to_date, 
            novel_title=novel_title, 
            category_novel=category_novel, 
            sort_by=sort_by, 
            ascending=ascending)
        container = {
            "novel_table": df_novel
        }
        logger.info(f"Data retrieved successfully!")

        return container

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")

    except Exception as e:
        logger.error(f"An error occurred while fetching novel details chart data: {e}")
        raise


async def fetch_novel_details(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
    novel_title: str = ""
):
    """
    Fetch novel details data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQL Database session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.
        novel_title (str, optional): Filter by novel title (case-insensitive). Defaults to ''.

    Returns:
        Dict[str, Any]: A dictionary containing novel details data.
    """
    try:
        # Initialize data fetchers
        novel_details = await NovelDetails.laod_data(
            session=session, 
            from_date=from_date, 
            to_date=to_date, 
            novel_title=novel_title)
        
        container = await novel_details.novel_details()
        logger.info(f"Data retrieved successfully!")

        return container

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")

    except Exception as e:
        logger.error(f"An error occurred while fetching novel details chart data: {e}")
        raise


async def fetch_novel_details_chart(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
    novel_title: str = ""
):
    """
    Fetch novel details chart data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQL Database session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.
        novel_title (str, optional): Filter by novel title (case-insensitive). Defaults to ''.

    Returns:
        Dict[str, Any]: A dictionary containing novel details chart data.
    """
    try:
        # Initialize data fetchers
        novel_details = await NovelDetails.laod_data(
            session=session, 
            from_date=from_date, 
            to_date=to_date, 
            novel_title=novel_title)

        chart_data = await asyncio.gather(
            novel_details.user_pembeli_chapter(types="reader"),
            novel_details.user_pembeli_chapter(types="coin"),
            novel_details.user_pembeli_chapter(types="ads-coin"),
            novel_details.user_pembeli_chapter(types="ads"),
            novel_details.frequency_dataframe(data="table"),
            novel_details.frequency_dataframe(data="chart")
        )
        
        container = {
            "user_table_pembaca": chart_data[0],
            "user_table_chapter_coin": chart_data[1],
            "user_table_chapter_adscoin": chart_data[2],
            "user_table_chapter_ads": chart_data[3],
            "frequency_table": chart_data[4],
            "frequency_chart": chart_data[5],
        }
        logger.info(f"Data retrieved successfully!")

        return container

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")

    except Exception as e:
        logger.error(f"An error occurred while fetching novel details chart data: {e}")
        raise
