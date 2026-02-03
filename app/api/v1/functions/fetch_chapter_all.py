import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from app.utils.chapter_all_utils import DataChapter, pembaca_pembeli_chapter_unique, chart_total_chapter_purchase
from app.utils.chapter_all_utils import old_new_user_pembaca_chapter, old_new_user_pembeli_chapter, pembaca_chapter_by_day
from app.utils.chapter_all_utils import pembeli_chapter_by_day, pembaca_chapter_by_genre, pembeli_chapter_by_genre, pembaca_chapter_table
pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_chapter_all(
    session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date,
    source: str = "app"
):
    """
    Fetch chapter all data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.
        source (str, optional): The source of the data (e.g., "app" or "web"). Defaults to "app".

    Returns:
        Dict[str, Any]: A dictionary containing chapter all data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")

        # Initialize data fetchers
        data_chapter = await DataChapter.load_data(session=session, from_date=from_date, to_date=to_date, period="daily", data="all")

        chapter_read_data, chapter_ads_data, chapter_coin_data, chapter_adscoin_data = await asyncio.gather(
            data_chapter.chapter_read(from_date=from_date, to_date=to_date, source=source),
            data_chapter.chapter_ads(from_date=from_date, to_date=to_date, source=source),
            data_chapter.chapter_coin(from_date=from_date, to_date=to_date, source=source),
            data_chapter.chapter_adscoin(from_date=from_date, to_date=to_date, source=source)
        )
        
        overall_chapter_purchase_data , overall_oldnew_chapter_purchase_data = await asyncio.gather(
            asyncio.to_thread(
                data_chapter.total_chapter_purchase,
                chapter_coin_data=chapter_coin_data, 
                chapter_adscoin_data=chapter_adscoin_data, 
                chapter_ads_data=chapter_ads_data,
                metrics_1='chapter_unique',
                metrics_2='chapter_count'),
            asyncio.to_thread(
                data_chapter.total_chapter_purchase,
                chapter_coin_data=chapter_coin_data, 
                chapter_adscoin_data=chapter_adscoin_data, 
                chapter_ads_data=chapter_ads_data,
                metrics_1='old_user_count', 
                metrics_2='new_user_count')
            )

        # Compile the final data dictionary
        chapter_data = {
            "chapter_read_data": chapter_read_data,
            "chapter_ads_data": chapter_ads_data,
            "chapter_coin_data": chapter_coin_data,
            "chapter_adscoin_data": chapter_adscoin_data,
            "overall_chapter_purchase": overall_chapter_purchase_data,
            "overall_oldnew_chapter_purchase": overall_oldnew_chapter_purchase_data
        }
        logger.info(f"Data retrieved successfully for source: {source}")
        
        return chapter_data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")
        raise

    except Exception as e:
        logger.error(f"An error occurred while fetching chapter all data: {e}")
        raise


async def fetch_chapter_daily_growth(
    session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date,
    source: str = "app"
):
    """
    Fetch chapter all data daily growth for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.
        source (str, optional): The source of the data (e.g., "app" or "web"). Defaults to "app".

    Returns:
        Dict[str, Any]: A dictionary containing chapter all data.
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
            data="all")

        daily_growth_data = await data_chapter.daily_growth(from_date=from_date, to_date=to_date, source=source)
    
        logger.info(f"Data retrieved successfully for source: {source}")

        return daily_growth_data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")
        raise

    except Exception as e:
        logger.error(f"An error occurred while fetching chapter all data: {e}")
        raise


async def fetch_chapter_all_chart(
    session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date,
    sort_by: bool = False,
    ascendings: bool = False,
    source: str = "app"
):
    """
    Fetch chapter all data chart for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.
        source (str, optional): The source of the data (e.g., "app" or "web"). Defaults to "app".

    Returns:
        Dict[str, Any]: A dictionary containing chapter all data.
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
            data="all")

        chapter_read_data, chapter_ads_data, chapter_coin_data, chapter_adscoin_data = await asyncio.gather(
            data_chapter.chapter_read_dataframe(from_date=from_date, to_date=to_date, source=source),
            data_chapter.chapter_ads_dataframe(from_date=from_date, to_date=to_date, source=source),
            data_chapter.chapter_coin_dataframe(from_date=from_date, to_date=to_date, source=source),
            data_chapter.chapter_adscoin_dataframe(from_date=from_date, to_date=to_date, source=source)
        )

        data = await asyncio.gather(
            pembaca_pembeli_chapter_unique(
                chapter_read_data=chapter_read_data,
                chapter_coin_data=chapter_coin_data,
                chapter_adscoin_data=chapter_adscoin_data,
                chapter_ads_data=chapter_ads_data,
                data="unique",
                source=source
            ),
            pembaca_pembeli_chapter_unique(
                chapter_read_data=chapter_read_data,
                chapter_coin_data=chapter_coin_data,
                chapter_adscoin_data=chapter_adscoin_data,
                chapter_ads_data=chapter_ads_data,
                data="count",
                source=source
            ),
            chart_total_chapter_purchase(
                chapter_coin_data=chapter_coin_data, 
                chapter_adscoin_data=chapter_adscoin_data, 
                chapter_ads_data=chapter_ads_data, 
                metrics_1='old_user_count', 
                metrics_2='new_user_count'),
            chart_total_chapter_purchase(chapter_read_data=chapter_read_data, metrics_1='chapter_read'),
            old_new_user_pembaca_chapter(chapter_read_data=chapter_read_data), 
            old_new_user_pembeli_chapter(
                chapter_coin_data=chapter_coin_data, 
                chapter_adscoin_data=chapter_adscoin_data, 
                chapter_ads_data=chapter_ads_data),
            pembaca_chapter_by_day(chapter_read_data=chapter_read_data),
            pembeli_chapter_by_day(
                chapter_coin_data=chapter_coin_data,
                chapter_adscoin_data=chapter_adscoin_data,
                chapter_ads_data=chapter_ads_data
            ),
            pembaca_chapter_by_genre(chapter_read_data=chapter_read_data),
            pembeli_chapter_by_genre(
                chapter_coin_data=chapter_coin_data,
                chapter_adscoin_data=chapter_adscoin_data,
                chapter_ads_data=chapter_ads_data
            ),
            pembaca_chapter_table(
                chapter_coin_data=chapter_coin_data,
                chapter_adscoin_data=chapter_adscoin_data,
                chapter_ads_data=chapter_ads_data,
                chapter_read_data=chapter_read_data,
                sort_by=sort_by,
                ascending=ascendings,
                source=source
            )
        )

        # Compile the final data dictionary
        chart_data = {
            "pembaca_pembeli_chapter_unique_chart": data[0],
            "pembaca_pembeli_chapter_count_chart": data[1],
            "pie_chart_old_new_chapter_purchase": data[2],
            "pie_chart_old_new_chapter_read": data[3],
            "pembaca_old_new_chart": data[4],
            "pembeli_old_new_chart": data[5],
            "pembaca_chapter_day": data[6],
            "pembeli_chapter_day": data[7],
            "pembaca_chapter_genre": data[8],
            "pembeli_chapter_genre": data[9],
            "pembaca_chapter_novel_table": data[10]
        }
        logger.info(f"Data retrieved successfully for source: {source}")

        return chart_data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")
        raise

    except Exception as e:
        logger.error(f"An error occurred while fetching chapter all data: {e}")
        raise

