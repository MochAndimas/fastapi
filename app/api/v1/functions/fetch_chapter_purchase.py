import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta

from app.utils.chapter_all_utils import DataChapter
from app.utils.chapter_purchase_utils import daily_growth, chapter_old_new, chapter_unique_count_chart
from app.utils.chapter_purchase_utils import chapter_coin_per_day, chapter_coin_category, chapter_table
pd.options.mode.copy_on_write = True


# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_chapter_types(
    session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date,
    chapter_types: str = "chapter_coin",
    sort_by: bool = False,
    ascendings: bool = False,
    source: str = "app"
):
    """
    Fetch chapter types data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.
        source (str, optional): The source of the data (e.g., "app" or "web"). Defaults to "app".

    Returns:
        Dict[str, Any]: A dictionary containing chapter types data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")
        
        # Calculate date range for the previous week
        delta = (to_date - from_date) + timedelta(days=1)
        fromdate_lastweek = from_date - delta
        todate_lastweek = to_date - delta
        
        # Initialize data fetchers
        data_chapter = await DataChapter.load_data(
            session=session,
            from_date=from_date,
            to_date=to_date,
            period="daily",
            data=chapter_types
        )

        if chapter_types == "chapter_coin":
            dataframe, curren_data, last_week_data = await asyncio.gather(
                data_chapter.chapter_coin_dataframe(from_date=from_date, to_date=to_date, source=source),
                data_chapter.chapter_coin(from_date=from_date, to_date=to_date, source=source),
                data_chapter.chapter_coin(from_date=fromdate_lastweek, to_date=todate_lastweek)
            )
        
        elif chapter_types == "chapter_adscoin":
            dataframe, curren_data, last_week_data = await asyncio.gather(
                data_chapter.chapter_adscoin_dataframe(from_date=from_date, to_date=to_date, source=source),
                data_chapter.chapter_adscoin(from_date=from_date, to_date=to_date, source=source),
                data_chapter.chapter_adscoin(from_date=fromdate_lastweek, to_date=todate_lastweek)
            )
        
        elif chapter_types == "chapter_ads":
            dataframe, curren_data, last_week_data = await asyncio.gather(
                data_chapter.chapter_ads_dataframe(from_date=from_date, to_date=to_date, source=source),
                data_chapter.chapter_ads(from_date=from_date, to_date=to_date, source=source),
                data_chapter.chapter_ads(from_date=fromdate_lastweek, to_date=todate_lastweek)
            )

        daily_growth_data = await asyncio.to_thread(daily_growth, curren_data, last_week_data)
        charts = await asyncio.gather(
            chapter_old_new(data=dataframe, chapter_types=chapter_types),
            chapter_unique_count_chart(data=dataframe, chapter_types=chapter_types),
            chapter_coin_per_day(data=dataframe),
            chapter_coin_category(data=dataframe),
            chapter_table(data=dataframe, chapter_types=chapter_types, sort_by=sort_by, ascending=ascendings)
        )

        data = {
            "chapter_data": curren_data,
            "data_daily_growth": daily_growth_data,
            "old_new_chart": charts[0],
            "unique_count_chart": charts[1],
            "chapter_by_day_chart": charts[2],
            "chapter_by_category": charts[3],
            "chapter_table": charts[4]
        }

        logger.info(f"Data retrieved successfully for source: {source}")

        return data

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")
        raise

    except Exception as e:
        logger.error(f"An error occurred while fetching chapter types data: {e}")
        raise

