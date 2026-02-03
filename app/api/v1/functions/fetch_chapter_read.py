import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta

from app.utils.chapter_all_utils import DataChapter
from app.utils.chapter_read_utils import chapter_read_frequency, chapter_read_old_new
from app.utils.chapter_read_utils import chapter_read_unique_count_chart, chapter_read_per_day
from app.utils.chapter_read_utils import chapter_read_category, chapter_read_table
from app.utils.chapter_read_utils import chapter_read_frequency
pd.options.mode.copy_on_write = True


# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_chapter_read(
    session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date,
    sort_by: bool = False,
    ascendings: bool = False,
    source: str = "app",
    read_is_completed: list = [True, False]
):
    """
    Fetch chapter read data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.
        source (str, optional): The source of the data (e.g., "app" or "web"). Defaults to "app".

    Returns:
        Dict[str, Any]: A dictionary containing chapter read data.
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
            data="chapter_read",
            read_is_completed=read_is_completed
        )

        chapter_read, chapter_read_dataframe, daily_growth_chapter_read = await asyncio.gather(
            data_chapter.chapter_read(from_date=from_date, to_date=to_date, source=source),
            data_chapter.chapter_read_dataframe(from_date=from_date, to_date=to_date, source=source),
            data_chapter.daily_growth(from_date=from_date, to_date=to_date, source=source, metrics=["chapter_read_data"])
        )
        
        charts = await asyncio.gather(
            chapter_read_frequency(session=session, from_date=from_date, to_date=to_date, chart_types="chart", read_is_completed=read_is_completed),
            chapter_read_frequency(session=session, from_date=from_date, to_date=to_date, chart_types="table", read_is_completed=read_is_completed),
            chapter_read_old_new(chapter_read_dataframe),
            chapter_read_unique_count_chart(chapter_read_dataframe),
            chapter_read_per_day(chapter_read_dataframe),
            chapter_read_category(chapter_read_dataframe),
            chapter_read_table(chapter_read_dataframe, sort_by=sort_by, ascending=ascendings)
        )

        data = {
            "chapter_read_data": chapter_read,
            "daily_growth": daily_growth_chapter_read,
            "frequency_chart": charts[0],
            "frequency_table": charts[1],
            "chart_old_new": charts[2],
            "chart_unique_count": charts[3],
            "chart_day": charts[4],
            "chart_category": charts[5],
            "chart_table": charts[6]
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
        logger.error(f"An error occurred while fetching chapter read data: {e}")
        raise

