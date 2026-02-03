import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from app.utils.user_activity_utils import heatmap_table, ga4_session

pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_user_activity(
    session: AsyncSession,
    sqlite_session: AsyncSession,
    from_date: datetime.date,
    to_date: datetime.date,
    source: str = "app",
    types: str = "hour"
):
    """
    Fetch user activity data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        sqlite_session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for fetching data.
        to_date (datetime.date): The end date for fetching data.
        source (str, optional): The source of the data (e.g., "app" or "web"). Defaults to "app".

    Returns:
        Dict[str, Any]: A dictionary containing user activity data.
    """
    try:
        # Validate date range
        if from_date > to_date:
            raise ValueError("from_date cannot be greater than to_date")

        # Initialize data fetchers
        chart_activity_time, session_chart = await asyncio.gather(
            heatmap_table(session=session, from_date=from_date, to_date=to_date, source=source, types=types),
            ga4_session(sqlite_session=sqlite_session, from_date=from_date, to_date=to_date, source=source)
        )
        
        data = {
            "chart_activity_time": chart_activity_time,
            "session_chart": session_chart
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
        logger.error(f"An error occurred while fetching user activity data: {e}")
        raise
