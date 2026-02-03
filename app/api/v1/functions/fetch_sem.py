import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from app.utils.sem_utils import df_file, dg_sem_awareness, spend_chart
from app.utils.sem_utils import metrics_chart, details_table

pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_sem(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
):
    """
    Fetch SEM data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing SEM data.
    """
    try:
        # Initialize data fetchers
        metrics_sem = await asyncio.gather(
            df_file(session=session, from_date=from_date, to_date=to_date, filename="googleads", file="sem", data="metrics"),
            df_file(session=session, from_date=from_date, to_date=to_date, filename="googleads", file="GDN", data="metrics"),
            df_file(session=session, from_date=from_date, to_date=to_date, filename="facebookads", file="awareness", data="metrics")
        )

        container = {
            "google_sem": metrics_sem[0],
            "google_gdn": metrics_sem[1],
            "facebook_gdn": metrics_sem[2]
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
        logger.error(f"An error occurred while fetching SEM data: {e}")
        raise


async def fetch_sem_daily_growth(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
):
    """
    Fetch SEM daily growth data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing SEM daily growth data.
    """
    try:
        # Initialize data fetchers
        metrics_sem = await asyncio.gather(
            dg_sem_awareness(session=session, from_date=from_date, to_date=to_date, filename="googleads", file="sem"),
            dg_sem_awareness(session=session, from_date=from_date, to_date=to_date, filename="googleads", file="GDN"),
            dg_sem_awareness(session=session, from_date=from_date, to_date=to_date, filename="facebookads", file="awareness")
        )

        container = {
            "google_sem": metrics_sem[0],
            "google_gdn": metrics_sem[1],
            "facebook_gdn": metrics_sem[2]
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
        logger.error(f"An error occurred while fetching SEM data: {e}")
        raise


async def fetch_sem_chart(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
):
    """
    Fetch SEM chart data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing SEM chart data.
    """
    try:
        # Initialize data fetchers
        metrics_dataframe = await asyncio.gather(
            df_file(session=session, from_date=from_date, to_date=to_date, filename="googleads", file="sem", data="dataframe"),
            df_file(session=session, from_date=from_date, to_date=to_date, filename="googleads", file="GDN", data="dataframe"),
            df_file(session=session, from_date=from_date, to_date=to_date, filename="facebookads", file="awareness", data="dataframe")
        )

        chart_data = await asyncio.gather(
            # Google SEM chart
            spend_chart(dataframe=metrics_dataframe[0], file="sem", source="google"),
            metrics_chart(dataframe=metrics_dataframe[0], file="sem", source="google"),
            details_table(dataframe=metrics_dataframe[0], file="sem", source="google"),
            # Google GDN chart
            spend_chart(dataframe=metrics_dataframe[1], file="GDN", source="google"),
            metrics_chart(dataframe=metrics_dataframe[1], file="GDN", source="google"),
            details_table(dataframe=metrics_dataframe[1], file="GDN", source="google"),
            # Facebook GDN chart
            spend_chart(dataframe=metrics_dataframe[2], file="GDN", source="facebook"),
            metrics_chart(dataframe=metrics_dataframe[2], file="GDN", source="facebook"),
            details_table(dataframe=metrics_dataframe[2], file="GDN", source="facebook"),
        )

        container = {
            "google_sem_spend_chart": chart_data[0],
            "google_sem_metrics_chart": chart_data[1],
            "google_sem_details_table": chart_data[2],
            "google_gdn_spend_chart": chart_data[3],
            "google_gdn_metrics_chart": chart_data[4],
            "google_gdn_details_table": chart_data[5],
            "facebook_gdn_spend_chart": chart_data[6],
            "facebook_gdn_metrics_chart": chart_data[7],
            "facebook_gdn_details_table": chart_data[8],
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
        logger.error(f"An error occurred while fetching SEM data: {e}")
        raise
