import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from app.utils.new_install_utils import InstallData, AcquisitionData
from app.utils.new_install_utils import ads_install_chart, cost_install_chart, campaign_details_table

pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_new_install(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
):
    """
    Fetch new install data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing new install data.
    """
    try:
        # Initialize data fetchers
        data_install = await InstallData.load_data(
            session=session, 
            from_date=from_date, 
            to_date=to_date)
        campaign_data = await AcquisitionData.load_data(
            session=session, 
            from_date=from_date, 
            to_date=to_date)

        metrics = await asyncio.gather(
            data_install.overall_install(from_date=from_date, to_date=to_date),
            campaign_data.metrics(data="google", from_date=from_date, to_date=to_date),
            campaign_data.metrics(data="facebook", from_date=from_date, to_date=to_date),
            campaign_data.metrics(data="tiktok", from_date=from_date, to_date=to_date),
            campaign_data.metrics(data="asa", from_date=from_date, to_date=to_date)
        )

        container = {
            "install_all": metrics[0],
            "google_performance": metrics[1],
            "facebook_performance": metrics[2],
            "tiktok_performance": metrics[3],
            "asa_performance": metrics[4],
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
        logger.error(f"An error occurred while fetching new install chart data: {e}")
        raise


async def fetch_new_install_daily_growth(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
):
    """
    Fetch new install daily growth data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing new install daily growth data.
    """
    try:
        # Calculate date range for the previous week
        delta = (to_date - from_date) + timedelta(days=1)
        fromdate_lastweek = from_date - delta
        # Initialize data fetchers
        data_install = await InstallData.load_data(
            session=session, 
            from_date=fromdate_lastweek, 
            to_date=to_date)
        campaign_data = await AcquisitionData.load_data(
            session=session, 
            from_date=fromdate_lastweek, 
            to_date=to_date)

        metrics = await asyncio.gather(
            data_install.daily_growth(from_date=from_date, to_date=to_date),
            campaign_data.daily_growth(data="google", from_date=from_date, to_date=to_date),
            campaign_data.daily_growth(data="facebook", from_date=from_date, to_date=to_date),
            campaign_data.daily_growth(data="tiktok", from_date=from_date, to_date=to_date),
            campaign_data.daily_growth(data="asa", from_date=from_date, to_date=to_date)
        )

        container = {
            "install_all": metrics[0],
            "google_performance": metrics[1],
            "facebook_performance": metrics[2],
            "tiktok_performance": metrics[3],
            "asa_performance": metrics[4],
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
        logger.error(f"An error occurred while fetching new install chart data: {e}")
        raise


async def fetch_new_install_chart(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
):
    """
    Fetch new install chart data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing new install chart data.
    """
    try:
        # Initialize data fetchers
        data_install = await InstallData.load_data(
            session=session, 
            from_date=from_date, 
            to_date=to_date)
        campaign_data = await AcquisitionData.load_data(
            session=session, 
            from_date=from_date, 
            to_date=to_date)

        metrics = await asyncio.gather(
            data_install.install_source_chart(from_date=from_date, to_date=to_date),
            data_install.install_source_table(from_date=from_date, to_date=to_date),
            data_install.aso_chart(from_date=from_date, to_date=to_date),
            campaign_data.dataframe(data="google", from_date=from_date, to_date=to_date),
            campaign_data.dataframe(data="facebook", from_date=from_date, to_date=to_date),
            campaign_data.dataframe(data="tiktok", from_date=from_date, to_date=to_date),
            campaign_data.dataframe(data="asa", from_date=from_date, to_date=to_date)
        )

        metrics_chart = await asyncio.gather(
            # facebook chart
            cost_install_chart(data=metrics[4]),
            campaign_details_table(data=metrics[4]),
            ads_install_chart(data=metrics[4]),
            # google chart
            cost_install_chart(data=metrics[3]),
            campaign_details_table(data=metrics[3]),
            ads_install_chart(data=metrics[3]),
            # tiktok chart
            cost_install_chart(data=metrics[5]),
            ads_install_chart(data=metrics[5]),
            campaign_details_table(data=metrics[5]),
            # Apple search ads chart
            cost_install_chart(data=metrics[6]),
            ads_install_chart(data=metrics[6]),
            campaign_details_table(data=metrics[6]),
        )

        container = {
            'source_chart': metrics[0],
            'source_table': metrics[1],
            'chart_aso': metrics[2],
            'fb_chart': metrics_chart[0],
            'fb_table': metrics_chart[1],
            'fb_install_chart': metrics_chart[2],
            'ggl_chart': metrics_chart[3],
            'ggl_table': metrics_chart[4],
            'ggl_install_chart': metrics_chart[5],
            'chart_tiktok_cost_installs': metrics_chart[6],
            'chart_tiktok_installs': metrics_chart[7],
            'table_tiktok_campaign': metrics_chart[8],
            'chart_asa_cost_install': metrics_chart[9],
            'chart_asa_install': metrics_chart[10],
            'table_asa': metrics_chart[11]
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
        logger.error(f"An error occurred while fetching new install chart data: {e}")
        raise

