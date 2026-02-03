import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils.retention_utils import get_date_range, retention_chart, cohort_table

pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_retention(
    session: AsyncSession,
    event_name: str,
    data: str,
    period: str, 
    preset_date: str,
    source: str
):
    """
    Fetch user retention data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        event_name (str): The name of the event (
            - 'User Read Chapter'
            - 'User Buy Chapter With Coin'
            - 'User Buy Chpater With AdsCoin'
            - 'User Buy Chapter With Ads'
            - 'User Buy Coin' default is an empty string.
        )
        data (str): The data types to fetch ('presentase' or 'total_user').
        period (str): The period for grouping the event data, e.g., '%Y-%m-01' for Monthly, '%Y-%m-%d' for Daily.
        preset_date(str): The Date preset data to fetch (
            - 'last_7_days'
            - 'last_14_days'
            - 'last_28_days'
            - 'last_3_months'
            - 'last_6_months'
            - 'last_12_months'
        )
        source (str): The specific data source ('all', 'app', 'web'), default is 'all'.

    Returns:
        Dict[str, Any]: A dictionary containing retention data.
    """
    try:
        # Define mappings for form selections
        event_name_mapping = {
            'user_read_chapter': 'User Read Chapter',
            'user_buy_chapter_coin': 'User Buy Chapter With Coin',
            'user_buy_chapter_adscoin': 'User Buy Chapter With AdsCoin',
            'user_buy_chapter_ads': 'User Buy Chapter With Ads',
            'user_coin_purchase': 'User Buy Coin'
        }

        data_type_mapping = {
            'persentase': 'float',
            'total_user': 'int'
        }

        # Update event_name based on form data
        event_name_form = event_name_mapping.get(event_name, 'User Read Chapter')

        # Update data type based on form data
        data_form = data_type_mapping.get(data, 'float')

        # Handle Daily period
        if period == 'Daily':
            period_form = "%Y-%m-%d"
            preset_days_mapping = {
                'last_7_days': 7,
                'last_14_days': 14,
                'last_28_days': 28
            }
            # Handle preset date selection
            if preset_date in preset_days_mapping:
                preset_days = preset_days_mapping[preset_date]
                from_date, to_date = get_date_range(preset_days)
            else:
                from_date, to_date = get_date_range(7)
        # Handle Monthly period
        elif period == 'Monthly':
            period_form = "%Y-%m-01"
            preset_days_mapping = {
                'last_3_months': 3,
                'last_6_months': 6,
                'last_12_months': 12
            }
            # Handle preset date selection
            if preset_date in preset_days_mapping:
                preset_days = preset_days_mapping[preset_date]
                from_date, to_date = get_date_range(days=0, period='months', months=preset_days)
            else:
                from_date, to_date = get_date_range(days=0, period='months', months=3)
                
        # Initialize data fetchers
        retention_charts, table_cohort = await asyncio.gather(
            retention_chart(
                session=session, 
                from_date=from_date, 
                to_date=to_date, 
                period=period_form, 
                event_name=event_name_form, 
                data=data_form,
                source=source),
            cohort_table(
                session=session, 
                from_date=from_date, 
                to_date=to_date, 
                period=period_form, 
                event_name=event_name_form, 
                data=data_form,
                source=source
            )
        )

        container = {
            "retention_charts": retention_charts,
            "table_cohort": table_cohort
        }
        logger.info(f"Data retrieved successfully for source: {source}")

        return container

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise

    except KeyError as ke:
        logger.error(f"Validation error: {ke}")

    except Exception as e:
        logger.error(f"An error occurred while fetching retention data: {e}")
        raise
