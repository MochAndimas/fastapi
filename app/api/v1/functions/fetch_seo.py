import pandas as pd
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from app.utils.seo_utils import range_of_date, ga4_metrics, indexing_df, ranking_df
from app.utils.seo_utils import dg_ga4_metrics, dg_indexing, ga4_metrics_chart
from app.utils.seo_utils import ga4_source_chart, ga4_device_chart, landing_page_tbl
from app.utils.seo_utils import ranking_chart, web_traffic_table

pd.options.mode.copy_on_write = True

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_seo(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
):
    """
    Fetch SEO data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing SEO data.
    """
    try:
        # Initialize data fetchers
        range_date = range_of_date(7)
        metrics_ga4 = await asyncio.gather(
            ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='sessions'),
            ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='source'),
            ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='total_user'),
            ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='new_user'),
            ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='bounce_rate')
        )

        df_index, df_ranking = await asyncio.gather(
            indexing_df(),
            ranking_df()
        )

        container = {
            "sessions": metrics_ga4[0],
            "source": metrics_ga4[1],
            "total_user": metrics_ga4[2],
            "new_user": metrics_ga4[3],
            "bounce_rate": metrics_ga4[4],
            "periode": df_index['week'][-1:].item(),
            "dr": df_index['DR'][-1:].item(),
            "indexing": df_index['Indexing'][-1:].item(),
            "backlinks": df_index['Backlinks'][-1:].item(),
            "ref_domain": df_index['Reffering Domains'][-1:].item(),
            "total_keywords": int(df_ranking['Category'].count()),
            "rank_1_3": int(df_ranking[range_date[6]].between(1,3).sum()),
            "rank_4_10": int(df_ranking[range_date[6]].between(4,10).sum()),
            "rank_11_30": int(df_ranking[range_date[6]].between(11,30).sum()),
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
        logger.error(f"An error occurred while fetching SEO data: {e}")
        raise


async def fetch_seo_daily_growth(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
):
    """
    Fetch SEO daily growth data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing SEO daily growth data.
    """
    try:
        # Initialize data fetchers
        range_last_week_date = range_of_date(7)
        range_last2_week_date = range_of_date(14)
        metrics_ga4 = await asyncio.gather(
            dg_ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='sessions'),
            dg_ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='source'),
            dg_ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='total_user'),
            dg_ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='new_user'),
            dg_ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics='bounce_rate'),
            dg_indexing(column='DR'),
            dg_indexing(column='Indexing'),
            dg_indexing(column='Backlinks'),
            dg_indexing(column='Reffering Domains')
        )
        df_ranking = await ranking_df()

        w1_rank_1_3 = df_ranking[range_last_week_date[6]].between(1,3).sum()
        w1_rank_4_10 = df_ranking[range_last_week_date[6]].between(4,10).sum()
        w1_rank_11_30 = df_ranking[range_last_week_date[6]].between(11,30).sum()

        w2_rank_1_3 = df_ranking[range_last2_week_date[6]].between(1,3).sum()
        w2_rank_4_10 = df_ranking[range_last2_week_date[6]].between(4,10).sum()
        w2_rank_11_30 = df_ranking[range_last2_week_date[6]].between(11,30).sum()
        if w2_rank_1_3 == 0 :
            dg_rank_1_3 = 0
        else:
            dg_rank_1_3 = (w1_rank_1_3 - w2_rank_1_3)/w2_rank_1_3
        if w2_rank_4_10 == 0 :
            dg_rank_4_10 = 0
        else:
            dg_rank_4_10 = (w1_rank_4_10 - w2_rank_4_10)/w2_rank_4_10
        if w2_rank_11_30 == 0 :
            dg_rank_11_30 = 0
        else:
            dg_rank_11_30 = (w1_rank_11_30 - w2_rank_11_30)/w2_rank_11_30

        container = {
            "sessions": float(metrics_ga4[0]),
            "source": float(metrics_ga4[1]),
            "total_user": float(metrics_ga4[2]),
            "new_user": float(metrics_ga4[3]),
            "bounce_rate": float(metrics_ga4[4]),
            "dr": float(metrics_ga4[5]),
            "indexing": float(metrics_ga4[6]),
            "backlinks": float(metrics_ga4[7]),
            "ref_domain": float(metrics_ga4[8]),
            "rank_1_3": float(round(dg_rank_1_3, 4)),
            "rank_4_10": float(round(dg_rank_4_10, 4)),
            "rank_11_30": float(round(dg_rank_11_30, 4)),
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
        logger.error(f"An error occurred while fetching SEO daily growth data: {e}")
        raise


async def fetch_seo_chart(
    session: AsyncSession,
    from_date: datetime.date, 
    to_date: datetime.date,
):
    """
    Fetch SEO chart data for a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): Start date for data filtering.
        to_date (datetime.date): End date for data filtering.

    Returns:
        Dict[str, Any]: A dictionary containing SEO chart data.
    """
    try:
        # Initialize data fetchers
        charts = await asyncio.gather(
            ga4_metrics_chart(session=session, from_date=from_date, to_date=to_date),
            ga4_source_chart(session=session, from_date=from_date, to_date=to_date),
            ga4_device_chart(session=session, from_date=from_date, to_date=to_date),
            landing_page_tbl(session=session, from_date=from_date, to_date=to_date, medium='organic'),
            landing_page_tbl(session=session, from_date=from_date, to_date=to_date, medium='cpc'),
            ranking_chart(),
            web_traffic_table(session=session, from_date=from_date, to_date=to_date)
        )

        container = {
            "metrics_chart": charts[0],
            "source_chart": charts[1],
            "device_chart": charts[2],
            "landing_page_organic": charts[3],
            "landing_page_cpc": charts[4],
            "ranking_chart": charts[5],
            "web_traffic_chart": charts[6]
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
        logger.error(f"An error occurred while fetching SEO chart data: {e}")
        raise

