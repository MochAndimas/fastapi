import pandas as pd
import json
import plotly
import plotly.graph_objects as go
import asyncio
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from app.db.models.acquisition import Ga4SessionsData
from app.db.models.user import GooddreamerUserData as gud
from app.db.models.data_source import ModelHasSources as mhs, Sources as s
from app.db.models.novel import GooddreamerUserChapterProgression as gucp, GooddreamerChapterTransaction as gct
from app.db.models.novel import GooddreamerUserChapterAdmob as guca
from app.db.models.coin import GooddreamerTransaction as gt


# Define a function to convert decimal hours to timedelta
def decimal_to_timedelta(decimal_hours):
    hours = int(decimal_hours)
    minutes = int((decimal_hours - hours) * 60)
    return timedelta(hours=hours, minutes=minutes)


async def install_to_register(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        source: str = 'app', 
        types: str = 'hour') -> pd.DataFrame:
    """
    Calculate the average time taken from user install to registration, grouped by month.

    Args:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (datetime.date): Start date for filtering user data.
        to_date (datetime.date): End date for filtering user data.
        source (str): Filter by a specific source of user installation, or 'all' for all sources ('all', 'web', 'app').
        types (str): Type of time unit for calculation ('hour', 'month') (default is 'hour').

    Returns:
        pandas.DataFrame: DataFrame containing the calculated average time from install to register, grouped by month.
    """
    # Query the database based on the provided parameters
    if source == 'all':
        query = select(
            func.concat(func.month(gud.created_at), "-", func.year(gud.created_at)).label('period'),
            func.avg(func.timestampdiff(text(types), gud.created_at, gud.registered_at)).label(types)
        ).filter(
            func.date(gud.created_at).between(from_date, to_date)
        ).group_by('period')
    else:
        query = select(
                func.concat(func.month(gud.created_at), "-", func.year(gud.created_at)).label('period'),
                func.avg(func.timestampdiff(text(types), gud.created_at, gud.registered_at)).label(types)
            ).join(
                gud.model_has_sources
            ).join(
                mhs.sources
            ).filter(
                mhs.model_type == 'App\\Models\\User',
                s.name == source,
                func.date(gud.created_at).between(from_date, to_date)
            ).group_by('period')

    # Convert query result to DataFrame
    data = await session.execute(query)
    results = data.fetchall()
    df = pd.DataFrame(results)
    
    # If DataFrame is empty, fill with default values
    if df.empty:
        df['period'] = await asyncio.to_thread(pd.date_range, from_date, from_date)
        df['period'] = await asyncio.to_thread(pd.to_datetime, df["period"])
        df['period'] = await asyncio.to_thread(lambda: df["period"].dt.to_period("M"))
        df[types] = 0
    else:    
        df["period"] = await asyncio.to_thread(pd.to_datetime, df["period"], format="%m-%Y")
        df['period'] = await asyncio.to_thread(lambda: df['period'].dt.to_period('M'))
        await asyncio.to_thread(lambda: df.sort_values(by='period', ascending=True, inplace=True))
    
    return df


async def register_to_read(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date,
        source: str = 'app', 
        types: str = 'hour') -> pd.DataFrame:
    """
    Generate a time register for user activity to read, aggregated by month.

    Parameters:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (datetime.date): Start date for filtering user registrations and reading activities.
        to_date (datetime.date): End date for filtering user registrations and reading activities.
        source (str): Source of user registrations and reading activities (e.g., 'app', 'web', 'all').
        types (str): Type of time difference to calculate (e.g., 'hour', 'month').

    Returns:
        pandas.DataFrame: DataFrame with periods (months) and corresponding average time differences.
    """

    # Construct subqueries based on the specified source
    if source == 'all':
        register_date_subquery = select(
            gud.id.label('id'),
            gud.registered_at.label('register_date')
        ).filter(
            func.date(gud.registered_at).between(from_date, to_date)
        ).subquery()

        read_date_subquery = select(
            gucp.user_id.label('user_id'),
            func.min(gucp.created_at).label('read_date')
        ).join(
            gucp.gooddreamer_user_data
        ).filter(
            gud.is_guest == 0
        ).group_by(gucp.user_id).subquery()
    else:
        register_date_subquery = select(
            gud.id.label('id'),
            gud.registered_at.label('register_date')
        ).join(
            gud.model_has_sources
        ).join(
            mhs.sources
        ).filter(
            mhs.model_type == 'App\\Models\\User',
            s.name == source,
            func.date(gud.registered_at).between(from_date, to_date)
        ).subquery()

        read_date_subquery = select(
            gucp.user_id.label('user_id'),
            func.min(gucp.created_at).label('read_date')
        ).join(
            gucp.model_has_sources
        ).join(
            mhs.sources
        ).join(
            gucp.gooddreamer_user_data
        ).filter(
            mhs.model_type == 'App\\Models\\ChapterProgression',
            gud.is_guest == 0,
            s.name == source,
        ).group_by(gucp.user_id).subquery()

    # Construct the main query to calculate time differences
    period = func.concat(func.month(register_date_subquery.c.register_date), '-', func.year(register_date_subquery.c.register_date)).label('period')
    diff = func.avg(func.timestampdiff(text(types), register_date_subquery.c.register_date, read_date_subquery.c.read_date)).label(types)

    query = select(
        period.label('period'),
        diff.label(types)
    ).join(
        read_date_subquery, register_date_subquery.c.id == read_date_subquery.c.user_id
    ).filter(
        read_date_subquery.c.read_date >= register_date_subquery.c.register_date
    ).group_by('period')

    # Convert query results to a DataFrame
    results = await session.execute(query)
    data = results.fetchall()
    df = pd.DataFrame(data)

    # Handle empty DataFrame or sort DataFrame by period
    if df.empty:
        df['period'] = await asyncio.to_thread(pd.date_range, from_date, from_date)
        df['period'] = await asyncio.to_thread(pd.to_datetime, df["period"])
        df['period'] = await asyncio.to_thread(lambda: df['period'].dt.to_period('M'))
        df[types] = 0
    else:
        df['period'] = await asyncio.to_thread(pd.to_datetime, df['period'], format='%m-%Y')
        df['period'] = await asyncio.to_thread(lambda: df['period'].dt.to_period('M'))
        await asyncio.to_thread(lambda: df.sort_values(by='period', ascending=True, inplace=True))
    
    return df


async def read_to_buy_chapter(
        session: AsyncSession, 
        from_date: datetime.date,
        to_date: datetime.date,
        source: str = 'app', 
        types: str = 'hour') -> pd.DataFrame:
    """
    Generate a time register for user activity from reading to buying chapters, aggregated by month.

    Parameters:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (datetime.date): Start date for filtering user activities.
        to_date (datetime.date): End date for filtering user activities.
        source (str): Source of user activities (e.g., 'app', 'web', 'all').
        types (str): Type of time difference to calculate (e.g., 'hour', 'month').

    Returns:
        pandas.DataFrame: DataFrame with periods (months) and corresponding average time differences.
    """

    # Construct subqueries based on the specified source
    if source == 'all':
        read_date_subquery = select(
            gucp.user_id.label('user_id'),
            func.min(gucp.created_at).label('read_date')
        ).group_by(gucp.user_id).subquery()

        coin_chapter_subquery = select(
            gct.user_id.label('user_id'),
            func.min(gct.created_at).label('chapter_coin_first_buy')
        ).group_by(gct.user_id).subquery()

        admob_chapter_subquery = select(
            guca.user_id.label('user_id'),
            func.min(guca.created_at).label('chapter_admob_first_buy')
        ).group_by(guca.user_id).subquery()
    else:
        read_date_subquery = select(
            gucp.user_id.label('user_id'),
            func.min(gucp.created_at).label('read_date')
        ).join(
            gucp.model_has_sources
        ).join(
            mhs.sources
        ).filter(
            mhs.model_type == 'App\\Models\\ChapterProgression',
            s.name == source,
        ).group_by(gucp.user_id).subquery()

        coin_chapter_subquery = select(
            gct.user_id.label('user_id'),
            func.min(gct.created_at).label('chapter_coin_first_buy')
        ).join(
            gct.model_has_sources
        ).join(
            mhs.sources
        ).filter(
            mhs.model_type == 'App\\Models\\ChapterTransaction',
            s.name == source,
        ).group_by(gct.user_id).subquery()

        admob_chapter_subquery = select(
            guca.user_id.label('user_id'),
            func.min(guca.created_at).label('chapter_admob_first_buy')
        ).join(
            guca.model_has_sources
        ).join(
            mhs.sources
        ).filter(
            mhs.model_type == 'App\\Models\\UserChapterAdmob',
            s.name == source,
        ).group_by(guca.user_id).subquery()

    # Union subqueries to determine the first buy date for each user
    chapter_date_subquery = select(
        coin_chapter_subquery.c.user_id.label('user_id'),
        coin_chapter_subquery.c.chapter_coin_first_buy.label('first_buy')
    ).union_all(
        select(
            admob_chapter_subquery.c.user_id.label('user_id'),
            admob_chapter_subquery.c.chapter_admob_first_buy.label('first_buy')
        )
    ).subquery()

    # Find the first buy date for each user and calculate time difference from reading
    first_buy_chapter_subquery = select(
        chapter_date_subquery.c.user_id.label('user_id'),
        func.min(chapter_date_subquery.c.first_buy).label('first_buy')
    ).group_by('user_id').subquery()

    period = func.concat(func.month(read_date_subquery.c.read_date), '-', func.year(read_date_subquery.c.read_date)).label('period')
    diff = func.avg(func.timestampdiff(text(types), read_date_subquery.c.read_date, first_buy_chapter_subquery.c.first_buy)).label(types)

    query = select(
        period.label('period'),
        diff.label(types)
    ).join(
        first_buy_chapter_subquery, read_date_subquery.c.user_id == first_buy_chapter_subquery.c.user_id
    ).filter(
        func.date(read_date_subquery.c.read_date).between(from_date, to_date)
    ).group_by('period')

    # Convert query results to a DataFrame
    results = await session.execute(query)
    data = results.fetchall()
    df = pd.DataFrame(data)

    # Handle empty DataFrame or sort DataFrame by period
    if df.empty:
        df['period'] = await asyncio.to_thread(pd.date_range, from_date, from_date)
        df['period'] = await asyncio.to_thread(pd.to_datetime, df['period'])
        df['period'] = await asyncio.to_thread(lambda: df['period'].dt.to_period('M'))
        df[types] = 0
    else:
        df['period'] = await asyncio.to_thread(pd.to_datetime, df['period'], format='%m-%Y')
        df['period'] = await asyncio.to_thread(lambda: df['period'].dt.to_period('M'))
        await asyncio.to_thread(lambda: df.sort_values(by='period', ascending=True, inplace=True))
    
    return df


async def install_to_coin(
        session: AsyncSession, 
        from_date: datetime.date,
        to_date: datetime.date,
        source: str = 'app',
        types: str = 'hour') -> pd.DataFrame:
    """
    Generate a time register for user activity from installation to purchasing coins, aggregated by month.

    Parameters:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (datetime.date): Start date for filtering user installations and coin purchases.
        to_date (datetime.date): End date for filtering user installations and coin purchases.
        source (str): Source of user installations and coin purchases (e.g., 'app', 'web', 'all').
        types (str): Type of time difference to calculate (e.g., 'hour', 'month').

    Returns:
        pandas.DataFrame: DataFrame with periods (months) and corresponding average time differences.
    """

    # Construct subqueries based on the specified source
    if source == 'all':
        install_subquery = select(
            gud.id.label('user_id'),
            gud.created_at.label('install_date')
        ).filter(
            func.date(gud.created_at).between(from_date, to_date)
        ).subquery()

        coin_purchase_subquery = select(
            gt.user_id.label('user_id'),
            func.min(gt.created_at).label('transaction_date')
        ).filter(
            func.date(gt.created_at).between(from_date, to_date),
            gt.transaction_status == 1
        ).group_by(gt.user_id).subquery()
    else:
        install_subquery = select(
            gud.id.label('user_id'),
            gud.created_at.label('install_date')
        ).join(
            gud.model_has_sources
        ).join(
            mhs.sources
        ).filter(
            mhs.model_type == 'App\\Models\\User',
            s.name == source,
            func.date(gud.created_at).between(from_date, to_date)
        ).subquery()

        coin_purchase_subquery = select(
            gt.user_id.label('user_id'),
            func.min(gt.created_at).label('transaction_date')
        ).join(
            gt.model_has_sources
        ).join(
            mhs.sources
        ).filter(
            mhs.model_type == 'App\\Models\\Transaction',
            s.name == source,
            func.date(gt.created_at).between(from_date, to_date),
            gt.transaction_status == 1
        ).group_by(gt.user_id).subquery()

    # Calculate the time difference between installation and first coin purchase
    period = func.concat(func.month(install_subquery.c.install_date), '-', func.year(install_subquery.c.install_date)).label('period')
    diff = func.avg(func.timestampdiff(text(types), install_subquery.c.install_date, coin_purchase_subquery.c.transaction_date)).label(types)

    query = select(
        period.label('period'),
        diff.label(types)
    ).join(
        coin_purchase_subquery, install_subquery.c.user_id == coin_purchase_subquery.c.user_id
    ).group_by('period')

    # Convert query results to a DataFrame
    results = await session.execute(query)
    data = results.fetchall()
    df = pd.DataFrame(data)
    
    # Handle empty DataFrame or sort DataFrame by period
    if df.empty:
        df['period'] = await asyncio.to_thread(pd.date_range, from_date, from_date)
        df['period'] = await asyncio.to_thread(pd.to_datetime, df['period'])
        df['period'] = await asyncio.to_thread(lambda: df['period'].dt.to_period('M'))
        df[types] = 0
    else:
        df['period'] = await asyncio.to_thread(pd.to_datetime, df['period'], format='%m-%Y')
        df['period'] = await asyncio.to_thread(lambda: df['period'].dt.to_period('M'))
        await asyncio.to_thread(lambda: df.sort_values(by='period', ascending=True, inplace=True))
    
    return df


async def user_activity_time(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        source: str = 'app', 
        types: str = 'hour') -> pd.DataFrame:
    """
    Generate a DataFrame containing user activity times for different stages of user engagement.

    Parameters:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (datetime.date): Start date for filtering user activities.
        to_date (datettime.date): End date for filtering user activities.
        source (str): Source of user activities (e.g., 'app', 'web', 'all').
        types (str): Type of time difference to calculate (e.g., 'hour', 'month').

    Returns:
        pandas.DataFrame: DataFrame with periods (months) and corresponding time durations for each activity stage.
    """

    # Retrieve time durations for each user activity stage
    install_regis, regis_read, read_chapter, install_coin = await asyncio.gather(
         install_to_register(session=session, from_date=from_date, to_date=to_date, source=source, types=types),
        register_to_read(session=session, from_date=from_date, to_date=to_date, source=source, types=types),
        read_to_buy_chapter(session=session, from_date=from_date, to_date=to_date, source=source, types=types),
        install_to_coin(session=session, from_date=from_date, to_date=to_date, source=source, types=types)
     )

    await asyncio.gather(
        asyncio.to_thread(lambda: install_regis.fillna(0, inplace=True)),
        asyncio.to_thread(lambda: regis_read.fillna(0, inplace=True)),
        asyncio.to_thread(lambda: read_chapter.fillna(0, inplace=True)),
        asyncio.to_thread(lambda: install_coin.fillna(0, inplace=True))
    )

    # Transform time durations to timedelta format if types is 'hour'
    if types == 'hour':
        install_regis[f'Install To Regis {types.capitalize()}'] = await asyncio.to_thread(lambda: install_regis[types].apply(decimal_to_timedelta))
        install_regis = await asyncio.to_thread(lambda: install_regis.loc[:, ['period', f'Install To Regis {types.capitalize()}']])
        regis_read[f'Register To Read {types.capitalize()}'] = await asyncio.to_thread(lambda: regis_read[types].apply(decimal_to_timedelta))
        regis_read = await asyncio.to_thread(lambda: regis_read.loc[:, ['period', f'Register To Read {types.capitalize()}']])
        read_chapter[f'Read To Buy Chapter {types.capitalize()}'] = await asyncio.to_thread(lambda: read_chapter[types].apply(decimal_to_timedelta))
        read_chapter = await asyncio.to_thread(lambda: read_chapter.loc[:, ['period', f'Read To Buy Chapter {types.capitalize()}']])
        install_coin[f'Install To Buy Coin {types.capitalize()}'] = await asyncio.to_thread(lambda: install_coin[types].apply(decimal_to_timedelta))
        install_coin = await asyncio.to_thread(lambda: install_coin.loc[:, ['period', f'Install To Buy Coin {types.capitalize()}']])

    # Rename columns if types is 'day'
    elif types == 'day':
        await asyncio.to_thread(lambda: install_regis.rename(columns={'day': f'Install To Regis {types.capitalize()}'}, inplace=True))
        await asyncio.to_thread(lambda: regis_read.rename(columns={'day': f'Register To Read {types.capitalize()}'}, inplace=True))
        await asyncio.to_thread(lambda: read_chapter.rename(columns={'day': f'Read To Buy Chapter {types.capitalize()}'}, inplace=True))
        await asyncio.to_thread(lambda: install_coin.rename(columns={'day': f'Install To Buy Coin {types.capitalize()}'}, inplace=True))

    # Merge DataFrames for different activity stages
    df_1 = await asyncio.to_thread(pd.merge, install_regis, regis_read, on='period', how='outer')
    df_2 = await asyncio.to_thread(pd.merge, read_chapter, install_coin, on='period', how='outer')
    df = await asyncio.to_thread(pd.merge, df_1, df_2, on='period', how='outer')

    # Fill missing values and sort DataFrame
    if types == 'hour':
        await asyncio.to_thread(lambda: df.fillna(timedelta(days=0.0, hours=0.0, minutes=0.0, seconds=0.0), inplace=True))
    elif types == 'day':
        await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
    await asyncio.to_thread(lambda: df.sort_values(by='period', ascending=True, inplace=True))

    return df


async def heatmap_table(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        source: str = 'app',
        types: str = 'hour') -> str:
    """
    Generate a heatmap table visualizing user activity durations for different stages of user engagement.

    Parameters:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (str): Start date for filtering user activities.
        to_date (str): End date for filtering user activities.
        source (str): Source of user activities (e.g., 'app', 'web', 'all').
        types (str): Type of time difference to visualize (e.g., 'hour', 'month').

    Returns:
        str: JSON object representing the Plotly table.
    """
    # Fetch user activity data
    df = await user_activity_time(session=session, from_date=from_date, to_date=to_date, source=source, types=types)
    
    if types == 'hour':
        # Convert timedeltas to hours
        df[f'Install To Regis {types.capitalize()}'] = await asyncio.to_thread(pd.to_timedelta, df[f'Install To Regis {types.capitalize()}'])
        df[f'Register To Read {types.capitalize()}'] = await asyncio.to_thread(pd.to_timedelta, df[f'Register To Read {types.capitalize()}'])
        df[f'Read To Buy Chapter {types.capitalize()}'] = await asyncio.to_thread(pd.to_timedelta, df[f'Read To Buy Chapter {types.capitalize()}'])
        df[f'Install To Buy Coin {types.capitalize()}'] = await asyncio.to_thread(pd.to_timedelta, df[f'Install To Buy Coin {types.capitalize()}'])

        df[f'Install To Regis {types.capitalize()}'] = await asyncio.to_thread(lambda: df[f'Install To Regis {types.capitalize()}'].dt.total_seconds())
        df[f'Register To Read {types.capitalize()}'] = await asyncio.to_thread(lambda: df[f'Register To Read {types.capitalize()}'].dt.total_seconds())
        df[f'Read To Buy Chapter {types.capitalize()}'] = await asyncio.to_thread(lambda: df[f'Read To Buy Chapter {types.capitalize()}'].dt.total_seconds())
        df[f'Install To Buy Coin {types.capitalize()}'] = await asyncio.to_thread(lambda: df[f'Install To Buy Coin {types.capitalize()}'].dt.total_seconds())

        df[f'Install To Regis {types.capitalize()}'] = await asyncio.to_thread(lambda: df[f'Install To Regis {types.capitalize()}']) / 3600
        df[f'Register To Read {types.capitalize()}'] = await asyncio.to_thread(lambda: df[f'Register To Read {types.capitalize()}']) / 3600
        df[f'Read To Buy Chapter {types.capitalize()}'] = await asyncio.to_thread(lambda: df[f'Read To Buy Chapter {types.capitalize()}']) / 3600
        df[f'Install To Buy Coin {types.capitalize()}'] = await asyncio.to_thread(lambda: df[f'Install To Buy Coin {types.capitalize()}']) / 3600
    elif types == 'day':
        pass

    # Convert period to string
    df['period'] = await asyncio.to_thread(lambda: df['period'].astype(str))
    
    # Replace NaN values with 0 in relevant columns
    await asyncio.to_thread(lambda: df.fillna({f'Install To Regis {types.capitalize()}': 0,
               f'Register To Read {types.capitalize()}': 0,
               f'Read To Buy Chapter {types.capitalize()}': 0,
               f'Install To Buy Coin {types.capitalize()}': 0}, inplace=True))
    
    # Function to normalize and scale values to the range 0-255
    def normalize_and_scale(series):
        min_val = series.min()
        max_val = series.max()
        if min_val == max_val:
            # Exclude columns with all 0 values from scaling
            return series * 255
        else:
            scaled_series = 255 * (series - min_val) / (max_val - min_val)
            return scaled_series
    
    # Apply normalization and scaling
    install_to_regis_scaled = normalize_and_scale(df[f'Install To Regis {types.capitalize()}'])
    register_to_read_scaled = normalize_and_scale(df[f'Register To Read {types.capitalize()}'])
    read_to_buy_chapter_scaled = normalize_and_scale(df[f'Read To Buy Chapter {types.capitalize()}'])
    install_to_buy_coin_scaled = normalize_and_scale(df[f'Install To Buy Coin {types.capitalize()}'])
    
    # Header color
    header_color = 'grey'  # Light grey color
    
    # Create Plotly table with conditional formatting
    fig = go.Figure(
        go.Table(
            header=dict(
                line_color='black',
                font=dict(color='black'),
                values=[
                    'Period',
                    f'Install To Regis {types.capitalize()}',
                    f'Register To Read {types.capitalize()}',
                    f'Read To Buy Chapter {types.capitalize()}',
                    f'Install To Buy Coin {types.capitalize()}'
                ],
                fill_color=header_color
            ),
            cells=dict(
                line_color='black',
                font=dict(color='black'),
                values=[
                    df['period'],
                    df[f'Install To Regis {types.capitalize()}'].apply(lambda x: "{:,.0f}".format(x)),
                    df[f'Register To Read {types.capitalize()}'].apply(lambda x: "{:,.0f}".format(x)),
                    df[f'Read To Buy Chapter {types.capitalize()}'].apply(lambda x: "{:,.0f}".format(x)),
                    df[f'Install To Buy Coin {types.capitalize()}'].apply(lambda x: "{:,.0f}".format(x))
                ],
                fill_color=[
                    [header_color]*len(df),  # Period column, same color as header
                    install_to_regis_scaled.apply(lambda x: f'rgb({255-int(x)}, 255, {255-int(x)})').tolist(),
                    register_to_read_scaled.apply(lambda x: f'rgb({255-int(x)}, 255, {255-int(x)})').tolist(),
                    read_to_buy_chapter_scaled.apply(lambda x: f'rgb({255-int(x)}, 255, {255-int(x)})').tolist(),
                    install_to_buy_coin_scaled.apply(lambda x: f'rgb({255-int(x)}, 255, {255-int(x)})').tolist()
                ]
            )
        )
    )

    # Return the table as a JSON object
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
    return chart


async def ga4_session(
        sqlite_session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        source: str = 'app') -> str:
    """
    Generates a Plotly JSON-encoded chart of user engagement duration from a GA4 session data table.

    This asynchronous function queries a SQLite database for user engagement data from the GA4 sessions table.
    It filters data based on a date range and platform (source), processes the fetched data, and generates a 
    Plotly line chart showing the average user engagement time in minutes for each date.

    Args:
        sqlite_session (AsyncSession): The SQLAlchemy AsyncSession instance connected to the SQLite database.
        from_date (datetime.date): The starting date for filtering the data.
        to_date (datetime.date): The ending date for filtering the data.
        source (str, optional): The platform to filter the data by. Defaults to 'app'.
            - 'app': Filters for entries with platforms 'android' or 'ios'.
            - 'all': Includes all platforms without filtering.

    Returns:
        str: A JSON-encoded Plotly chart showing average user engagement duration (in minutes) over the date range.

    Example:
        >>> await ga4_session(sqlite_session, from_date=date(2024, 1, 1), to_date=date(2024, 2, 1), source='app')
        # Returns a JSON string containing the Plotly chart.

    Notes:
        - This function uses asynchronous database operations and multiprocessing for intensive data processing.
        - The 'user_engaged_duration' is converted from milliseconds to minutes.
        - 'platform' entries for 'android' and 'ios' are mapped to 'app'.
    """
    query = select(
        Ga4SessionsData.date.label("date"),
        Ga4SessionsData.device_category.label("device_category"),
        Ga4SessionsData.platform.label("platform"),
        Ga4SessionsData.user_engaged_duration.label("user_engaged_duration")
    ).filter(
        Ga4SessionsData.date.between(from_date, to_date)
    )
    result = await sqlite_session.execute(query)
    data = result.fetchall()
    df = pd.DataFrame(data)

    if df.empty:
        df = pd.DataFrame({
            "date": pd.date_range(to_date, to_date).date,
            "device_category": ["-"],
            "platform": ["-"],
            "user_engaged_duration": [0]
        })

    df['user_engaged_duration'] = await asyncio.to_thread(lambda: df['user_engaged_duration'] / 60000)
    df['user_engaged_duration'] = await asyncio.to_thread(lambda: round(df['user_engaged_duration'], 2))
    df['platform'] = await asyncio.to_thread(lambda: df['platform'].str.lower())
    df['platform'] = await asyncio.to_thread(lambda: df['platform'].replace('android', 'app'))
    df['platform'] = await asyncio.to_thread(lambda: df['platform'].replace('ios', 'app'))
    df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
    df['date'] = await asyncio.to_thread(lambda: df['date'].dt.date)
    df = await asyncio.to_thread(lambda: df[df['platform'] == source] if source != 'all' else df.copy())
    
    df = await asyncio.to_thread(lambda: df.groupby(['date', 'platform']).agg(
        user_engaged_duration=('user_engaged_duration', 'sum')
    ).reset_index())
    await asyncio.to_thread(lambda: df.sort_values(by='date', ascending=False, inplace=True))
    
    fig = go.Figure(
        go.Scatter(
            x=df['date'],
            y=df['user_engaged_duration'],
            text=df['user_engaged_duration'].apply(lambda x: "{:,.0f} Minute".format(x)),
            line=dict(color='blue')
        )
    )
    fig.update_layout(title='Average User Engagement Time By Minutes')
    fig.update_yaxes(title="Minute")
    
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    return chart
