import pandas as pd
import re
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from datetime import datetime, timedelta
from app.db.models.user import  GooddreamerUserData as ac
from app.db.models.data_source import Sources as s, ModelHasSources as mhs
from app.db.models.novel import GooddreamerUserChapterProgression as gucp, GooddreamerChapterTransaction as gct, GooddreamerUserChapterAdmob as guca


async def play_console_install(from_date:datetime.date, to_date: datetime.date):
    """
    Retrieves the total number of installs from the Google Play Console within a specified date range.

    Args:
        from_date (datetime.date): The start date for the data range. Default is None.
        to_date (datetime.date): The end date for the data range. Default is None.

    Returns:
        int: The total number of installs within the specified date range.
    """
    
    # Read the CSV file
    read_csv = await asyncio.to_thread(pd.read_csv, './csv/play_console_install.csv', delimiter=',', index_col=False)
    
    # Rename columns for better readability
    await asyncio.to_thread(
        lambda: read_csv.rename(
            columns={
                'Date': 'date', 
                'User acquisition (All users, All events, Per interval, Daily): All countries / regions': 'install'}, 
            inplace=True))
    
    # Select relevant columns
    read_csv = await asyncio.to_thread(lambda: read_csv.loc[:, ['date', 'install']])
    
    # Convert the DataFrame to ensure compatibility
    df = pd.DataFrame(read_csv)

    # Convert date column to datetime and install column to integers
    df['date'] = await asyncio.to_thread(pd.to_datetime, df['date'], format='mixed', dayfirst=True)
    df['date'] = await asyncio.to_thread(lambda: df['date'].dt.date)
    df['install'] = await asyncio.to_thread(lambda: df['install'].replace(re.compile(r'[\,]'), '', regex=True))
    df['install'] = await asyncio.to_thread(lambda: df['install'].astype(int))
    
    # Sort the DataFrame by install values in descending order
    await asyncio.to_thread(lambda: df.sort_values(by='install', ascending=False, inplace=True))
    
    # Downcast install column to integer for memory efficiency
    df['install'] = await asyncio.to_thread(lambda: pd.to_numeric(df['install'], downcast='integer'))

    # Filter the DataFrame based on the specified date range
    df_filter = await asyncio.to_thread(lambda: df[(df['date'] >= from_date) & (df['date'] <= to_date)])
    
    # Calculate the sum of installs within the filtered date range
    install_sum = await asyncio.to_thread(lambda: df_filter['install'].sum())

    return int(install_sum)


async def apple_total_download(from_date: datetime.date, to_date: datetime.date):
    """
    Calculates the total downloads from the Apple store within the specified date range.

    Args:
        from_date (datetime.date): The start date of the period. Default is None.
        to_date (datetime.date): The end date of the period. Default is None.

    Returns:
        int: The total number of downloads from the Apple store within the specified date range.
    """
    # Read the CSV file containing Apple total download data
    df_read = await asyncio.to_thread(pd.read_csv, './csv/apple_total_download.csv')
    
    # Convert 'Date' column to datetime and extract date
    df_read['Date'] = await asyncio.to_thread(pd.to_datetime, df_read['Date'], format='%m/%d/%y')
    df_read["Date"] = await asyncio.to_thread(lambda: df_read['Date'].dt.date)
    
    # Convert 'Total Downloads' column to integer
    df_read['Total Downloads'] = await asyncio.to_thread(lambda: df_read['Total Downloads'].astype(int))

    # Filter the dataframe based on the provided date range
    df = await asyncio.to_thread(lambda: df_read[(df_read['Date'] >= from_date) & (df_read['Date'] <= to_date)])

    # Calculate the total downloads within the specified date range
    total_downloads = await asyncio.to_thread(lambda: df['Total Downloads'].sum())
    
    return int(total_downloads)


async def overall_data(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        data: str = 'register'):
    """
    Retrieves overall data based on the specified criteria.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date, optional): The start date of the period. Default is None.
        to_date (datetime.date, optional): The end date of the period. Default is None.
        data (str, optional): Specifies the type of data to retrieve. Possible values are:
            - 'register': Total number of registered users between the specified dates.
            - 'pembaca': Total number of unique readers between the specified dates.
            - 'pembeli': Total number of unique buyers (combining two different sources) between the specified dates. Default is 'register'.

    Returns:
        int: The overall data based on the specified criteria.
    """
    if data == 'register':
        query = select(
            func.count(ac.id)
        ).filter(
            func.date(ac.registered_at).between(from_date, to_date)
        )
        result = await session.execute(query)
        result_data = result.scalar()

    elif data == 'pembaca':
        query = select(
            func.count(gucp.user_id.distinct())
        ).filter(
            func.date(gucp.created_at).between(from_date, to_date)
        )
        result = await session.execute(query)
        result_data = result.scalar()
    
    elif data == 'pembeli':
        query_1 = select(
            func.count(gct.user_id.distinct())
        ).filter(
            func.date(gct.created_at).between(from_date, to_date)
        )
        query1_result = await session.execute(query_1)
        query1_data = query1_result.scalar()

        query_2 = select(
            func.count(guca.user_id.distinct())
        ).filter(
            func.date(guca.created_at).between(from_date, to_date)
        )
        query2_result = await session.execute(query_2)
        query2_data = query2_result.scalar()

        result_data = int(query1_data + query2_data)

    return result_data


async def register(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        source: str = 'app'):
    """
    Retrieves the total number of registrations within a specified date range and source.

    Args:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date, optional): The start date of the date range. Default is None.
        to_date (datetime.date, optional): The end date of the date range. Default is None.
        source (str, optional): The source of registration. Default is 'app'.

    Returns:
        int: The total number of registrations within the specified date range and source.
    """
    if source in ["app", "web"]:
        reg = select(
            func.count(ac.id).label('register')
        ).join(
            ac.model_has_sources
        ).join(
            mhs.sources
        ).filter(
            mhs.model_type == 'App\\Models\\User',
            s.name == source,
            func.date(ac.registered_at).between(from_date, to_date), 
            ac.is_guest == 0)
    elif source == "all":
        reg = select(
            func.count(ac.id).label('register')
        ).join(
            ac.model_has_sources
        ).join(
            mhs.sources
        ).filter(
            mhs.model_type == 'App\\Models\\User',
            func.date(ac.registered_at).between(from_date, to_date), 
            ac.is_guest == 0)
    

    result = await session.execute(reg)
    data = result.scalar()

    return data


async def dg_register(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        source: str = 'app'):
    """
    Calculate the daily growth rate of user registrations.

    This function calculates the daily growth rate of user registrations for a specified source
    within the given date range and returns it as a formatted percentage.

    Args:
        from_date (datetime.date): The start date of the date range.
        to_date (datetime.date): The end date of the date range.
        source (str, optional): The source of user registrations. Defaults to 'app'.

    Returns:
        str: The daily growth rate as a formatted percentage.

    Raises:
        None
    """

    delta = (to_date - from_date)+timedelta(1)
    fromdate_lastweek = from_date - delta
    todate_lastweek = to_date - delta

    # get scalar value for register first week
    week1 = await register(session=session, from_date=from_date, to_date=to_date, source=source)
    
    # get scalar value for register second week
    week2 = await register(session=session, from_date=fromdate_lastweek, to_date=todate_lastweek, source=source)
    
    # set growth value to 0 to avoid divide by zero error
    if week2 == 0:
        growth = 0
    else:
        # calculate the daily growth rate
        growth = (week1 - week2)/week2

    # Convert int value to str persentase
    txt = float(round(growth, 4))

    return txt

