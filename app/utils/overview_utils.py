import pandas as pd
import asyncio
import json
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from app.utils.new_install_utils import cost
from app.db.models.acquisition import Ga4ActiveUserData, AdmobReportData, AdsenseReportData


async def dau_mau_df(
        from_date: datetime.date, 
        to_date: datetime.date, 
        data: str  = "dataframe",
        source: str = 'app'):
    """
    Generate a DataFrame containing daily active users (DAU), monthly active users (MAU), and stickiness.

    This function reads daily and monthly active user data from CSV files, merges them based on the source,
    calculates stickiness, and filters the data based on the specified date range and source.

    Args:
        from_date (datetime.date): The start date of the date range.
        to_date (datetime.date): The end date of the date range.
        data (str, optional): The data to return either 'dataframe' or 'stickiness'.
        source (str, optional): The source of the data ('app', 'web', or 'all'). Defaults to 'app'.

    Returns:
        pandas.DataFrame: A DataFrame containing daily active users, monthly active users, and stickiness.

    Note:
        CSV files are expected to be named 'dau_mau_android.csv', 'dau_mau_ios.csv', and 'dau_mau_web.csv'.
        Ensure the necessary packages are installed:
            - pandas
    """

    if source == 'app':
        # Pre process data read csv file, convert to datetime type, renameing column, merge the data
        df_android = await asyncio.to_thread(pd.read_csv, './csv/dau_mau_android.csv', delimiter=',', index_col=False)
        await asyncio.to_thread(lambda: df_android.rename(columns={'daily_active_user':'daily_active_user_android', 'monthly_active_user':'monthly_active_user_android'}, inplace=True))
        df_android["date"] = await asyncio.to_thread(pd.to_datetime, df_android['date'])
        df_android['date'] = await asyncio.to_thread(lambda: df_android['date'].dt.date)
        df_ios = await asyncio.to_thread(pd.read_csv, './csv/dau_mau_ios.csv', delimiter=',', index_col=False)
        await asyncio.to_thread(lambda: df_ios.rename(columns={'daily_active_user':'daily_active_user_ios', 'monthly_active_user':'monthly_active_user_ios'}, inplace=True))
        df_ios['date'] = await asyncio.to_thread(pd.to_datetime, df_ios['date'])
        df_ios['date'] = await asyncio.to_thread(lambda: df_ios['date'].dt.date)
        df_app = await asyncio.to_thread(lambda: pd.merge(df_android, df_ios, on=['date'], how='outer'))
        # Calculate DAU, MAU, & Stickieness
        df_app['daily_active_user'] = await asyncio.to_thread(lambda: df_app['daily_active_user_ios'] + df_app['daily_active_user_android'])
        df_app['monthly_active_user'] = await asyncio.to_thread(lambda: df_app['monthly_active_user_ios'] + df_app['monthly_active_user_android'])
        df_app['stickiness'] =await asyncio.to_thread(lambda:  df_app['daily_active_user'] / df_app['monthly_active_user'])
        # filtering the dataframe
        df_app = await asyncio.to_thread(lambda: df_app.loc[:, ['date', 'daily_active_user', 'monthly_active_user', 'stickiness']])
        filtered_df = await asyncio.to_thread(lambda: df_app[(df_app['date'] >= from_date) & (df_app['date'] <= to_date)])
    elif source == 'web':
        # Pre process data. Read CSV and convert date object to datetime type
        df_web = await asyncio.to_thread(pd.read_csv, './csv/dau_mau_web.csv', delimiter=',', index_col=False)
        df_web['date'] = await asyncio.to_thread(pd.to_datetime, df_web['date'])
        df_web['date'] = await asyncio.to_thread(lambda: df_web['date'].dt.date)
        # Calculate stickieness
        df_web['stickiness'] = await asyncio.to_thread(lambda: df_web['daily_active_user'] / df_web['monthly_active_user'])
        # filtering the dataframe
        filtered_df = await asyncio.to_thread(lambda: df_web[(df_web['date'] >= from_date) & (df_web['date'] <= to_date)])
    elif source == 'all':
        # Pre process data. Read CSV and convert date object to datetime type, merege the data
        df_android = await asyncio.to_thread(pd.read_csv, './csv/dau_mau_android.csv', delimiter=',', index_col=False)
        await asyncio.to_thread(lambda: df_android.rename(columns={'daily_active_user':'daily_active_user_android', 'monthly_active_user':'monthly_active_user_android'}, inplace=True))
        df_android['date'] = await asyncio.to_thread(pd.to_datetime, df_android['date'])
        df_android['date'] = await asyncio.to_thread(lambda: df_android['date'].dt.date)
        df_ios = await asyncio.to_thread(pd.read_csv, './csv/dau_mau_ios.csv', delimiter=',', index_col=False)
        await asyncio.to_thread(lambda: df_ios.rename(columns={'daily_active_user':'daily_active_user_ios', 'monthly_active_user':'monthly_active_user_ios'}, inplace=True))
        df_ios['date'] = await asyncio.to_thread(pd.to_datetime, df_ios['date'])
        df_ios['date'] = await asyncio.to_thread(lambda: df_ios['date'].dt.date)
        df_web = await asyncio.to_thread(pd.read_csv, './csv/dau_mau_web.csv', delimiter=',', index_col=False)
        df_web['date'] = await asyncio.to_thread(pd.to_datetime, df_web['date'])
        df_web['date'] = await asyncio.to_thread(lambda: df_web['date'].dt.date)
        await asyncio.to_thread(lambda: df_web.rename(columns={'daily_active_user':'daily_active_user_web', 'monthly_active_user':'monthly_active_user_web'}, inplace=True))
        df_merge = await asyncio.to_thread(lambda: pd.merge(df_android, df_ios, on=['date'], how='outer'))
        df_merge['date'] = await asyncio.to_thread(pd.to_datetime, df_merge['date'])
        df_merge['date'] = await asyncio.to_thread(lambda: df_merge['date'].dt.date)
        df_all = await asyncio.to_thread(lambda: pd.merge(df_merge, df_web, on='date', how='outer'))
        # Calculate MAU, DAU & Sticieness
        df_all['daily_active_user'] = await asyncio.to_thread(lambda: df_all['daily_active_user_ios'] + df_all['daily_active_user_android'] + df_all['daily_active_user_web'])
        df_all['monthly_active_user'] = await asyncio.to_thread(lambda: df_all['monthly_active_user_ios'] + df_all['monthly_active_user_android'] + df_all['monthly_active_user_web'])
        df_all['stickiness'] =await asyncio.to_thread(lambda:  df_all['daily_active_user'] / df_all['monthly_active_user'])
        # Filtering the data
        df_all = await asyncio.to_thread(lambda: df_all.loc[:, ['date', 'daily_active_user', 'monthly_active_user', 'stickiness']])
        filtered_df = await asyncio.to_thread(lambda: df_all[(df_all['date'] >= from_date) & (df_all['date'] <= to_date)])

    # Make default data if dataframe empty
    if filtered_df.empty:
        filtered_df['date'] = pd.date_range(from_date, to_date).date
        filtered_df['daily_active_user'] = 0
        filtered_df['monthly_active_user'] = 0
        filtered_df['stickiness'] = 0

    df = await asyncio.to_thread(lambda: filtered_df[(filtered_df['date'] >= from_date) & (filtered_df['date'] <= to_date)])
    stickiness = await asyncio.to_thread(lambda: filtered_df[filtered_df['date'] == to_date])
    data_stickiness = {
        "last_day": float(round(stickiness["stickiness"].item(), 4)) if to_date in stickiness["date"].values else 0,
        "average": float(round(df["stickiness"].mean(), 4))
    }
    
    return df if data == "dataframe" else data_stickiness


async def ga4_mau_dau_df(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        data: str = "dataframe",
        source: str = None):
    """
    Generate a DataFrame containing Google Analytics 4 (GA4) Daily Active Users (DAU) and Monthly Active Users (MAU) data.

    This function calculates DAU, MAU,
    and stickiness (DAU/MAU) for each date within the specified date range. If no data is available for the specified
    range, it generates default data with zeros.

    Args:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (str): The start date of the period in 'YYYY-MM-DD' format.
        to_date (str): The end date of the period in 'YYYY-MM-DD' format.
        data (str, optional): The data to return either 'dataframe' or 'stickiness'.
        source (str, optional): The source of the data ('app', 'web', or 'all'). Defaults to 'app'.

    Returns:
        pandas.DataFrame: A DataFrame containing columns 'date', 'active28DayUsers', 'active1DayUsers', and 'stickiness'.

    Example:
        ga4_mau_dau_df(from_date='2024-01-01', to_date='2024-01-31')
    """

    # Initiate the data
    query = select(
        Ga4ActiveUserData.date.label("date"), 
        Ga4ActiveUserData.platform.label("platform"), 
        Ga4ActiveUserData.active_1day_users.label("active1DayUsers"), 
        Ga4ActiveUserData.active_28day_users.label("active28DayUsers")
    ).filter(func.date(Ga4ActiveUserData.date).between(from_date, to_date))
    result = await session.execute(query)
    result_data = result.fetchall()

    df = pd.DataFrame(result_data)

    if df.empty:
        df = pd.DataFrame({
            "date": pd.date_range(to_date, to_date).date,
            "platform": ["app"],
            "active1DayUsers": [0],
            "active28DayUsers": [0]
        })
    
    df['platform'] = await asyncio.to_thread(lambda: df['platform'].str.lower())
    await asyncio.to_thread(lambda: df.replace({"platform": ["android", "ios"]}, value="app", inplace=True))
    df = await asyncio.to_thread(lambda: df[df['platform'] == source])
    
    #  Group data by date
    df_group = await asyncio.to_thread(
        lambda: df.groupby(["date"]).agg(
            active28DayUsers=("active28DayUsers", "sum"),
            active1DayUsers=("active1DayUsers", "sum")
        ).reset_index())
    # convert date column to datetime type and merge data
    df_group['date'] = await asyncio.to_thread(pd.to_datetime, df_group['date'])
    df_group['date'] = await asyncio.to_thread(lambda: df_group['date'].dt.date)
    # Calculate stickieness
    df_group['stickiness'] = await asyncio.to_thread(lambda: df_group['active1DayUsers'] / df_group['active28DayUsers'])
    await asyncio.to_thread(lambda: df_group.fillna(0, inplace=True))
    # filtering the data by date
    df_filter = await asyncio.to_thread(lambda: df_group[(df_group['date'] >= from_date) & (df_group['date'] <= to_date)])
    df_filter['date'] = await asyncio.to_thread(pd.to_datetime, df_filter['date'])
    df_filter['date'] = await asyncio.to_thread(lambda: df_filter['date'].dt.date)
    
    stickiness = await asyncio.to_thread(lambda: df_group[df_group["date"] == to_date])
    data_stickiness = {
        "last_day": float(round(stickiness["stickiness"].item(), 4)) if to_date in stickiness["date"].values else 0,
        "average": float(round(df_filter["stickiness"].mean(), 4))
    }
    
    if df_filter.empty:
        df_filter['date'] = pd.date_range(from_date, to_date)
        df_filter['active28DayUsers'] = 0
        df_filter['active1DayUsers'] = 0
        df_filter['stickiness']= 0.00
    
    return df_filter if data == "dataframe" else data_stickiness


async def dg_stickiness(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        file: str,
        source: str
):
    """
    Calculate the daily growth percentage for stickiness data between the current and previous week.

    This function fetches data for a specified date range (`from_date` to `to_date`) and compares it with the previous 
    week to calculate the growth percentage for each key. The data source can be either 'moe' or 'ga4', and the function 
    ensures that both datasets have matching keys before computing the percentage change.

    Parameters
    ----------
    session : AsyncSession
        The SQLAlchemy async session used to interact with the database.
    from_date : datetime.date
        The starting date of the current week for which the stickiness data is being fetched.
    to_date : datetime.date
        The ending date of the current week for which the stickiness data is being fetched.
    file : str
        Indicates the type of data source ('moe' or 'ga4') from which to fetch the stickiness data.
    source : str
        The identifier for the data source ('app', 'web', or 'all')

    Returns
    -------
    growth_percentage : dict
        A dictionary where each key represents an identifier, and the corresponding value is the daily growth percentage 
        of stickiness between the current and previous week. The percentage values are rounded to 4 decimal places.

    Raises
    ------
    ValueError
        If the keys in the current and previous week datasets do not match.

    Notes
    -----
    - When the `file` is set to "moe", the function fetches stickiness data using the `dau_mau_df` function.
    - When the `file` is set to "ga4", the function fetches stickiness data using the `ga4_mau_dau_df` function.
    - The function calculates the growth percentage using the formula: 
      `(new_value - old_value) / old_value`, and it returns 0 if the `old_value` is 0 to avoid division errors.

    Example
    -------
    >>> await dg_stickiness(session, from_date=date(2024, 1, 1), to_date=date(2024, 1, 7), file='moe', source='app')
    {'metric_1': 0.1234, 'metric_2': -0.5678, ...}
    """
    # Calculate date range for the previous week
    delta = (to_date - from_date) + timedelta(days=1)
    fromdate_lastweek = from_date - delta
    todate_lastweek = to_date - delta

    if file == "moe":
        # Fetch data for the current and previous week
        current_data = await dau_mau_df(from_date=from_date, to_date=to_date, data="stickiness", source=source)
        last_week_data = await dau_mau_df(from_date=fromdate_lastweek, to_date=todate_lastweek, data="stickiness", source=source)
    elif file == "ga4":
        current_data = await ga4_mau_dau_df(session=session, from_date=from_date, to_date=to_date, data="stickiness", source=source)
        last_week_data = await ga4_mau_dau_df(session=session, from_date=fromdate_lastweek, to_date=todate_lastweek, data="stickiness", source=source)

    # Check if both datasets have the same keys
    if set(current_data.keys()) != set(last_week_data.keys()):
        raise ValueError("Data from different periods must have the same keys")

    # Calculate daily growth percentage
    growth_percentage = {}
    for key in current_data:
        old_value = last_week_data[key]
        new_value = current_data[key]

        if old_value == 0:
            percentage = 0
        else:
            percentage = (new_value - old_value) / old_value
        
        growth_percentage[key] = round(percentage, ndigits=4)
        
    return growth_percentage


async def dau_mau_chart(
        from_date: datetime.date, 
        to_date: datetime.date, 
        source: str = 'app') -> str:
    """
    Generate a chart displaying Daily Active Users (DAU) and Monthly Active Users (MAU).

    This function generates a chart displaying the trends of Daily Active Users (DAU) and
    Monthly Active Users (MAU) over a specified date range and for a specified source.

    Args:
        from_date (str, optional): The start date of the date range in 'YYYY-MM-DD' format. Defaults to '2023-01-09'.
        to_date (str, optional): The end date of the date range in 'YYYY-MM-DD' format. Defaults to '2023-02-23'.
        source (str, optional): The source of the data ('app', 'web', or 'all'). Defaults to 'app'.

    Returns:
        str: A JSON representation of the chart.

    Note:
        Requires the dau_mau_df function to be properly implemented.

    Example:
        dau_mau_chart(from_date='2023-01-09', to_date='2023-02-23', source='app')
    """

    # filtering dataframe and convert date column to datetime type 
    filtered_df = await dau_mau_df(from_date=from_date, to_date=to_date, source=source)
    filtered_df['date'] = await asyncio.to_thread(pd.to_datetime, filtered_df['date'])
    filtered_df['date'] = await asyncio.to_thread(lambda: filtered_df['date'].dt.date)

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=filtered_df.date, y=filtered_df.daily_active_user, line=dict(color='blue'),
                   name='Daily Active User', mode='lines+markers', text=filtered_df['daily_active_user'].apply(lambda x: "{:,.0f}".format((x)))),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=filtered_df.date, y=filtered_df.monthly_active_user, line=dict(color='red'),
                   name='Monthly Active User', mode='lines+markers', text=filtered_df['monthly_active_user'].apply(lambda x: "{:,.0f}".format((x)))),
        secondary_y=True
    )
    
    if from_date in filtered_df['date'].values and to_date in filtered_df['date'].values:
        fig.update_xaxes(title='Date', dtick='D1')
    else:
        fig.update_xaxes(title='Date', dtick='D1')
    fig.update_yaxes(title='Active Users')

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def ga4_mau_dau(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        source: str = 'app'):
    """
    Generate a chart showing 1-day and 28-day active users over a specified period.
    
    Args:
        from_date (datetime.date): The start date for the data range.
        to_date (datetime.date): The end date for the data range.
        source (str): The Source of data to return ('app', 'web', or 'all')
    
    Returns:
        str: The chart in JSON format.
    """

    # Initiate the data
    df_filter = await ga4_mau_dau_df(session=session, from_date=from_date, to_date=to_date, source=source)

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=df_filter['date'], y=df_filter['active1DayUsers'],
                   name='1 Day Active User', mode='lines+markers', 
                   text=df_filter['active1DayUsers'].apply(lambda x: "{:,.0f}".format((x)))),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=df_filter['date'], y=df_filter['active28DayUsers'],
                   name='28 Day Active User', mode='lines+markers', 
                   text=df_filter['active28DayUsers'].apply(lambda x: "{:,.0f}".format((x)))),
        secondary_y=True
    )
    fig.update_xaxes(title='Date', dtick='D1')
    fig.update_yaxes(title='Active Users')

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def install_chart(data: pd.DataFrame):
    """
    Generate a chart displaying various sources of app installations over a specified date range.

    This function collects data on app installations from different sources such as organic,
    Facebook Ads, Google Ads, TikTok Ads, Apple Search Ads, referrals, and undetected sources,
    aggregates the data, and generates a chart displaying the total installations over time.

    Args:
        data (pd.DataFrame): Pandas daatframe that contain install data.

    Returns:
        str: A JSON representation of the chart.

    Note:
        Requires the organic_df function to be properly implemented.
        Requires CSV files for Facebook Ads, Google Ads, TikTok Ads, Apple Search Ads, and ASA report.

    Example:
        install_chart(from_date='2023-01-01', to_date='2023-01-31')
    """
    # create the chart
    fig = go.Figure(data=[
        go.Bar(x=data['date'], y=data['total_install'], name='Total Install', text=data['total_install'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside')
    ])
    fig.update_layout(title='Installs /Days', barmode='stack')
    fig.update_xaxes(title='Date', dtick='D1')
    fig.update_yaxes(title='Total install')

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def revenue_cost_periods_chart(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        app_coin_data: pd.DataFrame, 
        web_coin_data: pd.DataFrame):
    """
    Generate a chart showing the revenue, cost, and cost-to-revenue ratio over a specified time period.

    This function retrieves revenue and cost data for the given time period and generates a chart
    showing the total revenue, total cost, and the cost-to-revenue ratio for each day within the period.

    Args:
        from_date (str, optional): The start date of the time period in 'YYYY-MM-DD' format. Defaults to None.
        to_date (str, optional): The end date of the time period in 'YYYY-MM-DD' format. Defaults to None.
        app_coin_data, web_coin_data: An Dict with revenue and cost data.

    Returns:
        str: A JSON representation of the generated chart.

    Example:
        revenue_cost_periods_chart(from_date='2023-01-01', to_date='2023-01-31', object_1=my_data_object)
    """
    
    # initiate data app web revenue and merge the data 
    app_revenue_koin_df = app_coin_data['cost_revenue']
    app_revenue_koin_df = pd.DataFrame(app_revenue_koin_df)
    await asyncio.to_thread(lambda: app_revenue_koin_df.rename(columns={'total_rev_koin':'app_rev'}, inplace=True))
    web_revenue_koin_df = web_coin_data['cost_revenue']
    web_revenue_koin_df = pd.DataFrame(web_revenue_koin_df)
    await asyncio.to_thread(lambda: web_revenue_koin_df.rename(columns={'total_rev_koin':'web_rev'}, inplace=True))
    revenue_koin_df = await asyncio.to_thread(lambda: pd.merge(app_revenue_koin_df, web_revenue_koin_df, on='date_start', how='outer'))
    await asyncio.to_thread(lambda: revenue_koin_df.fillna(0, inplace=True))
    revenue_koin_df["app_rev"] = await asyncio.to_thread(lambda: revenue_koin_df["app_rev"].astype(int))
    revenue_koin_df["web_rev"] = await asyncio.to_thread(lambda: revenue_koin_df["web_rev"].astype(int))
    revenue_koin_df['total_rev_koin'] = await asyncio.to_thread(lambda: revenue_koin_df['app_rev'] + revenue_koin_df['web_rev'])
    revenue_koin_df = await asyncio.to_thread(lambda: revenue_koin_df.loc[:, ['date_start', 'total_rev_koin']])
    revenue_koin_df['date_start'] = await asyncio.to_thread(pd.to_datetime, revenue_koin_df['date_start'])
    revenue_koin_df['date_start'] = await asyncio.to_thread(lambda: revenue_koin_df['date_start'].dt.date)

    if revenue_koin_df.empty:
        revenue_koin_df['date_start'] = pd.date_range(from_date, to_date)
        revenue_koin_df['date_start'] = pd.to_datetime(revenue_koin_df['date_start']).dt.date
        revenue_koin_df['total_rev_koin'] = 0

    # Pre Process admob revenue
    query_admob = select(
        AdmobReportData.date.label("Date"),
        AdmobReportData.platform.label("Platform"),
        AdmobReportData.estimated_earnings.label("Estimated earnings"),
        AdmobReportData.impressions.label("Impressions"),
        AdmobReportData.observed_ecpm.label("Observed ECPM"),
        AdmobReportData.impression_ctr.label("Impression CTR"),
        AdmobReportData.clicks.label("Clicks"),
        AdmobReportData.ad_requests.label("Ad requests"),
        AdmobReportData.match_rate.label("Match rate"),
        AdmobReportData.match_requests.label("Matched requests"),
    ).filter(
        AdmobReportData.date.between(from_date, to_date)
    )
    result_admob = await session.execute(query_admob)
    data_admob = result_admob.fetchall()
    df_admob = pd.DataFrame(data_admob)
    if df_admob.empty:
        df_admob = pd.DataFrame({
                "Date": pd.date_range(to_date,to_date).date,
                "Platform": ["-"],
                "Estimated earnings": [0],
                "Impressions": [0],
                "Observed ECPM": [0],
                "Impression CTR": [0],
                "Clicks": [0],
                "Ad requests": [0],
                "Match rate": [0],
                "Matched requests": [0]
            })

    await asyncio.to_thread(lambda: df_admob.sort_values(by='Date', ascending=True, inplace=True))
    df_admob['Date'] = await asyncio.to_thread(pd.to_datetime, df_admob['Date'])
    df_admob["Date"] = await asyncio.to_thread(lambda: df_admob['Date'].dt.date)
    df_admob['Estimated earnings'] = await asyncio.to_thread(lambda: df_admob['Estimated earnings'] / 1000000)
    df_admob['Estimated earnings'] = await asyncio.to_thread(lambda: df_admob['Estimated earnings'].round(2))
    df_loc = await asyncio.to_thread(lambda: df_admob.loc[:, ['Date', 'Estimated earnings']])
    df_loc['Date'] = await asyncio.to_thread(pd.to_datetime, df_loc['Date'])
    df_loc['Date'] = await asyncio.to_thread(lambda: df_loc['Date'].dt.date)
    df_group_admob = await asyncio.to_thread(lambda: df_loc.groupby(['Date'])['Estimated earnings'].sum().reset_index())
    await asyncio.to_thread(lambda: df_group_admob.rename(columns={'Date':'date_start'}, inplace=True))
    df_filter_admob = await asyncio.to_thread(lambda: df_group_admob[(df_group_admob['date_start'] >= from_date) & (df_group_admob['date_start'] <= to_date)])
    if df_filter_admob.empty:
        df_filter_admob['date_start'] = pd.date_range(from_date, to_date).date
        df_filter_admob['Estimated earnings'] = 0

    # Pre Process adsense revenue
    query_adsense = select(
        AdsenseReportData.date.label("DATE"),
        AdsenseReportData.estimated_earnings.label("ESTIMATED_EARNINGS"),
    ).filter(
        AdsenseReportData.date.between(from_date, to_date)
    )
    result_adsense = await session.execute(query_adsense)
    data_adsense = result_adsense.fetchall()
    df_adsense = pd.DataFrame(data_adsense)
    if df_adsense.empty:
        df_adsense = pd.DataFrame({
                "DATE": pd.date_range(to_date,to_date).date,
                "ESTIMATED_EARNINGS": [0]
            })

    await asyncio.to_thread(lambda: df_adsense.rename(columns={'DATE':'date_start'}, inplace=True))
    df_adsense = await asyncio.to_thread(lambda: df_adsense.groupby(['date_start'])['ESTIMATED_EARNINGS'].sum().reset_index())
    df_adsense['date_start'] = await asyncio.to_thread(pd.to_datetime, df_adsense['date_start'])
    df_adsense['date_start'] = await asyncio.to_thread(lambda: df_adsense['date_start'].dt.date)
    df_adsense = await asyncio.to_thread(lambda: df_adsense[(df_adsense['date_start'] >= from_date) & (df_adsense['date_start'] <= to_date)])
    if df_adsense.empty:
        df_adsense['date_start'] = pd.date_range(from_date, to_date).date
        df_adsense['ESTIMATED_EARNINGS'] = 0

    # merge all revenue data and calculate total revenue
    revenue1_df = await asyncio.to_thread(lambda: pd.merge(revenue_koin_df, df_filter_admob, on='date_start', how='outer'))
    revenue1_df['date_start'] = await asyncio.to_thread(pd.to_datetime, revenue1_df['date_start'])
    revenue1_df['date_start'] = await asyncio.to_thread(lambda: revenue1_df['date_start'].dt.date)
    revenue_df = await asyncio.to_thread(lambda: pd.merge(revenue1_df, df_adsense, on='date_start', how='outer'))
    revenue_df['date_start'] = await asyncio.to_thread(pd.to_datetime, revenue_df['date_start'])
    revenue_df['date_start'] = await asyncio.to_thread(lambda: revenue_df['date_start'].dt.date)
    await asyncio.to_thread(lambda: revenue_df.fillna(0, inplace=True))
    revenue_df['total'] = await asyncio.to_thread(lambda: revenue_df['total_rev_koin'] + revenue_df['Estimated earnings'].astype(int) + revenue_df['ESTIMATED_EARNINGS'].astype(int))
    
    # merge all the data cost & revenue
    df_spend = await cost(session=session, from_date=from_date, to_date=to_date, data="dataframe")
    if df_spend.empty:
        df_spend = pd.DataFrame({
            "date": pd.date_range(to_date,to_date).date,
            "google": [0],
            "facebook": [0],
            "tiktok": [0],
            "asa": [0],
            "total_spend": [0]
        })
    
    await asyncio.to_thread(lambda: df_spend.rename(columns={"date": "date_start"}, inplace=True))
    df_spend["date_start"] = await asyncio.to_thread(pd.to_datetime, df_spend["date_start"])
    df_spend["date_start"] = await asyncio.to_thread(lambda: df_spend["date_start"].dt.date)
    full_merged = await asyncio.to_thread(lambda: pd.merge(df_spend, revenue_df, how='outer', on='date_start'))
    full_merged['date_start'] = await asyncio.to_thread(pd.to_datetime, full_merged['date_start'])
    full_merged['date_start'] = await asyncio.to_thread(lambda: full_merged['date_start'].dt.date)
    await asyncio.to_thread(lambda: full_merged.fillna(0, inplace=True))
    full_merged['total'] = await asyncio.to_thread(lambda: full_merged['total'].astype(float))
    full_merged['cost_to_revenue'] = await asyncio.to_thread(lambda: full_merged.total / full_merged.total_spend)
    await asyncio.to_thread(lambda: full_merged.fillna(0, inplace=True))
    await asyncio.to_thread(lambda: full_merged.sort_values('date_start', ascending=True, inplace=True))

    # create the chart
    trace1 = go.Bar(
        x=full_merged['date_start'],
        y=full_merged['total_spend'],
        name='Cost',
        yaxis='y',
        text=full_merged['total_spend'].apply(lambda x: "Rp. {:,.0f}".format((x))),
        textposition='inside'
    )

    trace2 = go.Scatter(
        x=full_merged['date_start'],
        y=full_merged['cost_to_revenue'],
        name='Cost To Revenue',
        yaxis='y2',
        # Set the y-axis format to be a percentage with 2 decimal places
        hovertemplate='%{y:.2%}',
        text=full_merged['cost_to_revenue'],
        textposition='middle center'
    )

    trace3 = go.Bar(
        x=full_merged['date_start'],
        y=full_merged['total'],
        name='Revenue',
        yaxis='y',
        text=full_merged['total'].apply(lambda x: "Rp. {:,.0f}".format((x))),
        textposition='outside'
    )

    # Define the layout with a secondary y-axis
    layout = go.Layout(
        title='Cost To Revenue Per Hari',
        yaxis=dict(
            title='Cost'
        ),
        yaxis2=dict(
            title='Cost To Revenue',
            overlaying='y',
            side='right',
            # Set the y-axis format to be a percentage with 2 decimal places
            tickformat='.0%'
        )
    )

    # Combine the traces and layout into a Figure object
    fig = go.Figure(data=[trace1, trace2, trace3], layout=layout)

    fig.update_layout(barmode='stack', legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.01
    ))

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def revenue_cost_chart():
    """
    Generate a chart showing the revenue, cost, and cost-to-revenue ratio for the last 7 days.

    This function retrieves revenue, cost, and cost-to-revenue ratio data for the last 7 days and generates
    a chart showing the total revenue, total cost, and the cost-to-revenue ratio for each day.

    Returns:
        str: A JSON representation of the generated chart.

    Example:
        revenue_cost_chart()
    """

    # initiate the data
    read_csv = await asyncio.to_thread(pd.read_csv, './csv/cost_revenue.csv', delimiter=',')
    df = pd.DataFrame(read_csv)
    df['revenue_to_cost'] = await asyncio.to_thread(pd.to_numeric, df['revenue_to_cost'])

    # create the chart
    trace1 = go.Bar(
        x=df['date'][-7:],
        y=df['cost'][-7:],
        name='Cost',
        yaxis='y',
        text=df['cost'][-7:].apply(lambda x: "Rp. {:,.0f}".format((x))),
        textposition='inside'
    )

    trace2 = go.Scatter(
        x=df['date'][-7:],
        y=df['revenue_to_cost'][-7:],
        name='Cost To Revenue',
        yaxis='y2',
        # Set the y-axis format to be a percentage with 2 decimal places
        hovertemplate='%{y:.2%}',
        text=df['revenue_to_cost'][-7:],
        textposition='middle center'
    )

    trace3 = go.Bar(
        x=df['date'][-7:],
        y=df['revenue'][-7:],
        name='Revenue',
        yaxis='y',
        text=df['revenue'][-7:].apply(lambda x: "Rp. {:,.0f}".format((x))),
        textposition='outside'
    )

    # Define the layout with a secondary y-axis
    layout = go.Layout(
        title='Cost To Revenue Per Minggu',
        yaxis=dict(
            title='Cost'
        ),
        yaxis2=dict(
            title='Cost To Revenue',
            overlaying='y',
            side='right',
            # Set the y-axis format to be a percentage with 2 decimal places
            tickformat='.0%'
        )
    )

    # Combine the traces and layout into a Figure object
    fig = go.Figure(data=[trace1, trace2, trace3], layout=layout)

    fig.update_layout(barmode='stack', legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.01
    ))

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def payment_channel(app_coin_data: pd.DataFrame, web_coin_data: pd.DataFrame):
    """
    Generate a pie chart showing the distribution of payment channels for transactions.

    This function retrieves payment channel data for a specified date range and source (app or web) using
    the provided object_1. It then generates a pie chart to visualize the distribution of payment channels
    based on the total number of transactions.

    Args:
        app_coin_data: An Dict used to retrieve app payment channel data.
        web_coin_data: An Dict used to retrieve web payment channel data.

    Returns:
        str: A JSON representation of the pie chart.

    Example:
        payment_channel(from_date=date(2024, 1, 1), to_date=date(2024, 12, 31), object_1=my_object, source='app')
    """

    # Initiate app web data and merge it
    df_app = app_coin_data['payment_channel']
    df_app = pd.DataFrame(df_app)
    df_web = web_coin_data['payment_channel']
    df_web = pd.DataFrame(df_web)
    df = await asyncio.to_thread(lambda: pd.merge(df_app, df_web, on=['payment_channel', 'total_transaksi'], how='outer'))

    # group the data by payment channel and sum to total transaksi
    df = await asyncio.to_thread(lambda: df.groupby(['payment_channel'])['total_transaksi'].sum().reset_index())

    if df.empty:
        df['payment_channel'] = '-'
        df['total_transaksi'] = 0
    
    df['payment_channel'] = df['payment_channel'].str.upper()

    # Create the chart
    fig = go.Figure(
        go.Pie(labels=df[0:10].payment_channel, values=df[0:10].total_transaksi)
    )

    fig.update_layout(title='Payment Channel')

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart
