"""seo function file"""
from datetime import datetime, timedelta
import pandas as pd
import json
import plotly
import plotly.graph_objects as go
import re
import warnings
import asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.acquisition import Ga4AnalyticsData, Ga4LandingPageData
warnings.simplefilter(action='ignore', category=FutureWarning)


def range_of_date(delta: int = 7):
    """
    Get a range of date

    Parameters:
        delta (int): Number of the date to return.
    
    Retruns:
        Pd.Dataframe: A DataFrame containing a range of date.
    """
    # Get the current date
    current_date = datetime.now().date()

    # Calculate the number of days since the last Monday
    days_since_last_monday = current_date.weekday() + delta  # Monday is 0, so add 7 to go back to last week

    # Get the previous Monday
    last_monday = current_date - timedelta(days=days_since_last_monday)

    # Get the last Sunday
    last_sunday = last_monday + timedelta(days=6)
    df = pd.date_range(last_monday, last_sunday).strftime("%-d-%-m-%Y")

    return df


async def ga4_df(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date):
    """
    Read Google Analytics data from a CSV file and filter it based on the specified date range and platform.

    Parameters:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date of the date range.
        to_date (datetime.date): The end date of the date range.

    Returns:
        pandas.DataFrame: A DataFrame containing the filtered Google Analytics data.
    """
    query = select(
        Ga4AnalyticsData.date.label("date"),
        Ga4AnalyticsData.device_category.label("device_category"),
        Ga4AnalyticsData.platform.label("platform"),
        Ga4AnalyticsData.source.label("source"),
        Ga4AnalyticsData.sessions.label("sessions"),
        Ga4AnalyticsData.new_user.label("new_user"),
        Ga4AnalyticsData.active_user.label("active_user"),
        Ga4AnalyticsData.total_user.label("total_user"),
        Ga4AnalyticsData.bounce_rate.label("bounce_rate"),
        Ga4AnalyticsData.avg_sesseion_duration.label("avg_session_duration"),
        Ga4AnalyticsData.engaged_session.label("engaged_session"),
        Ga4AnalyticsData.user_enagged_duration.label("user_enagged_duration")
    ).filter(
        Ga4AnalyticsData.date.between(from_date, to_date),
        Ga4AnalyticsData.platform == "web"
    )
    result = await session.execute(query)
    data = result.fetchall()
    df = pd.DataFrame(data)
    if df.empty:
        df = pd.DataFrame({
            "date": pd.date_range(to_date, to_date).date,
            "device_category": ["-"],
            "platform": ["-"],
            "source": ["-"],
            "sessions": [0],
            "new_user": [0],
            "active_user": [0],
            "total_user": [0],
            "bounce_rate": [0],
            "avg_session_duration": [0],
            "engaged_session": [0],
            "user_enagged_duration": [0]
        })
    await asyncio.to_thread(lambda: df.sort_values(by='date', ascending=True, inplace=True))

    # Format datetime into date only
    df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
    df['date'] = await asyncio.to_thread(lambda: df['date'].dt.date)
    
    return df


async def ga4_metrics(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        metrics: str = ''):
    """
    Calculate Google Analytics metrics based on the specified date range and metric type.

    Parameters:
        session (AsyncSession): The asynchronous SQLite session.
        from_dates (datetime.date): The start date of the date range.
        to_dates (datetime.date): The end date of the date range.
        metrics (str): The type of metric to calculate ('sessions', 'total_user', 'new_user', 'source', 'bounce_rate').

    Returns:
        float: The calculated value of the specified metric.
    """
    # Get Google Analytics data for the specified date range
    df = await ga4_df(session=session, from_date=from_date, to_date=to_date)
    
    data = await asyncio.gather(
        asyncio.to_thread(lambda: df['sessions'].sum()),  # Total sessions
        asyncio.to_thread(lambda: df['total_user'].sum()),  # Total users
        asyncio.to_thread(lambda: df['new_user'].sum()),  # Total new users
        asyncio.to_thread(lambda: df[df['source'] == 'Organic Search']['sessions'].sum()),  # Sessions from Organic Search
        asyncio.to_thread(lambda: df['bounce_rate'].mean())  # Average bounce rate
    )
    
    # Calculate different metrics based on the specified type
    value = {
        'sessions': int(data[0]),
        'total_user': int(data[1]),
        'new_user': int(data[2]),
        'source': int(data[3]),
        'bounce_rate': float(round(data[4], 4))
    }
    
    # Return the calculated value for the specified metric
    return value[metrics]


async def indexing_df(week: int = -1):
    """
    Read data from the 'indexing.csv' file and return it as a DataFrame.

    Parameters:
        week (int): The week number for which to retrieve data. Default is -1 for the latest week.

    Returns:
        pandas.DataFrame: DataFrame containing the data from the 'indexing.csv' file.
    """
    read_df = pd.read_csv('./csv/indexing.csv')
    df = pd.DataFrame(read_df)
    await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
    df['Indexing'] = await asyncio.to_thread(lambda: df['Indexing'].replace(re.compile(r'[\,]'), '', regex=True))
    df['Indexing'] = await asyncio.to_thread(lambda: df['Indexing'].astype(int))
    df['DR'] = await asyncio.to_thread(lambda: df['DR'].astype(int))
    df['Reffering Domains'] = await asyncio.to_thread(lambda: df['Reffering Domains'].astype(int))
    df['Backlinks'] = await asyncio.to_thread(lambda: df['Backlinks'].astype(int))
    
    return df


async def ranking_df():
    """
    Load and preprocess the ranking data from a CSV file.

    Returns:
        pandas.DataFrame: A DataFrame containing the ranking data.

    This function reads the ranking data from a CSV file located at './csv/ranking.csv'.
    It preprocesses the data by replacing '-' with 0 and converting the columns '26-5-2024' and '2-6-2024' to integer type.
    The resulting DataFrame contains the processed ranking data.
    """
    range_last_week_date = range_of_date(7)
    range_last2_week_date = range_of_date(14)
    
    # Read the ranking data from the CSV file
    df = pd.read_csv('./csv/ranking.csv', index_col=False)
    
    # Create a DataFrame to work with
    # df = pd.DataFrame(read_df).copy()
    
    # Replace '-' with 0 in the columns '26-5-2024' and '2-6-2024'
    await asyncio.to_thread(lambda: df.replace({range_last2_week_date[6]: "-"}, value=0, inplace=True))
    await asyncio.to_thread(lambda: df.replace({range_last_week_date[6]: "-"}, value=0, inplace=True))
    
    # Fill missing values with 0
    await asyncio.to_thread(lambda: df.fillna({range_last2_week_date[6] : 0}, inplace=True))
    await asyncio.to_thread(lambda: df.fillna({range_last_week_date[6] : 0,}, inplace=True))
    
    # Convert the columns '26-5-2024' and '2-6-2024' to integer type
    df[range_last2_week_date[6]] = await asyncio.to_thread(lambda: df[range_last2_week_date[6]].astype(int))
    df[range_last_week_date[6]] = await asyncio.to_thread(lambda: df[range_last_week_date[6]].astype(int))
    
    return df


async def dg_ga4_metrics(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        metrics=''):
    """
    Calculate the daily growth of Google Analytics metrics based on the specified date range and metric type.

    Parameters:
        session (AsyncSession): The asynchronous SQLite session.
        from_dates (datetime.date): The start date of the date range.
        to_dates (datetime.date): The end date of the date range.
        metricss (str): The type of metric to calculate the daily growth for.

    Returns:
        str: The calculated daily growth percentage formatted as a string.
    """
    # Calculate the duration of the date range
    delta = (to_date - from_date) + timedelta(1)

    # Calculate the start and end dates of the previous week
    fromdate_lastweek = from_date - delta
    todate_lastweek = to_date - delta

    # Get the metric value for the current and previous week
    w1 = await ga4_metrics(session=session, from_date=from_date, to_date=to_date, metrics=metrics)
    w2 = await ga4_metrics(session=session, from_date=fromdate_lastweek, to_date=todate_lastweek, metrics=metrics)

    # Calculate the daily growth percentage
    if w2 == 0:
        dg = 0
    else:
        dg = (w1 - w2) / w2

    # Format the result as a percentage string
    

    return dg


async def dg_indexing(column=''):
    """
    Calculate the daily growth indexing based on the given column.

    Parameters:
        column (str): The column name in the dataframe to calculate indexing for.

    Returns:
        str: A string representing the daily growth indexing percentage.

    This function computes the daily growth indexing by comparing the values of the given column 
    between the latest two entries in the dataframe. If the most recent value (w1) is zero, 
    indicating no growth, the indexing will be zero. Otherwise, it calculates the indexing 
    as the percentage change between w1 and w2 (the second latest value), divided by w1.
    """
    # Fetch the dataframe containing the data for indexing
    df = await indexing_df()
    
    # Get the latest value (w1) and the second latest value (w2) from the dataframe
    w1 = await asyncio.to_thread(lambda: df.iloc[-1][column])
    w2 = await asyncio.to_thread(lambda: df.iloc[-2][column])

    # Calculate the daily growth indexing
    if w2 == 0:
        dg = 0
    else:
        dg = (w1 - w2) / w2

    return dg


async def ga4_metrics_chart(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
):
    """
    Generate a chart displaying daily sessions and total users over a specified date range.

    Parameters:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date of the date range.
        to_date (datetime.date): The end date of the date range.

    Returns:
        str: The JSON representation of the generated chart.
    """
    # Load data from the CSV file
    df = await ga4_df(session=session, from_date=from_date, to_date=to_date)

    # Group data by date and aggregate metrics
    df_group_sessions = await asyncio.to_thread(lambda: df.groupby(['date'])['sessions'].sum().reset_index())
    df_group_total_user = await asyncio.to_thread(lambda: df.groupby(['date'])['total_user'].sum().reset_index())
    df_group_new_user = await asyncio.to_thread(lambda: df.groupby(['date'])['new_user'].sum().reset_index())

    # Merge aggregated dataframes
    df_merged = await asyncio.to_thread(lambda: pd.merge(df_group_sessions, df_group_new_user, how='outer', on='date'))
    full_merged = await asyncio.to_thread(lambda: pd.merge(df_merged, df_group_total_user, how='outer', on='date'))

    # Create traces for sessions and total users
    trace1 = go.Bar(x=full_merged['date'], y=full_merged['sessions'], name='Sessions', text=full_merged['sessions'].apply(lambda x: "{:,.0f}".format((x))), textposition='outside')
    trace2 = go.Scatter(x=full_merged['date'], y=full_merged['total_user'], name='Users', mode='lines+markers')

    # Define layout for the chart
    layout = go.Layout(title='Daily Sessions and Total Users',
                   xaxis=dict(title='Date'),
                   yaxis=dict(title='Value'))
    
    # Create a figure with the traces and layout
    fig = go.Figure(data=[trace1, trace2], layout=layout)
    fig.update_xaxes(dtick='D1')

    # Convert the figure to JSON format
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def ga4_source_chart(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
):
    """
    Generate a pie chart displaying sessions by traffic source over a specified date range.

    Parameters:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date of the date range.
        to_date (datetime.date): The end date of the date range.

    Returns:
        str: The JSON representation of the generated chart.
    """
    # Load data from the CSV file
    df = await ga4_df(session=session, from_date=from_date, to_date=to_date)

    # Group data by traffic source and calculate total sessions
    df_group = await asyncio.to_thread(lambda: df.groupby(['source'])['sessions'].sum().reset_index())

    # Rename the 'sessions' column to 'total'
    await asyncio.to_thread(lambda: df_group.rename(columns={'sessions': 'total'}, inplace=True))

    # Replace 'Unassigned' with 'other' for better visualization
    df_group['source'] = await asyncio.to_thread(lambda: df_group['source'].str.replace('Unassigned', 'other'))

    # Create a pie chart
    fig = go.Figure(go.Pie(labels=df_group['source'], values=df_group['total']))

    # Set title for the chart
    fig.update_layout(title='Sessions Source')

    # Convert the figure to JSON format
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def ga4_device_chart(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
):
    """
    Generate a pie chart displaying sessions by device category over a specified date range.

    Parameters:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date of the date range.
        to_date (datetime.date): The end date of the date range.

    Returns:
        str: The JSON representation of the generated chart.
    """
    # Load data from the CSV file
    df = await ga4_df(session=session, from_date=from_date, to_date=to_date)

    # Group data by device category and count the number of sessions
    df_group = await asyncio.to_thread(lambda: df.groupby(['device_category'])['source'].count().reset_index())

    # Rename the 'source' column to 'total'
    await asyncio.to_thread(lambda: df_group.rename(columns={'source': 'total'}, inplace=True))

    # Create a pie chart
    fig = go.Figure(go.Pie(labels=df_group['device_category'], values=df_group['total']))

    # Set title for the chart
    fig.update_layout(title='Platform')

    # Convert the figure to JSON format
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def landing_page_tbl(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        medium: str = ''):
    """
    Generate a table displaying the top 10 landing pages by sessions for a specific medium.

    Parameters:
        session (AsyncSession): The asynchronous SQLAlchemy session.
        from_date (datetime.date): The start date of the date range.
        to_date (datetime.date): The end date of the date range.
        medium (str): The medium for which the table is generated. Can be 'organic' or 'cpc'.

    Returns:
        str: The JSON representation of the generated table.
    """
    # Load landing page data from the CSV file
    
    query = select(
        Ga4LandingPageData.date.label("date"),
        Ga4LandingPageData.landing_page.label("landing_page"),
        Ga4LandingPageData.source.label("source"),
        Ga4LandingPageData.platform.label("platform"),
        Ga4LandingPageData.medium.label("medium"),
        Ga4LandingPageData.sessions.label("sessions"),
    ).filter(
        Ga4LandingPageData.date.between(from_date, to_date),
        Ga4LandingPageData.platform == "web",
        Ga4LandingPageData.source == "Organic Search"
    )
    result = await session.execute(query)
    data  = result.fetchall()
    df = pd.DataFrame(data)

    if df.empty:
        df = pd.DataFrame({
            "date": pd.date_range(to_date, to_date).date,
            "landing_page": ["-"],
            "source": ["-"],
            "platform": ["-"],
            "medium": ["-"],
            "sessions": [0],
        })

    # Group data by landing page and medium, and sum the sessions
    df_group = await asyncio.to_thread(lambda: df.groupby(['landing_page', 'medium'])['sessions'].sum().reset_index())

    # Sort the dataframe by sessions in descending order
    await asyncio.to_thread(lambda: df_group.sort_values(by='sessions', ascending=False, inplace=True))

    # Filter data based on the specified medium
    df_medium = await asyncio.to_thread(lambda: df_group[df_group['medium'].str.contains(medium)])

    # Create a table figure
    fig = go.Figure(
        go.Table(
            
            columnorder=[1, 2, 3],
            columnwidth=[150, 60, 60],
            header=dict(
                fill_color="grey",
                line_color="black",
                font=dict(color="black"),
                values=df_medium.columns),
            cells=dict(
                fill_color="white",
                line_color="black",
                font=dict(color="black"),
                values=[
                    df_medium['landing_page'][:10], 
                    df_medium['medium'][:10], 
                    df_medium['sessions'][:10]])
        )
    )

    # Set title based on the medium
    if medium == 'organic':
        fig.update_layout(title='Top 10 Landing Page By Organic')
    else:
        fig.update_layout(title='Top 10 Landing Page By CPC ')

    # Convert the figure to JSON format
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def ranking_group_week(week=''):
    """
    Group ranking data by a specified week and count occurrences.

    Parameters:
        week (str): The column name representing the week in the ranking data.

    Returns:
        pandas.DataFrame: A DataFrame containing the count of occurrences for each week.

    This function groups the ranking data by the specified week and counts the occurrences.
    It returns a DataFrame containing the count of occurrences for each week.
    """
    # Load the ranking data
    df = await ranking_df()
    
    # Group the data by the specified week and count occurrences
    week_count = await asyncio.to_thread(lambda: df.groupby([week]).agg(count=(week, "count")).reset_index())

    return week_count


async def ranking_chart():
    """
    Generate a stacked bar chart representing the ranking data.

    Returns:
        str: A JSON string representing the stacked bar chart.

    This function generates a stacked bar chart showing the distribution of rankings 
    over different weeks. It merges ranking data for multiple weeks and visualizes 
    the distribution of rankings within three groups: rank 1-3, rank 4-10, and rank 11-30.
    """
    async def merged(week=''):
        # Get ranking data grouped by week
        week_1 = await ranking_group_week(week=week)

        # Preprocess ranking data
        await asyncio.to_thread(lambda: week_1.replace({week : "-"}, value=0, inplace=True))
        await asyncio.to_thread(lambda: week_1.fillna({week : 0}, inplace=True))
        week_1[week] = await asyncio.to_thread(lambda: week_1[week].astype(int))
        
        # Filter data into different rank groups
        week_1_rank1_rank3 = await asyncio.to_thread(lambda: week_1[week_1[week].between(1,3)])
        week_1_rank1_rank3['date'] = week
        week_1_rank1_rank3_group = await asyncio.to_thread(lambda: week_1_rank1_rank3.groupby(['date'])['count'].sum().reset_index())
        await asyncio.to_thread(lambda: week_1_rank1_rank3_group.rename(columns={'count':'rank_1_3'}, inplace=True))

        week_1_rank4_rank10 = await asyncio.to_thread(lambda: week_1[week_1[week].between(4,10)].copy())
        week_1_rank4_rank10['date'] = week
        week_1_rank4_rank10_group = await asyncio.to_thread(lambda: week_1_rank4_rank10.groupby(['date'])['count'].sum().reset_index())
        await asyncio.to_thread(lambda: week_1_rank4_rank10_group.rename(columns={'count':'rank_4_10'}, inplace=True))

        week_1_rank11_rank30 = await asyncio.to_thread(lambda: week_1[week_1[week].between(11,30)].copy())
        week_1_rank11_rank30['date'] = week
        week_1_rank11_rank30_group = await asyncio.to_thread(lambda: week_1_rank11_rank30.groupby(['date'])['count'].sum().reset_index())
        await asyncio.to_thread(lambda: week_1_rank11_rank30_group.rename(columns={'count':'rank_11_30'}, inplace=True))

        # Merge dataframes
        df_merged_w1 = await asyncio.to_thread(lambda: pd.merge(week_1_rank1_rank3_group, week_1_rank4_rank10_group, how='outer', on='date'))
        full_merged_w1 = await asyncio.to_thread(lambda: pd.merge(df_merged_w1, week_1_rank11_rank30_group, how='outer', on='date'))
        full_merged_w1 = await asyncio.to_thread(lambda: full_merged_w1.reindex(columns=['date', 'rank_1_3','rank_4_10','rank_11_30']))

        return full_merged_w1
    
    range_last_week_date = range_of_date(7)
    range_last2_week_date = range_of_date(14)
    range_last3_week_date = range_of_date(21)
    range_last4_week_date = range_of_date(28)
    range_last5_week_date = range_of_date(35)
    range_last6_week_date = range_of_date(42)
    range_last7_week_date = range_of_date(49)

    # Get merged data for each week
    week_1 = await merged(week=range_last_week_date[6])
    week_2 = await merged(week=range_last2_week_date[6])
    week_3 = await merged(week=range_last3_week_date[6])
    week_4 = await merged(week=range_last4_week_date[6])
    week_5 = await merged(week=range_last5_week_date[6])
    week_6 = await merged(week=range_last6_week_date[6])
    week_7 = await merged(week=range_last7_week_date[6])

    # Merge data from all weeks
    merge_1 = await asyncio.to_thread(lambda: pd.merge(week_1, week_2, on=['date','rank_1_3','rank_4_10','rank_11_30'], how='outer'))
    merge_2 = await asyncio.to_thread(lambda: pd.merge(merge_1, week_3, on=['date','rank_1_3','rank_4_10','rank_11_30'], how='outer'))
    merge_3 = await asyncio.to_thread(lambda: pd.merge(merge_2, week_4, on=['date','rank_1_3','rank_4_10','rank_11_30'], how='outer'))
    merge_4 = await asyncio.to_thread(lambda: pd.merge(merge_3, week_5, on=['date','rank_1_3','rank_4_10','rank_11_30'], how='outer'))
    merge_5 = await asyncio.to_thread(lambda: pd.merge(merge_4, week_6, on=['date','rank_1_3','rank_4_10','rank_11_30'], how='outer'))
    merge_6 = await asyncio.to_thread(lambda: pd.merge(merge_5, week_7, on=['date','rank_1_3','rank_4_10','rank_11_30'], how='outer'))
    await asyncio.to_thread(lambda: merge_6.fillna(0, inplace=True))
    await asyncio.to_thread(lambda: merge_6.sort_values(by='date', ascending=True, inplace=True))

    # Create the stacked bar chart
    fig = go.Figure(data=[
        go.Bar(x=merge_6['date'], y=merge_6['rank_1_3'], name='Rank 1 - 3', text=merge_6['rank_1_3'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside'),
        go.Bar(x=merge_6['date'], y=merge_6['rank_4_10'], name='Rank 4 - 10', text=merge_6['rank_4_10'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside'),
        go.Bar(x=merge_6['date'], y=merge_6['rank_11_30'], name='Rank 11 - 30', text=merge_6['rank_11_30'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside')
    ])

    # Update layout
    fig.update_layout(title='Rank By Group', barmode='stack')
    fig.update_xaxes(title='Date', dtick='D1', categoryorder='array', categoryarray=[
        range_last7_week_date[6],
        range_last6_week_date[6],
        range_last5_week_date[6],
        range_last4_week_date[6],
        range_last3_week_date[6],
        range_last2_week_date[6],
        range_last_week_date[6]]
    )
                        # '30-6-2024', '7-7-2024', '14-7-2024', '21-7-2024', '28-7-2024', '4-8-2024', '11-8-2024'])
    fig.update_yaxes(title='Value')

    # Convert figure to JSON string
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def web_traffic_table(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date):
    """
    Generate a table displaying web traffic details.

    Parameters:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): Start date for the data query.
        to_date (datetime.date): End date for the data query.

    Returns:
        str: A JSON string representing the table.

    This function retrieves web traffic data from a CSV file, computes various metrics such as
    total sessions, total users, sessions percentage, and users percentage, and presents the
    information in a table format. It then generates a JSON string representing the table.
    """
    # Read web traffic data from CSV file
    read_df = await ga4_df(session=session, from_date=from_date, to_date=to_date)
    
    # Group data by source and compute total sessions and total users
    df_1 = await asyncio.to_thread(lambda: read_df.groupby(['source'])['sessions'].sum().reset_index())
    df_2 = await asyncio.to_thread(lambda: read_df.groupby(['source'])['total_user'].sum().reset_index())
    
    # Merge dataframes and rename columns
    df = await asyncio.to_thread(lambda: pd.merge(df_1, df_2, on='source', how='outer'))
    await asyncio.to_thread(lambda: df.rename(columns={'total_user':'users'}, inplace=True))
    
    # Sort data by source
    await asyncio.to_thread(lambda: df.sort_values(by='source', ascending=True, inplace=True))
    
    # Compute total sessions and total users
    df['total_sessions'] = await asyncio.to_thread(lambda: df['sessions'].sum())
    df['total_users'] = await asyncio.to_thread(lambda: df['users'].sum())
    
    # Compute sessions percentage and users percentage
    df['sessions_percentage'] = await asyncio.to_thread(lambda: df['sessions'] / df['total_sessions']  )
    df['users_percentage'] = await asyncio.to_thread(lambda: df['users'] / df['total_users'])

    # Create table figure
    fig = go.Figure(
        go.Table(
            header=dict(
                fill_color="grey",
                line_color="black",
                font=dict(color="black"),
                values=[
                    'Source', 
                    'Sessions', 
                    'Users', 
                    'Sessions Percentage', 
                    'Users Percentage']
                ),
            cells=dict(
                fill_color="white",
                line_color="black",
                font=dict(color="black"),
                values=[
                    df['source'], 
                    df['sessions'].apply(lambda x: "{:,.0f}".format((x))), 
                    df['users'].apply(lambda x: "{:,.0f}".format((x))), 
                    df['sessions_percentage'].apply(lambda x: "{:,.2%}".format((x))), 
                    df['users_percentage'].apply(lambda x: "{:,.2%}".format((x)))
                ])
        )
    )
    
    # Update layout
    fig.update_layout(title='Web Traffic Details')

    # Convert figure to JSON string
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart
