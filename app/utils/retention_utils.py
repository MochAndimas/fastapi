import pandas as pd
import json
import plotly
import asyncio
import plotly.graph_objects as go
import plotly.express as px
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from sqlalchemy import select, func
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.coin import GooddreamerTransaction as gt
from app.db.models.data_source import  ModelHasSources as mhs, Sources as s
from app.db.models.user import GooddreamerUserWalletItem as guwi, GooddreamerUserData as gud
from app.db.models.novel import GooddreamerUserChapterProgression as gucp, GooddreamerChapterTransaction as gct, GooddreamerUserChapterAdmob as guca


def get_date_range(days, period='days', months=3):
    """
    Returns the date range from today minus the specified number of days to yesterday, or from the start of the month
    a specified number of months ago to the last day of the previous month.

    Args:
        days (int): The number of days to go back from today to determine the start date when period is 'days'.
        period (str): The period type, either 'days' or 'months'. Default is 'days'.
        months (int): The number of months to go back from the current month to determine the start date when period is 'months'. Default is 3.

    Returns:
        tuple: A tuple containing the start date and end date (both in datetime.date format).
    """
    if period == 'days':
        # Calculate the end date as yesterday
        end_date = datetime.today() - timedelta(days=1)
        # Calculate the start date based on the number of days specified
        start_date = datetime.today() - timedelta(days=days)
    elif period == 'months':
        # Get the first day of the current month
        end_date = datetime.today().replace(day=1)
        # Calculate the start date based on the number of months specified
        start_date = end_date - relativedelta(months=months)
        # Adjust the end date to be the last day of the previous month
        end_date = end_date - relativedelta(days=1)
        
    return start_date.date(), end_date.date()


def generate_event_data_subquery(
        event_name: str, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        period: str, 
        source: str = 'all'):
    """
    Generates a subquery for event data based on the given event name, date range, period,
    and optionally a specific data source.

    Parameters:
        event_name (str): The name of the event.
            - 'User Read Chapter', 
            - 'User Buy Chapter With Coin
            - 'User Buy Chpater With AdsCoin
            - 'User Buy Chapter With Ads'
            - 'User Buy Coin'), default is an empty string.
        from_date (datetime.date): The start date of the date range in 'YYYY-MM-DD' format.
        to_date (datetime.date): The end date of the date range in 'YYYY-MM-DD' format.
        period (str): The period for grouping the event data, '%Y-%m-01' for monthly, '%Y-%m-%d' for daily.
        source (str, optional): The specific data source ('all', 'app', 'web'), default is 'all'.

    Returns:
        SQLAlchemy subquery: A subquery containing event data based on the given parameters.
    """
    # Initialize query based on event name
    if event_name == 'User Read Chapter':
        # Query for User Read Chapter event
        query = select(
            gucp.user_id.distinct().label('user_id'),
            func.date_format(gucp.updated_at, period).label('event_date')
        ).join(
            gucp.gooddreamer_user_data
        ).filter(
            func.date(gucp.updated_at).between(from_date, to_date),
            gud.is_guest == 0
        )
        # Apply source filter if specified
        if source != 'all':
            query = query.join(
                gucp.model_has_sources
            ).join(
                mhs.sources
            ).filter(
                mhs.model_type == 'App\\Models\\ChapterProgression',
                s.name == source
            )
    elif event_name == 'User Buy Chapter With Coin':
        # Query for User Buy Chapter With Coin event
        query = select(
            gct.user_id.distinct().label('user_id'),
            func.date_format(gct.created_at, period).label('event_date')
        ).join(
            gct.gooddreamer_user_wallet_item
        ).filter(
            guwi.reffable_type == 'App\\Models\\ChapterTransaction',
            guwi.coin_type == 'coin',
            func.date(gct.created_at).between(from_date, to_date)
        )
        # Apply source filter if specified
        if source != 'all':
            query = query.join(
                gct.model_has_sources
            ).join(
                mhs.sources
            ).filter(
                mhs.model_type == 'App\\Models\\ChapterTransaction',
                s.name == source
            )
    elif event_name == 'User Buy Chapter With AdsCoin':
        # Query for User Buy Chapter With AdsCoin event
        query = select(
            gct.user_id.distinct().label('user_id'),
            func.date_format(gct.created_at, period).label('event_date')
        ).join(
            gct.gooddreamer_user_wallet_item
        ).filter(
            guwi.reffable_type == 'App\\Models\\ChapterTransaction',
            guwi.coin_type == 'ads-coin',
            func.date(gct.created_at).between(from_date, to_date)
        )
        # Apply source filter if specified
        if source != 'all':
            query = query.join(
                gct.model_has_sources
            ).join(
                mhs.sources
            ).filter(
                mhs.model_type == 'App\\Models\\ChapterTransaction',
                s.name == source
            )
    elif event_name == 'User Buy Chapter With Ads':
        # Query for User Buy Chapter With Ads event
        query = select(
            guca.user_id.distinct().label('user_id'),
            func.date_format(guca.created_at, period).label('event_date')
        ).filter(
            func.date(guca.created_at).between(from_date, to_date)
        )
        # Apply source filter if specified
        if source != 'all':
            query = query.join(
                guca.model_has_sources
            ).join(
                mhs.sources
            ).filter(
                mhs.model_type == 'App\\Models\\UserChapterAdmob',
                s.name == source
            )
    elif event_name == 'User Buy Coin':
        # Query for User Buy Coin event
        query = select(
            gt.user_id.distinct().label('user_id'),
            func.date_format(gt.created_at, period).label('event_date')
        ).filter(
            func.date(gt.created_at).between(from_date, to_date),
            gt.transaction_status == 1
        )
        # Apply source filter if specified
        if source != 'all':
            query = query.join(
                gt.model_has_sources
            ).join(
                mhs.sources
            ).filter(
                mhs.model_type == 'App\\Models\\Transaction',
                s.name == source
            )

    return query.subquery()


async def data_query(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        period: str = '%Y-%m-%d', 
        source: str = 'all', 
        event_name: str = "") -> pd.DataFrame:
    """
    Generate a dataframe from a database query, focusing on event data and retention rates.

    Parameters:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (datetime.date): The start date of the date range in 'YYYY-MM-DD' format.
        to_date (datetime.date): The end date of the date range in 'YYYY-MM-DD' format.
        period (str, optional): The period for grouping the event data, e.g., '%Y-%m-01' for monthly, '%Y-%m-%d' for daily.
        source (str, optional): The specific data source ('all', 'app', 'web'), default is 'all'.
        event_name (str, optional): The name of the event 
            - 'User Read Chapter', 
            - 'User Buy Chapter With Coin
            - 'User Buy Chpater With AdsCoin
            - 'User Buy Chapter With Ads'
            - 'User Buy Coin'), default is an empty string.

    Returns:
        DataFrame: A DataFrame containing first event dates, total users for the first event,
                   subsequent event dates, and total users retained for each event.
    """
    # Generate event data subquery based on event name, date range, period, and source
    event_data_subquery = await asyncio.to_thread(generate_event_data_subquery, event_name, from_date, to_date, period, source)

    # Aliases for the subquery to compare events for retention calculation
    ed_1 = aliased(event_data_subquery)
    ed_2 = aliased(event_data_subquery)

    # Subquery to calculate retention rates
    retention_data_subquery = select(
        ed_1.c.user_id.label('user_id'),
        func.date_format(ed_1.c.event_date, period).label('first_event_date'),
        func.min(func.date_format(ed_2.c.event_date, period)).over(
            partition_by=ed_1.c.user_id,
            order_by=ed_2.c.event_date,
            rows=(0, None)
        ).label('after_event_date')
    ).outerjoin(
        ed_2, ed_1.c.user_id == ed_2.c.user_id
    ).filter(
        func.date_format(ed_2.c.event_date, period) > func.date_format(ed_1.c.event_date, period)
    ).subquery()

    # Subquery to aggregate retention data
    retention_table_subquery = select(
        retention_data_subquery.c.first_event_date.label('first_event_date'),
        (select(
            func.count(event_data_subquery.c.user_id)
        )).filter(
            func.date_format(event_data_subquery.c.event_date, period) == retention_data_subquery.c.first_event_date
        ).label('total_user_first_event'),
        retention_data_subquery.c.after_event_date.label('after_event_date'),
        func.count(retention_data_subquery.c.user_id).label('total_user_retention')
    ).group_by(
        'first_event_date',
        'after_event_date'
    ).subquery()

    # Subquery to aggregate first event data
    first_event_data_subquery = select(
        func.date_format(event_data_subquery.c.event_date, period).label('first_event_date'),
        func.count(event_data_subquery.c.user_id).label('total_user_first_event'),
        func.date_format(event_data_subquery.c.event_date, period).label('after_event_date'),
        func.count(event_data_subquery.c.user_id).label('total_user_retention')
    ).group_by(
        'first_event_date',
        'after_event_date'
    ).subquery()

    # Final query to combine retention and first event data
    query = select(
        first_event_data_subquery.c.first_event_date.label('first_event_date'),
        first_event_data_subquery.c.total_user_first_event.label('total_user_first_event'),
        first_event_data_subquery.c.after_event_date.label('after_event_date'),
        first_event_data_subquery.c.total_user_retention.label('total_user_retention')
    ).union_all(
        select(
            retention_table_subquery.c.first_event_date.label('first_event_date'),
            retention_table_subquery.c.total_user_first_event.label('total_user_first_event'),
            retention_table_subquery.c.after_event_date.label('after_event_date'),
            retention_table_subquery.c.total_user_retention.label('total_user_retention')
        )
    ).order_by(
        'first_event_date',
        'after_event_date'
    )

    # Convert query result to DataFrame
    result = await session.execute(query)
    data = result.fetchall()
    df = pd.DataFrame(data)

    # If DataFrame is empty, fill with default values
    if df.empty:
        df = pd.DataFrame({
            'first_event_date': pd.date_range(to_date, to_date).date,
            'total_user_first_event': [0],
            'after_event_date': pd.date_range(to_date, to_date).date,
            'total_user_retention': [0]
        })
    
    return df


async def cohort_df(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        period: str = "%Y-%m-%d", 
        data: str = 'float', 
        source: str = 'all', 
        event_name: str = "") -> pd.DataFrame:
    """
    Generate a cohort retention dataframe based on the specified date range, period, data type,
    source, and event name.

    Parameters:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (datetime.date): The start date of the date range in 'YYYY-MM-DD' format.
        to_date (datetime.date): The end date of the date range in 'YYYY-MM-DD' format.
        period (str): The period for grouping the event data, e.g., '%Y-%m-01' for monthly, '%Y-%m-%d' for daily.
        data (str): The type of data to return, either 'float' for retention rate or 'count' for raw counts.
        source (str): The specific data source ('all', 'app', 'web'), default is 'all'.
        event_name (str): The name of the event (
            - 'User Read Chapter', 
            - 'User Buy Chapter With Coin
            - 'User Buy Chpater With AdsCoin
            - 'User Buy Chapter With Ads'
            - 'User Buy Coin'), default is an empty string.

    Returns:
        DataFrame: A DataFrame containing the cohort retention matrix.
    """
    # Retrieve data using data_query function
    df = await data_query(session=session, from_date=from_date, to_date=to_date, period=period, source=source, event_name=event_name)

    # Calculate the retention days
    if period == '%Y-%m-%d':
        # Convert the date columns to datetime format
        df['first_event_date'] = await asyncio.to_thread(pd.to_datetime, df['first_event_date'])
        df['after_event_date'] = await asyncio.to_thread(pd.to_datetime, df['after_event_date'])
        # Calculate retention days as the difference between after_event_date and first_event_date
        df['retention_days'] = await asyncio.to_thread(lambda: (df['after_event_date'] - df['first_event_date']).dt.days)
    else:
        # Convert the date columns to datetime format
        df['first_event_date'] = await asyncio.to_thread(pd.to_datetime, df['first_event_date'])
        df['after_event_date'] = await asyncio.to_thread(pd.to_datetime, df['after_event_date'])
        # Calculate retention days as the difference in months between after_event_date and first_event_date
        df['retention_days'] = await asyncio.to_thread(lambda: df['after_event_date'].dt.to_period('M').astype(int) - df['first_event_date'].dt.to_period('M').astype(int))

    # Pivot the table to create the retention matrix
    retention_matrix = df.pivot_table(index='first_event_date', columns='retention_days', values='total_user_retention')
    
    # Calculate the retention rate by dividing by the total number of users on the first event date
    if data == "float":
        for column in retention_matrix.columns:
            retention_matrix[column] = retention_matrix[column] / df.groupby('first_event_date')['total_user_first_event'].first()
    else:
        pass

    # Fill NaN values with 0 for better readability
    retention_matrix = retention_matrix.fillna(0)
    
    return retention_matrix


async def cohort_table(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        period: str = "%Y-%m-%d", 
        data: str = 'int', 
        source: str = 'all', 
        event_name: str = "") -> str:
    """
    Generate a cohort table with conditional formatting based on the specified date range, period,
    data type, source, and event name.

    Parameters:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (str, optional): The start date of the date range in 'YYYY-MM-DD' format.
        to_date (str, optional): The end date of the date range in 'YYYY-MM-DD' format.
        period (str, optional): The period for grouping the event data, e.g., '%Y-%m-01' for monthly, '%Y-%m-%d' for daily.
        data (str, optional): The type of data to return, either 'float' for retention rate or 'count' for raw counts.
        source (str, optional): The specific data source ('all', 'app', 'web'), default is 'all'.
        event_name (str, optional): The name of the event (
            - 'User Read Chapter', 
            - 'User Buy Chapter With Coin
            - 'User Buy Chpater With AdsCoin
            - 'User Buy Chapter With Ads'
            - 'User Buy Coin'), default is an empty string.

    Returns:
        str: JSON representation of the Plotly figure containing the cohort table.
    """
    # Determine the period type for display in the table title
    period_type = "Daily" if period == "%Y-%m-%d" else "Monthly"
    
    # Generate cohort DataFrame using cohort_df function
    df = await cohort_df(session=session, from_date=from_date, to_date=to_date, period=period, data=data, source=source, event_name=event_name)
    
    # Reset index and rename columns for Plotly table
    df.reset_index(inplace=True)
    df.rename(columns={'first_event_date': f'{period_type} Retention'}, inplace=True)

    # Adjust date format based on the period
    if period == '%Y-%m-01':
        df[f'{period_type} Retention'] = pd.to_datetime(df[f'{period_type} Retention']).dt.to_period('M').astype(str)
    elif period == '%Y-%m-%d':
        df[f'{period_type} Retention'] = pd.to_datetime(df[f'{period_type} Retention']).dt.date

    # Create a list of header values
    header_values = df.columns.tolist()
    header_values[0] = f'{period_type} Retention'

    # Create a list of cell values
    cell_values = [df[col].tolist() for col in df.columns]
    
    # Calculate global min and max values for color scaling
    flat_values = [item for sublist in cell_values[1:] for item in sublist]
    global_min = min(flat_values)
    global_max = max(flat_values)

    # Define color scale from white (min) to medium green (max)
    color_scale = ['rgba(152, 251, 152, 0)', 'rgba(0, 128, 0, 1)']

    # Define formatting based on the data type
    if data == "float":
        formatting = ".0%"
    elif data == 'int':
        formatting = ""
    else:
        formatting = ""

    # Handle case where global_min equals global_max to avoid division by zero
    if global_min == global_max:
        fill_color = [[color_scale[0]] * len(df)] + [[color_scale[0]] * len(df) for _ in cell_values[1:]]
    else:
        fill_color = [
            ['#696969'] * len(df)] + [
            [f'rgba(0, 128, 0, {(value - global_min) / (global_max - global_min)})' if value > 0 else color_scale[0] for value in column]
            for column in cell_values[1:]
        ]
    
    # Set 'Periods' column color to match header color
    fill_color[0] = ['#696969'] * len(df)

    # Define column widths for better display
    column_widths = [105] + [45 for _ in range(len(df.columns) - 1)]

    # Define table height based on date range
    date_len = (to_date - from_date).days if period == '%Y-%m-%d' else 90
    table_height = {
        6: 400,
        13: 500,
        27: 800,
        90: 500
    }

    # Create Plotly table with conditional formatting
    fig = go.Figure(
            data=[
                go.Table(
                    columnwidth=column_widths,
                    header=dict(
                        font=dict(color="black"),
                        values=header_values,
                        fill_color='#696969',
                        align='left'),
                    cells=dict(
                        font=dict(color="black"),
                        values=cell_values,
                        fill=dict(
                            color=fill_color
                        ),
                        format=[None] + [formatting for _ in range(len(df.columns) - 1)],
                        align='left'))],
            layout=dict(height=table_height[date_len])
    )

    # Update table layout
    fig.update_layout(
        title=f'{period_type} Retention Table',
        # plot_bgcolor='rgba(0, 128, 0, 1)',  # Light grey background for the plot area
        paper_bgcolor='white'                   # White background for the entire figure
    )

    # Convert figure to JSON
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def retention_chart(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        period: str = "%Y-%m-%d", 
        data: str  = 'float', 
        source: str = 'all', 
        event_name: str = "") -> str:
    """
    Generate a retention chart based on the specified date range, period, data type,
    source, and event name.

    Parameters:
        session (AsyncSession): The asynchronous database session used for queries.
        from_date (str, optional): The start date of the date range in 'YYYY-MM-DD' format.
        to_date (str, optional): The end date of the date range in 'YYYY-MM-DD' format.
        period (str, optional): The period for grouping the event data, e.g., '%Y-%m-01' for monthly, '%Y-%m-%d' for daily.
        data (str, optional): The type of data to return, either 'float' for retention rate or 'count' for raw counts.
        source (str, optional): The specific data source ('all', 'app', 'web'), default is 'all'.
        event_name (str, optional): The name of the event (
            - 'User Read Chapter', 
            - 'User Buy Chapter With Coin
            - 'User Buy Chpater With AdsCoin
            - 'User Buy Chapter With Ads'
            - 'User Buy Coin'), default is an empty string.

    Returns:
        str: JSON representation of the Plotly figure containing the retention chart.
    """
    # Determine the period type for display in the chart title
    period_type = "Daily" if period == '%Y-%m-%d' else "Monthly"
    
    # Generate cohort DataFrame using cohort_df function
    df = await cohort_df(session=session, from_date=from_date, to_date=to_date, period=period, data=data, source=source, event_name=event_name)

    # Ensure column names are strings
    df.columns = df.columns.map(str)
    
    # Calculate the retention rate for each period
    retention_rates = df.div(df['0'], axis=0) * 100

    # Compute the average retention rate for each period across all days
    average_retention = retention_rates.mean(axis=0)

    # Create a DataFrame from the average retention rates for plotting
    average_retention_df = average_retention.reset_index()
    average_retention_df.columns = ['Period', 'Overall Retention Rate']
    average_retention_df = average_retention_df[average_retention_df['Period'] != 'Periods']

    # Create a line chart using Plotly
    fig = px.line(average_retention_df, x='Period', y='Overall Retention Rate',
                title=f'{period_type} Retention Rates',
                labels={'Period': f'{period_type} Retention', 'Overall Retention Rate': f'{period_type} Retention Rate (%)'})
    
    # Convert figure to JSON
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart
