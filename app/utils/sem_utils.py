from datetime import datetime, timedelta
import pandas as pd
import json
import plotly
import plotly.graph_objects as go
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.models.acquisition import GoogleAdsData, FacebookAdsData


async def df_file(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        filename: str = 'googleads', 
        file: str = 'sem', 
        data: str = 'dataframe') -> pd.DataFrame:
    """
    Retrieve and process data from CSV files.

    Parameters:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): Start date for the data query.
        to_date (datetime.date): End date for the data query.
        filename (str): The data to fetch ('googleads', 'facebookads').
        file (str): Type of Campaign ('sem' or 'GDN').
        data (str): Type of data to return ('dataframe', 'metrics', or 'table').

    Returns:
        pandas.DataFrame or float: Processed data depending on the 'data' parameter.

    This function reads data from CSV files, processes it based on the specified parameters,
    and returns the processed data in the specified format.
    """
    if filename == 'googleads':
        # Define campaign names based on file type
        if file == 'sem':
            campaign_name = [
                'SEM - Brand',
                'SEM - Generic'
            ]
        elif file == 'GDN':
            campaign_name = [
            'UA - GDN - Generic - Jan2024'
        ]
        
        query = select(
            GoogleAdsData.date.label("date"),
            GoogleAdsData.campaign_name.label("campaign_name"),
            GoogleAdsData.spend.label("spend"),
            GoogleAdsData.impressions.label("impressions"),
            GoogleAdsData.clicks.label("clicks")
        ).filter(
            GoogleAdsData.date.between(from_date, to_date),
            GoogleAdsData.campaign_name.in_(campaign_name)
        )
        result = await session.execute(query)
        get_data = result.fetchall()
        
        # Create DataFrame and preprocess data
        filtered_df = pd.DataFrame(get_data)

    elif filename == 'facebookads':
        query = select(
            FacebookAdsData.date_start.label("date"),
            FacebookAdsData.campaign_name.label("campaign_name"),
            FacebookAdsData.impressions.label("impressions"),
            FacebookAdsData.clicks.label("clicks"),
            FacebookAdsData.spend.label("spend")
        ).filter(
            FacebookAdsData.date_start.between(from_date, to_date),
            FacebookAdsData.campaign_name.in_(["FB-BA_UA-Traffic_Web-ID-AON"])
        )
        result = await session.execute(query)
        get_data = result.fetchall()
        
        # Create DataFrame and preprocess data
        filtered_df = pd.DataFrame(get_data)

    if filtered_df.empty:
        # If no data is found, create empty DataFrame with default values
        filtered_df['date'] = pd.date_range(to_date, to_date).date
        filtered_df['campaign_name'] = '-'
        filtered_df['spend'] = 0 
        filtered_df['impressions'] = 0
        filtered_df['clicks'] = 0
        filtered_df['ctr'] = 0
        filtered_df['cpm'] = 0
        filtered_df['cpc'] = 0

    filtered_df["date"] = await asyncio.to_thread(pd.to_datetime, filtered_df["date"])
    filtered_df['date'] = await asyncio.to_thread(lambda: filtered_df['date'].dt.date)
    await asyncio.to_thread(lambda: filtered_df.sort_values(by=['date', 'campaign_name'], ascending=True, inplace=True))

    if data == 'dataframe':
        # Process data to return as DataFrame
        df_merged = await asyncio.to_thread(
            lambda: filtered_df.groupby(["date", "campaign_name"]).agg(
                spend=("spend", "sum"),
                impressions=("impressions", "sum"),
                clicks=("clicks", "sum")
            ).reset_index()
        )

    elif data == 'metrics':
        # Process data to return specific metric
        df_merged = await asyncio.to_thread(
            lambda: filtered_df.groupby(["date"]).agg(
                spend=("spend", "sum"),
                impressions=("impressions", "sum"),
                clicks=("clicks", "sum")
            ).reset_index()
        )
        
        container = await asyncio.gather(
            asyncio.to_thread(lambda: df_merged["spend"].sum()),
            asyncio.to_thread(lambda: df_merged["impressions"].sum()),
            asyncio.to_thread(lambda: df_merged["clicks"].sum()),
            asyncio.to_thread(lambda: df_merged["clicks"].sum() / df_merged["impressions"].sum() if df_merged["impressions"].sum() != 0 else 0),
            asyncio.to_thread(lambda: df_merged["spend"].sum() / df_merged["impressions"].sum()if df_merged["impressions"].sum() != 0 else 0),
            asyncio.to_thread(lambda: df_merged["spend"].sum() / df_merged["clicks"].sum()if df_merged["clicks"].sum() != 0 else 0)
        )

        df_merged = {
            "spend": int(container[0]),
            "impressions": int(container[1]),
            "clicks": int(container[2]),
            "ctr": float(round(container[3], 4)),
            "cpm": int(container[4]),
            "cpc": int(container[5]),
        }

    return df_merged


async def dg_sem_awareness(
        session: AsyncSession,
        from_date: datetime.date, 
        to_date: datetime.date, 
        filename: str = 'googleads',
        file: str = 'sem'):
    """
    Compute the daily growth for Google SEM awareness campaigns.

    Parameters:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): Start date for the data query.
        to_date (datetime.date): End date for the data query.
        filename (str): The data to fetch from ('googleads', 'facebookads').
        file (str): Type of SEM campaign ('sem' or 'GDN').

    Returns:
        str: Daily growth percentage in formatted text.

    This function computes the daily growth percentage for Google SEM awareness campaigns
    based on the provided parameters and returns the result as a formatted text.
    """
    # Calculate the duration of one day
    delta = (to_date - from_date) + timedelta(1)
    # Calculate the start and end dates for the previous week
    fromdate_lastweek = from_date - delta
    todate_lastweek = to_date - delta

    # Initialize variables for current week and previous week data
    current_data = await df_file(session=session, from_date=from_date, to_date=to_date, filename=filename, data='metrics', file=file)
    last_week_data = await df_file(session=session, from_date=fromdate_lastweek, to_date=todate_lastweek, filename=filename, data='metrics', file=file)

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


async def spend_chart(
        dataframe: pd.DataFrame,
        file: str = 'sem', 
        source: str = 'google'):
    """
    Generate a detailed table for Google SEM or Facebook ads.

    Parameters:
        dataframe (pd.DataFrame): The dataframe contain Advertising data.
        file (str): Type of advertising data ('sem' for Google SEM or 'GDN' for Google Display Network).
        source (str): Source of advertising data ('google' or 'facebook').

    Returns:
        str: JSON representation of the performance chart.

    This function generates a performance chart for Google SEM or Facebook ads based on the provided parameters.
    """
    # Fetch data based on source and data type
    if source == 'google':
        sources = "Google"
        df = dataframe
        name = file.upper()
    elif source == 'facebook':
        sources = "Facebook"
        df = dataframe
        name = file.capitalize()

    df = await asyncio.to_thread(
        lambda: df.groupby(["date"]).agg(
            spend=("spend", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum")
        ).reset_index()
    )

    # Create a bar chart using Plotly
    fig = go.Figure(
        go.Bar(
            x=df['date'],
            y=df['spend'],
            name='Spend',
            text=df['spend'].apply(lambda x: "Rp. {:,.0f}".format((x))),
            textposition='inside'
        )
    )

    # Update layout and axis titles
    fig.update_layout(title=f'{sources} {name} Spend Chart')
    fig.update_xaxes(title='Date', dtick='D1')
    fig.update_yaxes(title='Spend')

    # Convert chart to JSON format
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def metrics_chart(
        dataframe: pd.DataFrame,
        file: str = 'sem', 
        source: str = 'google'):
    """
    Generate a detailed table for Google SEM or Facebook ads.

    Parameters:
        dataframe (pd.DataFrame): The dataframe contain Advertising data.
        file (str): Type of advertising data ('sem' for Google SEM or 'GDN' for Google Display Network).
        source (str): Source of advertising data ('google' or 'facebook').

    Returns:
        str: JSON representation of the performance chart.

    This function generates a performance chart for Google SEM or Facebook ads based on the provided parameters.
    """
    # Fetch data based on source and data type
    if source == 'google':
        sources = "Google"
        df = dataframe
        name = file.upper()
    elif source == 'facebook':
        sources = "Facebook"
        df = dataframe
        name = file.capitalize()

    df = await asyncio.to_thread(
        lambda: df.groupby(["date"]).agg(
            spend=("spend", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum")
        ).reset_index()
    )
    df['ctr'] = await asyncio.to_thread(lambda: df['clicks'] / df['impressions'])
    df['cpm'] = await asyncio.to_thread(lambda: df['spend'] / df['impressions'])
    df['cpc'] = await asyncio.to_thread(lambda: df['spend'] / df['clicks'])
    await asyncio.to_thread(lambda: df.fillna(0, inplace=True))

    # Define traces for each metric
    trace1 = go.Bar(
        x=df['date'],
        y=df['impressions'],
        name='Impressions',
        text=df['impressions'].apply(lambda x: "{:,.0f}".format((x))),
        textposition='inside'
    )
    
    trace2 = go.Bar(
        x=df['date'],
        y=df['clicks'],
        name='Clicks',
        text=df['clicks'].apply(lambda x: "{:,.0f}".format((x))),
        textposition='outside'
    )

    trace3 = go.Scatter(
        x=df['date'],
        y=df['ctr'],
        name='Click Through Rate',
        yaxis='y2',
        hovertext=df['ctr'].apply(lambda x: "{:,.0%}".format((x)))
    )

    trace4 = go.Scatter(
        x=df['date'],
        y=df['cpm'],
        name='Cost Per Impressions',
        yaxis='y3',
        hovertext=df['cpm'].apply(lambda x: "Rp. {:,.0f}".format((x)))
    )

    trace5 = go.Scatter(
        x=df['date'],
        y=df['cpc'],
        name='Cost Per Clicks',
        yaxis='y4',
        hovertext=df['cpc'].apply(lambda x: "Rp. {:,.0f}".format((x)))
    )

    # Define the layout with multiple y-axes
    layout = go.Layout(
        title=f'{sources} {name} Performance Chart',
        yaxis=dict(
            title='Value'
        ),
        yaxis2=dict(
            title='',
            overlaying='y',
            side='right',
            tickformat='.0%',
            showticklabels=False
        ),
        yaxis3=dict(
            title='',
            overlaying='y',
            side='right',
            position=1.0,
            showticklabels=False
        ),
        yaxis4=dict(
            title='',
            overlaying='y',
            side='right',
            position=1.0,
            showticklabels=False
        )
    )

    # Combine the traces and layout into a Figure object
    fig = go.Figure(data=[trace1, trace2, trace3, trace4, trace5], layout=layout)

    # Update layout and axis titles
    fig.update_layout(barmode='stack')
    fig.update_xaxes(title='Date', dtick='D1')

    # Convert chart to JSON format
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def details_table(
        dataframe: pd.DataFrame,
        file: str = 'sem', 
        source: str = 'google'):
    """
    Generate a detailed table for Google SEM or Facebook ads.

    Parameters:
        dataframe (pd.DataFrame): The dataframe contain Advertising data.
        file (str): Type of advertising data ('sem' for Google SEM or 'GDN' for Google Display Network).
        source (str): Source of advertising data ('google' or 'facebook').

    Returns:
        str: JSON representation of the detailed table.

    This function generates a detailed table for Google SEM or Facebook ads based on the provided parameters.
    """
    # Fetch data based on source and data type
    if source == 'google':
        sources = "Google"
        df = dataframe
        name = file.upper()
    elif source == 'facebook':
        sources = "Facebook"
        df = dataframe
        name = file.capitalize()

    df['ctr'] = await asyncio.to_thread(lambda: df['clicks'] / df['impressions'])
    df['cpm'] = await asyncio.to_thread(lambda: df['spend'] / df['impressions'])
    df['cpc'] = await asyncio.to_thread(lambda: df['spend'] / df['clicks'])
    await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
    
    # Create a table using Plotly
    fig = go.Figure(
        go.Table(
            header=dict(
                fill_color="grey",
                line_color='black',
                font=dict(color="black"),
                values=
                [
                    'Date',
                    'Campaign Name',
                    'Spend',
                    'Impressions',
                    'Clicks',
                    'Click Through Rate',
                    'Cost Per Impressions',
                    'Cost Per Clicks'
                ]),
            cells=dict(
                fill_color="white",
                line_color='black',
                font=dict(color='black'),
                values=
                [
                    df['date'],
                    df['campaign_name'],
                    df['spend'].apply(lambda x: "Rp. {:,.0f}".format((x))),
                    df['impressions'].apply(lambda x: "{:,.0f}".format((x))),
                    df['clicks'].apply(lambda x: "{:,.0f}".format((x))),
                    df['ctr'].apply(lambda x: "{:,.2%}".format((x))),
                    df['cpm'].apply(lambda x: "Rp. {:,.0f}".format((x))),
                    df['cpc'].apply(lambda x: "Rp. {:,.0f}".format((x)))
                ]
            )
        )
    )

    # Update layout and title
    fig.update_layout(title=f'{sources} {name} Table')

    # Convert chart to JSON format
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart
