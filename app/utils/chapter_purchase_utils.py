import pandas as pd
import asyncio
import json
import plotly
import plotly.graph_objects as go


def daily_growth(current_data: dict, last_week_data: dict):
    """
    Helper function to calculate daily growth from a Dict.

    Parameters:
        current_data (dict): A dict that contain current week data
        last_week_data (dict): A dict that contain last week data
    """
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


async def chapter_old_new(data: pd.DataFrame, chapter_types: str):
    """
    Fetches chapter purchase data using 'koin' and analyzes it based on old and new users.
    Returns a Plotly chart, new user count, or old user count based on the 'data' argument.

    Args:
        data (dataframe): Dataframe contains chapter data.
        chapter_types (str): Chapter types to fethc ('chapter_coin', 'chapter_adscoin', 'chapter_ads').

    Returns:
        str (JSON): Plotly chart JSON if data='chart', or int (new/old user count) otherwise.

    Raises:
        ValueError: If an invalid 'data' argument is provided.
    """

    try:
        # Data Retrieval
        df_read = data["df_old_new"]
        if chapter_types == "chapter_coin":
            await asyncio.to_thread(lambda: df_read.rename(columns={"chapter_koin_new_user": "new_user",
                           "chapter_koin_old_user": "old_user"}, inplace=True))
        if chapter_types == "chapter_adscoin":
            await asyncio.to_thread(lambda: df_read.rename(columns={"chapter_adskoin_new_user": "new_user",
                           "chapter_adskoin_old_user": "old_user"}, inplace=True))
        if chapter_types == "chapter_ads":
            await asyncio.to_thread(lambda: df_read.rename(columns={"chapter_admob_new_user": "new_user",
                           "chapter_admob_old_user": "old_user"}, inplace=True))

        # Create Chart
        trace1 = go.Scatter(
            yaxis='y',
            x=df_read['tanggal'],
            y=df_read['new_user'],
            name='New User',
            text=df_read['new_user'].apply(lambda x: "{:,.0f}".format((x))),
            textposition='top center')

        trace2 = go.Scatter(
            yaxis='y2',
            x=df_read['tanggal'],
            y=df_read['old_user'],
            name='Old User',
            text=df_read['old_user'].apply(lambda x: "{:,.0f}".format((x))),
            textposition='top center')

        layout = go.Layout(
            title='Old & New User Chapter Purchase',
            yaxis1=dict(
                title='New User'
            ),
            yaxis2=dict(
                title="Old User",
                overlaying='y',
                side='right',
            )
        )

        # Combine the traces and layout into a Figure object
        fig = go.Figure(data=[trace1, trace2], layout=layout)

        fig.update_xaxes(title='Date', dtick='D1')
        fig.update_yaxes(title='Total Users')
        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart

    except Exception as e:
        return json.dumps({"error": str(e)})


async def chapter_unique_count_chart(data: pd.DataFrame, chapter_types: str):
    """
    Fetches chapter purchase data using 'koin' and generates a chart showing unique and count of purchases over time.

    Args:
        data (dataframe): Dataframe contains chapter data.
        chapter_types (str): Chapter types to fethc ('chapter_coin', 'chapter_adscoin', 'chapter_ads').

    Returns:
        str (JSON): Plotly chart JSON if data='chart', or int (new/old user count) otherwise.

    Raises:
        ValueError: If an invalid 'data' argument is provided.
    """

    try:
        # Data Retrieval
        df_read = data["df_unique_count"]
        if chapter_types == "chapter_coin":
            await asyncio.to_thread(lambda: df_read.rename(columns={"chapter_koin_unique": "chapter_unique",
                           "chapter_koin_count": "chapter_count"}, inplace=True))
        if chapter_types == "chapter_adscoin":
            await asyncio.to_thread(lambda: df_read.rename(columns={"chapter_adskoin_unique": "chapter_unique",
                           "chapter_adskoin_count": "chapter_count"}, inplace=True))
        if chapter_types == "chapter_ads":
            await asyncio.to_thread(lambda: df_read.rename(columns={"chapter_admob_unique": "chapter_unique",
                           "chapter_admob_count": "chapter_count"}, inplace=True))

        # Create Chart
        fig = go.Figure(
            data=[
                go.Bar(
                    x=df_read['tanggal'],
                    y=df_read['chapter_count'],
                    name='Chapter Purchase Count',
                    text=df_read['chapter_count'].apply(
                        lambda x: f"{x:,.0f}"),
                    textposition='inside',
                ),
                go.Bar(
                    x=df_read['tanggal'],
                    y=df_read['chapter_unique'],
                    name='Chapter Purchase Unique',
                    text=df_read['chapter_unique'].apply(
                        lambda x: f"{x:,.0f}"),
                    textposition='outside',
                ),
            ]
        )
        fig.update_layout(
            title='Chapter Purchase (Unique & Count) Per Day', barmode='stack'
        )
        fig.update_xaxes(title='Date', dtick='D1')
        fig.update_yaxes(title='Total Purchases')

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart

    except Exception as e:
        return json.dumps({"error": str(e)})
    

async def chapter_coin_per_day(data: pd.DataFrame):
    """
    Fetches chapter purchase data and creates a chart showing daily purchase totals.

    Args:
        data (dataframe): Dataframe contains chapter data.

    Returns:
        str (JSON): Plotly chart JSON if data='chart', or int (new/old user count) otherwise.

    Raises:
        ValueError: If an invalid 'data' argument is provided.
    """

    try:
        # Data Retrieval
        df_read = data["df_day"]
        await asyncio.to_thread(lambda: df_read.sort_values(by='day_num', ascending=True, inplace=True))

        # Chart Creation
        fig = go.Figure(
            go.Bar(
                x=df_read['day_name'],
                y=df_read['total_pembeli'],
                name='Total Purchase',
                text=df_read['total_pembeli'].apply(lambda x: f"{x:,.0f}"),
                textposition='inside',
            )
        )

        fig.update_layout(title='Chapter Purchases Per Day')  # Corrected title
        fig.update_xaxes(title='Days')
        fig.update_yaxes(title='Total Purchases')  # Corrected title

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart
    
    except Exception as e:
        return json.dumps({"error": str(e)})  # Return error as JSON


async def chapter_coin_category(data: pd.DataFrame, top_n: int = 10):
    """
    Fetches chapter purchase data and creates a bar chart showing the percentage of purchases by genre.

    Args:
        data (dataframe): Dataframe contains chapter data.
        top_n (int): Number of data to be shown on the charts.

    Returns:
        str (JSON): Plotly chart JSON if data='chart', or int (new/old user count) otherwise.

    Raises:
        ValueError: If an invalid 'data' argument is provided.
    """

    try:
        # Data Retrieval
        df_read = data["df_genre"]
        df_read['total'] = await asyncio.to_thread(lambda: df_read['total_pembeli'].sum())
        df_read['persentase'] = await asyncio.to_thread(lambda: df_read['total_pembeli'] / df_read['total'])
        await asyncio.to_thread(lambda:  df_read.sort_values(by='persentase', ascending=False, inplace=True))

        # Limit to Top N Genres
        df_read = df_read.head(top_n)  

        # Chart Creation
        fig = go.Figure(
            go.Bar(
                x=df_read['category_name'],
                y=df_read['persentase'],
                name='% Chapter Purchase By Genre',  # Corrected name
                text=df_read['persentase'].apply(lambda x: "{:,.2%}".format((x))),
                textposition='inside',
            )
        )
        fig.update_layout(title='Chapter Purchase Percentage By Genre')  # Corrected title
        fig.update_xaxes(title='Genre')
        fig.update_yaxes(title='Percentage of Purchases')  # Corrected title

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart
    except Exception as e:
        return json.dumps({"error": str(e)})


async def chapter_table(
        data: pd.DataFrame, 
        chapter_types: str, 
        sort_by: str = "pembeli_chapter_unique", 
        ascending: bool = False):
    """
    Fetches chapter purchase data using 'koin' and creates a Plotly table displaying the details.

    Args:
        data (dataframe): Dataframe contains chapter data.
        chapter_types (str): Chapter types to fethc ('chapter_coin', 'chapter_adscoin', 'chapter_ads').
        sort_by (str, optional): Column to sort by ('pembeli_chapter_unique' or 'pembeli_chapter_count'). Defaults to 'pembeli_chapter_unique'.
        ascending (bool, optional): Whether to sort in ascending order. Defaults to False (descending).

    Returns:
        str (JSON): Plotly chart JSON if data='chart', or int (new/old user count) otherwise.

    Raises:
        ValueError: If an invalid 'data' argument is provided.
    """

    try:
        # Data Retrieval
        df_read = data["df_novel"]
        if chapter_types == "chapter_coin":
            await asyncio.to_thread(lambda: df_read.rename(columns={"pembeli_chapter_koin_unique": "pembeli_chapter_unique",
                           "pembeli_chapter_koin_count": "pembeli_chapter_count"}, inplace=True))
        if chapter_types == "chapter_adscoin":
            await asyncio.to_thread(lambda: df_read.rename(columns={"pembeli_chapter_adskoin_unique": "pembeli_chapter_unique",
                           "pembeli_chapter_adskoin_count": "pembeli_chapter_count"}, inplace=True))
        if chapter_types == "chapter_ads":
            await asyncio.to_thread(lambda: df_read.rename(columns={"pembeli_chapter_admob_unique": "pembeli_chapter_unique",
                           "pembeli_chapter_admob_count": "pembeli_chapter_count"}, inplace=True))
        await asyncio.to_thread(lambda: df_read.fillna(0, inplace=True))

        # Sorting
        if sort_by not in ['pembeli_chapter_unique', 'pembeli_chapter_count']:
            raise ValueError("Invalid sort_by column. Choose 'pembeli_chapter_unique' or 'pembeli_chapter_count'.")
        await asyncio.to_thread(lambda: df_read.sort_values(by=sort_by, ascending=ascending, inplace=True))

        # Create Table
        fig = go.Figure(
            go.Table(
                header=dict(
                    fill_color="grey",
                    line_color="black",
                    font=dict(color="black"),
                    values=[
                        "Novel ID",
                        "Novel Title",
                        "Unique Chapter Buyers",
                        "Total Chapters Purchased",
                    ]
                ),
                cells=dict(
                    fill_color="white",
                    line_color="black",
                    font=dict(color="black"),
                    values=[
                        df_read["novel_id"],
                        df_read["novel_title"],
                        df_read["pembeli_chapter_unique"].apply(
                            lambda x: f"{x:,.0f}"
                        ),
                        df_read["pembeli_chapter_count"].apply(
                            lambda x: f"{x:,.0f}"
                        ),
                    ]
                ),
            )
        )
        fig.update_layout(
            title="Chapter Purchase Details by Novel Title"
        )  # More descriptive title

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart
    except Exception as e:
        return json.dumps({"error": str(e)})
