import pandas as pd
import asyncio
import plotly
import json
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import select, func, case
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models.novel import GooddreamerNovelChapter, GooddreamerNovel
from app.db.models.novel import GooddreamerUserChapterProgression


async def chapter_read_frequency(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        chart_types: str,
        read_is_completed: list = [True, False]
):
    """
    Generates a visualization or table representing the chapter reader frequency distribution.

    Args:
        data (str): The type of visualization to generate ("chart" or "table").

    Returns:
        str: A JSON string representation of the generated visualization or table.
    """
    freq_cte = (
        select(
            GooddreamerNovelChapter.sort.label('bab'),
            func.count(GooddreamerUserChapterProgression.user_id.distinct()).label(
                "total_pembaca")
        )
        .join(GooddreamerUserChapterProgression.gooddreamer_novel_chapter)
        .filter(
            func.date(GooddreamerUserChapterProgression.updated_at).between(from_date, to_date),
            GooddreamerUserChapterProgression.is_completed.in_(read_is_completed)
        )
        .group_by(
            GooddreamerNovelChapter.sort
        )
        .cte("freq")
    )

    # Step 2: Define the 'main_query' CTE
    order_case = case(
        (freq_cte.c.bab.between(1, 5), 1),
        (freq_cte.c.bab.between(6, 10), 2),
        (freq_cte.c.bab.between(11, 20), 3),
        (freq_cte.c.bab.between(21, 50), 4),
        (freq_cte.c.bab >= 51, 5),
        else_=None
    ).label("order")

    bab_case = case(
        (freq_cte.c.bab.between(1, 5), '1-5'),
        (freq_cte.c.bab.between(6, 10), '6-10'),
        (freq_cte.c.bab.between(11, 20), '11-20'),
        (freq_cte.c.bab.between(21, 50), '21-50'),
        (freq_cte.c.bab >= 51, '> 51'),
        else_=None
    ).label("bab")

    total_pembaca = func.sum(
        freq_cte.c.total_pembaca).label("total_pembaca")

    # Step 3: Build the final query using the 'main_query' CTE
    main_query = (
        select(
            order_case.label("order"),
            bab_case.label("bab"),
            total_pembaca.label("total_pembaca")
        )
        .group_by(order_case, bab_case)
    )

    # Step 4: Execute the query
    result = await session.execute(main_query)
    result_data = result.fetchall()
    df = pd.DataFrame(result_data)
    if df.empty:
        df = pd.DataFrame({
            "order": [0],
            "bab": ["-"],
            "total_pembaca": [0]
        })
    await asyncio.to_thread(lambda: df.sort_values(by="order", ascending=True, inplace=True))

    if chart_types == 'chart':
        fig = go.Figure(
            go.Bar(
                x=df["bab"],
                y=df["total_pembaca"],
                text=df["total_pembaca"].apply(
                    lambda x: "{:,.0f}".format((x))),
                textposition="inside"
            )
        )
        fig.update_layout(
            title="Chapter Reader Frequency Distribution Chart",
            xaxis_title="Chapter",
            yaxis_title="Total User"
        )
    elif chart_types == "table":
        df = df.loc[:, ["bab", "total_pembaca"]].copy()
        df["total_pembaca"] = df["total_pembaca"].apply(
            lambda x: "{:,.0f}".format((x)))
        df.rename(columns={"bab": "Chapter",
                  "total_pembaca": "Chapter Reader"}, inplace=True)
        fig = go.Figure(
            go.Table(
                header=dict(
                    fill_color="grey",
                    line_color="black",
                    font=dict(color="black"),
                    values=list(df.columns),
                    align='center'
                ),
                cells=dict(
                    fill_color="white",
                    line_color="black",
                    font=dict(color="black"),
                    values=[df[col] for col in df.columns],
                    align='center'
                )
            )
        )
        fig.update_layout(title="Chapter Reader Frequency Distribution Table")

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def chapter_read_old_new(data: pd.DataFrame):
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
        await asyncio.to_thread(
            lambda: df_read.rename(
                columns={
                    "pembaca_chapter_new_user": "new_user", 
                    "pembaca_chapter_old_user": "old_user"}, 
                inplace=True))
        
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
            title='Old & New User Chapter Reader',
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
        # fig.update_yaxes(title='Total Users')
        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart

    except Exception as e:
        return json.dumps({"error": str(e)})


async def chapter_read_unique_count_chart(data: pd.DataFrame):
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
        await asyncio.to_thread(
            lambda: df_read.rename(
                columns={
                    "pembaca_chapter_unique": "chapter_unique", 
                    "pembaca_chapter_count": "chapter_count"}, 
                inplace=True))
        
        # Create Chart
        fig = go.Figure(
            data=[
                go.Bar(
                    x=df_read['tanggal'],
                    y=df_read['chapter_count'],
                    name='Chapter Reader Count',
                    text=df_read['chapter_count'].apply(
                        lambda x: f"{x:,.0f}"),
                    textposition='inside',
                ),
                go.Bar(
                    x=df_read['tanggal'],
                    y=df_read['chapter_unique'],
                    name='Chapter Reader Unique',
                    text=df_read['chapter_unique'].apply(
                        lambda x: f"{x:,.0f}"),
                    textposition='outside',
                ),
            ]
        )
        fig.update_layout(
            title='Chapter Reader (Unique & Count) Per Day', barmode='stack'
        )
        fig.update_xaxes(title='Date', dtick='D1')
        fig.update_yaxes(title='Total Purchases')

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart

    except Exception as e:
        return json.dumps({"error": str(e)})
    

async def chapter_read_per_day(data: pd.DataFrame):
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
                y=df_read['total_pembaca'],
                name='Total Reader',
                text=df_read['total_pembaca'].apply(lambda x: f"{x:,.0f}"),
                textposition='inside',
            )
        )

        fig.update_layout(title='Chapter Reader Per Day')  # Corrected title
        fig.update_xaxes(title='Days')
        fig.update_yaxes(title='Total Reader')  # Corrected title

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart
    
    except Exception as e:
        return json.dumps({"error": str(e)})  # Return error as JSON


async def chapter_read_category(data: pd.DataFrame, top_n: int = 10):
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
        df_read['total'] = await asyncio.to_thread(lambda: df_read['total_pembaca'].sum())
        df_read['persentase'] = await asyncio.to_thread(lambda: df_read['total_pembaca'] / df_read['total'])
        await asyncio.to_thread(lambda:  df_read.sort_values(by='persentase', ascending=False, inplace=True))

        # Limit to Top N Genres
        df_read = df_read.head(top_n)  

        # Chart Creation
        fig = go.Figure(
            go.Bar(
                x=df_read['category_name'],
                y=df_read['persentase'],
                name='% Chapter Reader By Genre',  # Corrected name
                text=df_read['persentase'].apply(lambda x: "{:,.2%}".format((x))),
                textposition='inside',
            )
        )
        fig.update_layout(title='Chapter Reader Percentage By Genre')  # Corrected title
        fig.update_xaxes(title='Genre')
        fig.update_yaxes(title='Percentage of Reader')  # Corrected title

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart
    except Exception as e:
        return json.dumps({"error": str(e)})


async def chapter_read_table(
        data: pd.DataFrame, 
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
        await asyncio.to_thread(lambda: df_read.fillna(0, inplace=True))

        # Sorting
        if sort_by not in ['pembaca_chapter_unique', 'pembaca_chapter_count']:
            raise ValueError("Invalid sort_by column. Choose 'pembaca_chapter_unique' or 'pembaca_chapter_count'.")
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
                        "Unique Chapter Reader",
                        "Total Chapters Reader",
                    ]
                ),
                cells=dict(
                    fill_color="white",
                    line_color="black",
                    font=dict(color="black"),
                    values=[
                        df_read["novel_id"],
                        df_read["novel_title"],
                        df_read["pembaca_chapter_unique"].apply(
                            lambda x: f"{x:,.0f}"
                        ),
                        df_read["pembaca_chapter_count"].apply(
                            lambda x: f"{x:,.0f}"
                        ),
                    ]
                ),
            )
        )
        fig.update_layout(
            title="Chapter Reader Details by Novel Title"
        )  # More descriptive title

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart
    except Exception as e:
        return json.dumps({"error": str(e)})

