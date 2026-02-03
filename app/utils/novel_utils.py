"""all novels functions file"""
import pandas as pd
import numpy as np
import json
import plotly
import asyncio
import plotly.graph_objects as go
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Case, select,case, func
from app.db.models.novel import GooddreamerNovel, DataCategory, GooddreamerNovelChapter, GooddreamerUserChapterProgression
from app.db.models.novel import  GooddreamerChapterTransaction, GooddreamerUserChapterAdmob, GooddreamerUserFavorite
from app.db.models.user import GooddreamerUserData, GooddreamerUserWalletItem


async def novel_dataframe(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        novel_title: str = "",
        category_novel: str = "",
        sort_by: str = "reader_purchase_percentage",
        ascending: bool = True
) -> pd.DataFrame:
    """
    Fetches data about all novels, including chapters, readers, and purchase information.
    Merges this data into a single DataFrame and returns it, filtered and sorted as specified.

    Args:
        session (AsyncSession): The asynchronous SQL Database session.
        from_date (datetime.date): Start date for filtering. Defaults to None.
        to_date (datetime.date): End date for filtering. Defaults to None.
        novel_title (str, optional): Filter by novel title (case-insensitive). Defaults to ''.
        category_novel (str, optional): Filter by novel category (case-insensitive). Defaults to ''.
        sort_by (str, optional): Column to sort by. Defaults to 'presentase_pembaca_ke_pembeli'.
        ascending (bool, optional): Whether to sort in ascending order. Defaults to True.

    Returns:
        pandas.DataFrame: DataFrame containing information about novels.
    """
    novel_detail_subquery = select(
        GooddreamerNovel.id.label("novel_id"),
        GooddreamerNovel.novel_title.label("novel_title"),
        DataCategory.category_name.label("category"),
        func.count(GooddreamerNovelChapter.sort).label("total_chapter"),
        func.count(GooddreamerNovelChapter.publication).label("published_chapter")
    ).join(
        GooddreamerNovel.gooddreamer_novel_chapter
    ).join(
        GooddreamerNovel.data_category
    ).filter(
        GooddreamerNovel.status == 2,
        GooddreamerNovel.deleted_at.is_(None),
    ).group_by(
        GooddreamerNovel.id
    ).subquery()

    register_reader_subquery = select(
        GooddreamerNovelChapter.novel_id.label("novel_id"),
        func.count(GooddreamerUserChapterProgression.user_id.distinct()).label("registered_reader")
    ).join(
        GooddreamerNovelChapter.gooddreamer_novel
    ).join(
        GooddreamerNovelChapter.gooddreamer_user_chapter_progression
    ).join(
        GooddreamerUserChapterProgression.gooddreamer_user_data
    ).filter(
        GooddreamerNovel.status == 2,
        GooddreamerNovel.deleted_at.is_(None),
        func.date(GooddreamerUserChapterProgression.created_at).between(from_date, to_date),
        GooddreamerUserData.is_guest == 0
    ).group_by(GooddreamerNovelChapter.novel_id).subquery()

    guest_reader_subquery = select(
        GooddreamerNovelChapter.novel_id.label("novel_id"),
        func.count(GooddreamerUserChapterProgression.user_id.distinct()).label("guest_reader")
    ).join(
        GooddreamerNovelChapter.gooddreamer_novel
    ).join(
        GooddreamerNovelChapter.gooddreamer_user_chapter_progression
    ).join(
        GooddreamerUserChapterProgression.gooddreamer_user_data
    ).filter(
        GooddreamerNovel.status == 2,
        GooddreamerNovel.deleted_at.is_(None),
        func.date(GooddreamerUserChapterProgression.created_at).between(from_date, to_date),
        GooddreamerUserData.is_guest == 1
    ).group_by(GooddreamerNovelChapter.novel_id).subquery()

    chapter_purchase_coin_subquery = select(
        GooddreamerChapterTransaction.novel_id.label("novel_id"),
        func.count(GooddreamerChapterTransaction.user_id.distinct()).label("chapter_purchase_coin")
    ).join(
        GooddreamerChapterTransaction.gooddreamer_user_wallet_item
    ).filter(
        GooddreamerUserWalletItem.reffable_type == "App\\Models\\ChapterTransaction",
        GooddreamerUserWalletItem.coin_type == "coin",
        func.date(GooddreamerChapterTransaction.created_at).between(from_date, to_date)
    ).group_by(GooddreamerChapterTransaction.novel_id).subquery()

    chapter_purchase_adscoin_subquery = select(
        GooddreamerChapterTransaction.novel_id.label("novel_id"),
        func.count(GooddreamerChapterTransaction.user_id.distinct()).label("chapter_purchase_adscoin")
    ).join(
        GooddreamerChapterTransaction.gooddreamer_user_wallet_item
    ).filter(
        GooddreamerUserWalletItem.reffable_type == "App\\Models\\ChapterTransaction",
        GooddreamerUserWalletItem.coin_type == "ads-coin",
        func.date(GooddreamerChapterTransaction.created_at).between(from_date, to_date)
    ).group_by(GooddreamerChapterTransaction.novel_id).subquery()

    chapter_purchase_ads_subquery = select(
    GooddreamerNovelChapter.novel_id.label("novel_id"),
    func.count(GooddreamerUserChapterAdmob.user_id.distinct()).label("chapter_purchase_ads")
    ).join(
        GooddreamerNovelChapter.gooddreamer_user_chapter_admob
    ).filter(
        func.date(GooddreamerUserChapterAdmob.created_at).between(from_date, to_date)
    ).group_by(GooddreamerNovelChapter.novel_id).subquery()

    query = select(
        novel_detail_subquery.c.novel_id.label("novel_id"),
        novel_detail_subquery.c.novel_title.label("novel_title"),
        novel_detail_subquery.c.category.label("category"),
        novel_detail_subquery.c.total_chapter.label("total_chapter"),
        novel_detail_subquery.c.published_chapter.label("published_chapter"),
        (func.ifnull(novel_detail_subquery.c.total_chapter, 0) - func.ifnull(novel_detail_subquery.c.published_chapter, 0)).label("unpublished_chapter"),
        register_reader_subquery.c.registered_reader.label("registered_reader"),
        guest_reader_subquery.c.guest_reader.label("guest_reader"),
        (func.ifnull(register_reader_subquery.c.registered_reader, 0) + func.ifnull(guest_reader_subquery.c.guest_reader, 0)).label("total_reader"),
        chapter_purchase_coin_subquery.c.chapter_purchase_coin.label("chapter_purchase_coin"),
        chapter_purchase_adscoin_subquery.c.chapter_purchase_adscoin.label("chapter_purchase_adscoin"),
        chapter_purchase_ads_subquery.c.chapter_purchase_ads.label("chapter_purchase_ads"),
        (func.ifnull(chapter_purchase_coin_subquery.c.chapter_purchase_coin, 0) + \
            func.ifnull(chapter_purchase_adscoin_subquery.c.chapter_purchase_adscoin, 0) + \
                func.ifnull(chapter_purchase_ads_subquery.c.chapter_purchase_ads, 0)).label("total_chapter_purchase")
    ).join(
        register_reader_subquery, novel_detail_subquery.c.novel_id == register_reader_subquery.c.novel_id, isouter=True
    ).join(
        guest_reader_subquery, novel_detail_subquery.c.novel_id == guest_reader_subquery.c.novel_id, isouter=True
    ).join(
        chapter_purchase_coin_subquery, novel_detail_subquery.c.novel_id == chapter_purchase_coin_subquery.c.novel_id, isouter=True
    ).join(
        chapter_purchase_adscoin_subquery, novel_detail_subquery.c.novel_id == chapter_purchase_adscoin_subquery.c.novel_id, isouter=True
    ).join(
        chapter_purchase_ads_subquery, novel_detail_subquery.c.novel_id == chapter_purchase_ads_subquery.c.novel_id, isouter=True
    )
    result = await session.execute(query)
    data = result.fetchall()
    df = pd.DataFrame(data)
    await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
    try:
        df["reader_purchase_percentage"] = await asyncio.to_thread(lambda: df["total_chapter_purchase"] / df["registered_reader"])
    except ZeroDivisionError:
        df["reader_purchase_percentage"] = 0
        
    await asyncio.to_thread(lambda: df.replace({"reader_purchase_percentage" : np.inf}, np.nan, inplace=True))
    await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
    
    df["registered_reader"] = await asyncio.to_thread(lambda: df["registered_reader"].astype(int))
    df["guest_reader"] = await asyncio.to_thread(lambda: df["guest_reader"].astype(int))
    df["total_reader"] = await asyncio.to_thread(lambda: df["total_reader"].astype(int))
    df["chapter_purchase_coin"] = await asyncio.to_thread(lambda: df["chapter_purchase_coin"].astype(int))
    df["chapter_purchase_adscoin"] = await asyncio.to_thread(lambda: df["chapter_purchase_adscoin"].astype(int))
    df["chapter_purchase_ads"] = await asyncio.to_thread(lambda: df["chapter_purchase_ads"].astype(int))
    df["total_chapter_purchase"] = await asyncio.to_thread(lambda: df["total_chapter_purchase"].astype(int))
    df["reader_purchase_percentage"] = await asyncio.to_thread(lambda: df["reader_purchase_percentage"].round(4))
    
    if novel_title:
        df['novel_title'] = await asyncio.to_thread(lambda: df['novel_title'].str.lower()  )
        df = await asyncio.to_thread(lambda: df[df['novel_title'].str.contains(f'.*{novel_title}.*')])
    
    if category_novel:
        df['category'] = await asyncio.to_thread(lambda: df['category'].str.lower()  )
        df = await asyncio.to_thread(lambda: df[df['category'].str.contains(f'.*{category_novel}.*')])
    
    await asyncio.to_thread(lambda: df.sort_values(by=sort_by, ascending=ascending, inplace=True))

    return df


async def novel_table(
        session: AsyncSession,
        from_date: datetime.date,
        to_date: datetime.date,
        novel_title: str = "",
        category_novel: str = "",
        sort_by: str = "reader_purchase_percentage",
        ascending: bool = True
):
    """
    Fetches novel data, optionally filters by title and date range, sorts the results,
    and generates a Plotly table visualization.

    Args:
        session (AsyncSession): The asynchronous SQL Database session.
        from_date (datetime.date): Start date for filtering. Defaults to None.
        to_date (datetime.date): End date for filtering. Defaults to None.
        novel_title (str, optional): Filter by novel title (case-insensitive). Defaults to ''.
        category_novel (str, optional): Filter by novel category (case-insensitive). Defaults to ''.
        sort_by (str, optional): Column to sort by. Defaults to 'presentase_pembaca_ke_pembeli'.
        ascending (bool, optional): Whether to sort in ascending order. Defaults to True.

    Returns:
        str: JSON representation of a Plotly table, or a JSON error message if any issues occur.
    """

    try:
        # Fetch Data
        df = await novel_dataframe(
            session=session,
            from_date=from_date,
            to_date=to_date,
            novel_title=novel_title,
            category_novel=category_novel,
            sort_by=sort_by,
            ascending=ascending
        )

        # Create Plotly Table
        fig = go.Figure(
            go.Table(
                columnorder=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                columnwidth=[100, 500, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
                header=dict(
                    fill_color="grey",
                    line_color="black",
                    font=dict(color="black"),
                    values=[
                        'Novel ID', 'Novel Title', 'Category', 'Total Chapter', 'Published Chapter',
                        'Unpublished Chapter', 'Register Reader', 'Guest Reader', 'Total Reader',
                        'Chapter Purchase With Coin', 'Chapter Purchase With AdsCoin', 'Chapter Purchase With Ads',
                        'Total Chapter Purchase', 'Reader To Purchase %'
                    ]
                ),
                cells=dict(
                    fill_color="white",
                    line_color="black",
                    font=dict(color="black"),
                    values=[
                        df['novel_id'],
                        df['novel_title'],
                        df['category'],
                        df['total_chapter'],
                        df['published_chapter'],
                        df['unpublished_chapter'],
                        df['registered_reader'].apply(lambda x: '{:.0f}'.format(x)),
                        df['guest_reader'].apply(lambda x: '{:.0f}'.format(x)),
                        df['total_reader'].apply(lambda x: '{:.0f}'.format(x)),
                        df['chapter_purchase_coin'].apply(lambda x: '{:.0f}'.format(x)),
                        df['chapter_purchase_adscoin'].apply(lambda x: '{:.0f}'.format(x)),
                        df['chapter_purchase_ads'].apply(lambda x: '{:.0f}'.format(x)),
                        df['total_chapter_purchase'].apply(lambda x: '{:.0f}'.format(x)),
                        df['reader_purchase_percentage'].apply(lambda x: '{:.2%}'.format(x))
                    ]
                )
            ),
            layout=dict(height=1500)
        )

        return await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    except Exception as e:  # Catching general errors
        return json.dumps({'error': f'An error occurred while generating the table, {e}'})


def str_converter(text, types='txt'):
    """
    Convert a numerical value into various string formats.

    Args:
        text (int, float): The numerical value to be formatted.
        types (str, optional): The type of formatting to apply. Defaults to 'txt'.
            Possible values are:
                - 'txt': Formats the number with commas as thousands separators and no decimal places.
                - 'rp': Formats the number as Indonesian Rupiah currency with commas as thousands separators and no decimal places.
                - 'persentase': Formats the number as a percentage with two decimal places.

    Returns:
        str: The formatted string based on the specified type.

    Raises:
        KeyError: If an unsupported type is provided.
    """
    if type(text) == int:
        return "{:,.0f}".format(text)
    else:
        return text


class NovelDetails:
    """
    Fetches and stores detailed information about a specific novel 
    from a database within a given date range.

    This class provides methods to retrieve data on:
        - Novel details (title, category, publish date, etc.)
        - Novel favorites count
        - Readers who read the novel (including email, register date, last read date, etc.)
        - User purchases (coins and ads coins) for chapters of the novel
        - Reader frequency by chapter range (e.g., 1-5, 6-10)

    Attributes:
        session (sqlalchemy.orm.session.Session): A database session object.
        from_date (datetime.date): The start date for the data query.
        to_date (datetime.date): The end date for the data query.
        novel_title (str): The title of the novel to retrieve details for.
    """
    def __init__(
            self,
            session: AsyncSession, 
            novel_title: str, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Initializes the NovelDetails object with the novel title, date range, and database session.

        Args:
            session: The SQLalchemy session object.
            novel_title (str): The title of the novel to retrieve details for.
            from_date (datetime.date): The start date for the data query.
            to_date (datetime.date): The end date for the data query.
        """
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.novel_title = novel_title
        self.df_novel_details = pd.DataFrame()
        self.df_novel_favorite = pd.DataFrame()
        self.df_reader = pd.DataFrame()
        self.df_chapter_coin = pd.DataFrame()
        self.df_chapter_adscoin = pd.DataFrame()
        self.df_chapter_ads = pd.DataFrame()
        self.df_frequency = pd.DataFrame()

    @classmethod
    async def laod_data(cls, session: AsyncSession, novel_title: str, from_date: datetime.date, to_date: datetime.date):
        """
        """
        instance = cls(session, novel_title, from_date, to_date)
        await instance.fetch_data()
        return instance
    
    async def fetch_data(self):
        """
        Fetches all relevant data for the specified novel from the database, including:
            - Novel details
            - Novel favorites count
            - Reader information
            - User purchases (coins and ads coins) for chapters
            - Reader frequency by chapter range

        This method calls the individual `_read_db` methods to populate the corresponding DataFrames within the object.
        """
        await self._read_db(data="novel_details")
        await self._read_db(data="novel_favorite")
        await self._read_db(data="novel_reader")
        await self._read_db(data="novel_purchase_coin")
        await self._read_db(data="novel_purchase_adscoin")
        await self._read_db(data="novel_purchase_ads")
        await self._read_db(data="reader_frequency")

    async def _read_db(self, data: str):
        """
        Reads detailed information about the specified novel from the database.

        This method populates the `df_novel_details` attribute with a DataFrame containing data like:
            - ID
            - Title
            - Category
            - Published Date
            - Last Updated
            - Status (Tamat/Ongoing)
            - Total Chapters
            - Published Chapters
            - Unpublished Chapters
            - Author Name
            - Author Pen Name
            - Author Gender
            - Author Contact (WA)
            - Author Email
            - Author Address

        Parameters:
            data (str): The data to fetch
                - 'novel_details'
                - 'novel_details'
                - 'novel_reader'
                - 'novel_purchase_coin'
                - 'novel_purchase_adscoin'
                - 'novel_purchase_ads'
                - 'reader_frequency' 

        The method handles potential empty results and merges data from different sources
        (novel details, author information) into a single DataFrame.
        """
        if data == "novel_details": 
            # Novel data query
            query = select(
                GooddreamerNovel.id.label('id_novel'),
                GooddreamerNovel.novel_title.label('novel_title'),
                DataCategory.category_name.label('category'),
                GooddreamerNovel.published_at.label('tanggal_terbit'),
                GooddreamerNovel.updated_at.label('last_updated'),
                Case((GooddreamerNovel.finish_status == 1, 'Tamat'), else_='Ongoing').label('status'),
                func.count(GooddreamerNovelChapter.sort).label("total_bab")
            ).join(
                GooddreamerNovel.gooddreamer_novel_chapter
            ).join(
                GooddreamerNovel.data_category
            ).filter(
                GooddreamerNovel.novel_title.like(f"{self.novel_title}"),
                GooddreamerNovel.status == 2,
                GooddreamerNovel.deleted_at.is_(None),
                GooddreamerNovelChapter.status == 1,
                GooddreamerNovelChapter.deleted_at.is_(None)
            ).group_by(
                GooddreamerNovel.id
            )
            result = await self.session.execute(query)
            result_data = result.fetchall()
            df_novel = pd.DataFrame(result_data)

            if df_novel.empty:
                df_novel = pd.DataFrame({
                    "id_novel": [0],
                    "novel_title": ["-"],
                    "category": ["-"],
                    "tanggal_terbit": ["-"],
                    "last_updated": ["-"],
                    "status": ["-"],
                    "total_bab": [0]
                })

            unpublised_query = select(
                GooddreamerNovel.id.label("id_novel"),
                func.count(GooddreamerNovelChapter.sort).label("bab_belum_terbit")
            ).join(
                GooddreamerNovel.gooddreamer_novel_chapter
            ).filter(
                GooddreamerNovel.novel_title.like(f"{self.novel_title}"),
                GooddreamerNovel.status == 2,
                GooddreamerNovel.deleted_at.is_(None),
                GooddreamerNovelChapter.deleted_at.is_(None),
                GooddreamerNovelChapter.status != 1
            ).group_by("id_novel")
            unpublised_result = await self.session.execute(unpublised_query)
            unpublished_data = unpublised_result.fetchall()
            df_unpublished = pd.DataFrame(unpublished_data)

            if df_unpublished.empty:
                df_unpublished = pd.DataFrame({
                    "id_novel": df_novel["id_novel"].item(),
                    "bab_belum_terbit": [0]
                })
            
            df_merged = await asyncio.to_thread(lambda: pd.merge(df_novel, df_unpublished, on="id_novel", how="inner"))
            df_merged["bab_terbit"] = await asyncio.to_thread(lambda: df_merged["total_bab"] - df_merged["bab_belum_terbit"])

            # Read data penulis
            df_penulis = pd.read_csv('./csv/data_penulis.csv')
            await asyncio.to_thread(lambda: df_penulis.rename(columns={"ID_Novel": "id_novel", "Alamat ": "Alamat"}, inplace=True))
            df_penulis = await asyncio.to_thread(lambda: df_penulis.loc[:, ["id_novel","Nama Penulis", "Nama Pena", "Gender", "WA", "Email Penulis", "Alamat"]])

            # merge data novel & data penulis
            df = await asyncio.to_thread(lambda: pd.merge(df_merged, df_penulis, on="id_novel", how="inner"))

            if df.empty:
                df = pd.DataFrame({
                    "id_novel": [0],
                    "novel_title": ["-"],
                    "category": ["-"],
                    "tanggal_terbit": ["-"],
                    "last_updated": ["-"],
                    "status": ["-"],
                    "bab_terbit": [0],
                    "bab_belum_terbit": [0],
                    "total_bab": [0],
                    "Nama Penulis": ["-"],
                    "Nama Pena": ["-"],
                    "Gender": ["-"],
                    "WA": [0],
                    "Email Penulis": ["-"],
                    "Alamat": ["-"]
                })
            
            self.df_novel_details = df.copy()
        
        elif data == "novel_favorite":
            query = select(
                GooddreamerUserFavorite.novel_id.label("id_novel"),
                func.count(GooddreamerUserFavorite.user_id).label("total_favorite")
            ).join(
                GooddreamerUserFavorite.gooddreamer_novel
            ).filter(
                GooddreamerNovel.novel_title.like(f"{self.novel_title}")
            ).group_by(GooddreamerUserFavorite.novel_id)
            result = await self.session.execute(query)
            result_data = result.fetchall()
            df = pd.DataFrame(result_data)

            if df.empty:
                df = pd.DataFrame({
                    "id_novel": [0],
                    "total_favorite": [0]
                })

            self.df_novel_favorite = df.copy()

        elif data == "novel_reader":
            query = select(
                GooddreamerUserChapterProgression.user_id.label("user_id"),
                GooddreamerUserData.email.label("email"),
                GooddreamerUserData.fullname.label("fullname"),
                func.date(GooddreamerUserData.registered_at).label("register_date"),
                GooddreamerNovel.novel_title.label("novel_title"),
                func.count(GooddreamerUserChapterProgression.id).label("chapter_count"),
                func.max(func.date(GooddreamerUserChapterProgression.updated_at)).label("last_read_date")
            ).join(
                GooddreamerUserChapterProgression.gooddreamer_user_data
            ).join(
                GooddreamerUserChapterProgression.gooddreamer_novel_chapter
            ).join(
                GooddreamerNovelChapter.gooddreamer_novel
            ).filter(
                GooddreamerNovel.novel_title.like(f"{self.novel_title}"),
                func.date(GooddreamerUserChapterProgression.updated_at).between(self.from_date, self.to_date)
            ).group_by(
                GooddreamerUserChapterProgression.user_id,
                GooddreamerNovel.novel_title
            )
            result = await self.session.execute(query)
            result_data = result.fetchall()
            df = pd.DataFrame(result_data)

            if df.empty:
                df = pd.DataFrame({
                    "user_id": [0],
                    "email": ["-"],
                    "fullname": ["-"],
                    "register_date": ["-"],
                    "novel_title": ["-"],
                    "chapter_count": [0],
                    "last_read_date": ["-"]
                })

            await asyncio.to_thread(lambda: df.fillna({"email": "guest"}, inplace=True))
            self.df_reader = df.copy()
        
        elif data == "novel_purchase_coin":
            query = select(
                GooddreamerChapterTransaction.user_id.label("user_id"),
                GooddreamerUserData.email.label("email"),
                GooddreamerUserData.fullname.label("fullname"),
                func.date(GooddreamerUserData.registered_at).label("register_date"),
                GooddreamerNovel.novel_title.label("novel_title"),
                func.sum(GooddreamerChapterTransaction.chapter_count).label("chapter_count"),
                func.max(func.date(GooddreamerChapterTransaction.created_at)).label("last_purchase_date")
            ).join(
                GooddreamerChapterTransaction.gooddreamer_user_data
            ).join(
                GooddreamerChapterTransaction.gooddreamer_novel
            ).join(
                GooddreamerChapterTransaction.gooddreamer_user_wallet_item
            ).filter(
                GooddreamerUserWalletItem.reffable_type == "App\\Models\\ChapterTransaction",
                GooddreamerUserWalletItem.coin_type == "coin",
                GooddreamerNovel.novel_title.like(f"{self.novel_title}"),
                func.date(GooddreamerChapterTransaction.created_at).between(self.from_date, self.to_date)
            ).group_by(
                GooddreamerChapterTransaction.user_id,
                GooddreamerNovel.novel_title
            )
            result = await self.session.execute(query)
            result_data = result.fetchall()
            df = pd.DataFrame(result_data)

            if df.empty:
                df = pd.DataFrame({
                    "user_id": [0],
                    "email": ["-"],
                    "fullname": ["-"],
                    "register_date": ["-"],
                    "novel_title": ["-"],
                    "chapter_count": [0],
                    "last_purchase_date": ["-"]
                })

            self.df_chapter_coin = df.copy()
        
        elif data == "novel_purchase_adscoin":
            query = select(
                GooddreamerChapterTransaction.user_id.label("user_id"),
                GooddreamerUserData.email.label("email"),
                GooddreamerUserData.fullname.label("fullname"),
                func.date(GooddreamerUserData.registered_at).label("register_date"),
                GooddreamerNovel.novel_title.label("novel_title"),
                func.sum(GooddreamerChapterTransaction.chapter_count).label("chapter_count"),
                func.max(func.date(GooddreamerChapterTransaction.created_at)).label("last_purchase_date")
            ).join(
                GooddreamerChapterTransaction.gooddreamer_user_data
            ).join(
                GooddreamerChapterTransaction.gooddreamer_novel
            ).join(
                GooddreamerChapterTransaction.gooddreamer_user_wallet_item
            ).filter(
                GooddreamerUserWalletItem.reffable_type == "App\\Models\\ChapterTransaction",
                GooddreamerUserWalletItem.coin_type == "ads-coin",
                GooddreamerNovel.novel_title.like(f"{self.novel_title}"),
                func.date(GooddreamerChapterTransaction.created_at).between(self.from_date, self.to_date)
            ).group_by(
                GooddreamerChapterTransaction.user_id,
                GooddreamerNovel.novel_title
            )
            result = await self.session.execute(query)
            result_data = result.fetchall()
            df = pd.DataFrame(result_data)

            if df.empty:
                df = pd.DataFrame({
                    "user_id": [0],
                    "email": ["-"],
                    "fullname": ["-"],
                    "register_date": ["-"],
                    "novel_title": ["-"],
                    "chapter_count": [0],
                    "last_purchase_date": ["-"]
                })

            self.df_chapter_adscoin = df.copy()

        elif data == "novel_purchase_ads":
            query = select(
                GooddreamerUserChapterAdmob.user_id.label("user_id"),
                GooddreamerUserData.email.label("email"),
                GooddreamerUserData.fullname.label("fullname"),
                func.date(GooddreamerUserData.registered_at).label("register_date"),
                GooddreamerNovel.novel_title.label("novel_title"),
                func.count(GooddreamerUserChapterAdmob.id).label("chapter_count"),
                func.max(func.date(GooddreamerUserChapterAdmob.created_at)).label("last_purchase_date")
            ).join(
                GooddreamerUserChapterAdmob.gooddreamer_user_data
            ).join(
                GooddreamerUserChapterAdmob.gooddreamer_novel_chapter
            ).join(
                GooddreamerNovelChapter.gooddreamer_novel
            ).filter(
                GooddreamerNovel.novel_title.like(f"{self.novel_title}"),
                func.date(GooddreamerUserChapterAdmob.created_at).between(self.from_date, self.to_date)
            ).group_by(
                GooddreamerUserChapterAdmob.user_id,
                GooddreamerNovel.novel_title
            )
            result = await self.session.execute(query)
            result_data = result.fetchall()
            df = pd.DataFrame(result_data)

            if df.empty:
                df = pd.DataFrame({
                    "user_id": [0],
                    "email": ["-"],
                    "fullname": ["-"],
                    "register_date": ["-"],
                    "novel_title": ["-"],
                    "chapter_count": [0],
                    "last_purchase_date": ["-"]
                })

            self.df_chapter_ads = df.copy()

        elif data == "reader_frequency":
            freq_cte = (
                select(
                    GooddreamerNovelChapter.novel_id,
                    GooddreamerNovelChapter.sort.label('bab'),
                    func.count(GooddreamerUserChapterProgression.user_id.distinct()).label("total_pembaca")
                )
                .join(GooddreamerUserChapterProgression.gooddreamer_novel_chapter)
                .join(GooddreamerNovelChapter.gooddreamer_novel)
                .filter(
                    func.date(GooddreamerUserChapterProgression.updated_at).between(self.from_date, self.to_date),
                    GooddreamerNovel.novel_title.like(f"{self.novel_title}")
                    )
                .group_by(
                    GooddreamerNovelChapter.novel_id,
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

            total_pembaca = func.sum(freq_cte.c.total_pembaca).label("total_pembaca")

            # Step 3: Build the final query using the 'main_query' CTE
            main_query = (
                select(
                    freq_cte.c.novel_id.label("novel_id"),
                    order_case.label("order"),
                    bab_case.label("bab"),
                    total_pembaca.label("total_pembaca")
                )
                .group_by(freq_cte.c.novel_id, order_case, bab_case)
            )

            # Step 4: Execute the query
            result = await self.session.execute(main_query)
            result_data = result.fetchall()
            df = pd.DataFrame(result_data)
            if df.empty:
                df = pd.DataFrame({
                    "novel_id": [0],
                    "order": [0],
                    "bab": ["-"],
                    "total_pembaca": [0]
                })
            self.df_frequency = df.copy()

    async def novel_details(self):
        """
        Calculates various metrics related to the specified novel, including:
            - Novel details (title, category, etc.)
            - Favorite count
            - Reader counts (unique and total)
            - Chapter purchases (coins and ads coins)
            - Total chapter count

        Returns:
            dict: A dictionary containing the calculated metrics for the novel.
        """
        df_novel_details = self.df_novel_details
        df_novel_favorite = self.df_novel_favorite
        df_reader = self.df_reader
        df_chapter_coin = self.df_chapter_coin
        df_chapter_adscoin = self.df_chapter_adscoin
        df_chapter_ads = self.df_chapter_ads

        df_reader_guest = await asyncio.to_thread(lambda: df_reader[df_reader["email"] == "guest"])
        df_reader_register = await asyncio.to_thread(lambda: df_reader[df_reader["email"] != "guest"])

        metrics = await asyncio.gather(
            asyncio.to_thread(lambda: df_novel_details["id_novel"].item()),
            asyncio.to_thread(lambda: df_novel_details["novel_title"].item()),
            asyncio.to_thread(lambda: df_novel_details["category"].item()),
            asyncio.to_thread(lambda: df_novel_details["total_bab"].item()),
            asyncio.to_thread(lambda: df_novel_favorite["total_favorite"].item()),
            asyncio.to_thread(lambda: df_novel_details["bab_belum_terbit"].item()),
            asyncio.to_thread(lambda: df_novel_details["bab_terbit"].item()),
            asyncio.to_thread(lambda: df_novel_details["tanggal_terbit"].item()),
            asyncio.to_thread(lambda: df_novel_details["status"].item()),
            asyncio.to_thread(lambda: df_novel_details["last_updated"].item()),
            asyncio.to_thread(lambda: df_novel_details["Nama Pena"].item()),
            asyncio.to_thread(lambda: df_novel_details["Nama Penulis"].item()),
            asyncio.to_thread(lambda: df_novel_details["Gender"].item()),
            asyncio.to_thread(lambda: df_novel_details["Email Penulis"].item()),
            asyncio.to_thread(lambda: df_novel_details["WA"].item()),
            asyncio.to_thread(lambda: df_novel_details["Alamat"].item()),
            asyncio.to_thread(lambda: int(df_reader_guest["user_id"].count() + df_reader_register["user_id"].count())),
            asyncio.to_thread(lambda: int(df_reader_guest["user_id"].count())),
            asyncio.to_thread(lambda: int(df_reader_register["user_id"].count())),
            asyncio.to_thread(lambda: int(df_reader_guest["chapter_count"].sum() + df_reader_register["chapter_count"].sum())),
            asyncio.to_thread(lambda: int(df_reader_guest["chapter_count"].sum())),
            asyncio.to_thread(lambda: int(df_reader_register["chapter_count"].sum())),
            asyncio.to_thread(lambda: int(df_chapter_coin["user_id"].count())),
            asyncio.to_thread(lambda: int(df_chapter_adscoin["user_id"].count())),
            asyncio.to_thread(lambda: int(df_chapter_ads["user_id"].count())),
            asyncio.to_thread(lambda: int(df_chapter_coin["chapter_count"].sum())),
            asyncio.to_thread(lambda: int(df_chapter_adscoin["chapter_count"].sum())),
            asyncio.to_thread(lambda: int(df_chapter_ads["chapter_count"].sum())),
            asyncio.to_thread(lambda: int(df_chapter_coin["user_id"].count() + df_chapter_adscoin["user_id"].count() + df_chapter_ads["user_id"].count())),
            asyncio.to_thread(lambda: int(df_chapter_coin["chapter_count"].sum() + df_chapter_adscoin["chapter_count"].sum() + df_chapter_ads["chapter_count"].sum()))
        )

        data = {
            "id_novel": metrics[0],
            "judul_novel": metrics[1],
            "category": metrics[2],
            "total_bab": metrics[3],
            "total_favorite": metrics[4],
            "belum_terbit": metrics[5],
            "bab_terbit":metrics[6],
            "tanggal_terbit": metrics[7],
            "status": metrics[8],
            "last_updated": metrics[9],
            "nama_pena": metrics[10],
            "nama_penulis": metrics[11],
            "gender": metrics[12],
            "email": metrics[13],
            "no_tlp": f"0{metrics[14]}",
            "alamat": metrics[15],
            "total_pembaca_unique": metrics[16],
            "guest_pembaca_unique": metrics[17],
            "regis_pembaca_unique": metrics[18],
            "total_pembaca_count": metrics[19],
            "guest_pembaca_count": metrics[20],
            "regis_pembaca_count": metrics[21],
            "chapter_coin_unique": metrics[22],
            "chapter_adscoin_unique": metrics[23],
            "chapter_ads_unique": metrics[24],
            "chapter_coin_count": metrics[25],
            "chapter_adscoin_count": metrics[26],
            "chapter_ads_count": metrics[27],
            "total_chapter_unique": metrics[28],
            "total_chapter_count": metrics[29],
        }

        return data

    async def user_pembeli_chapter(self, types: str):
        """
        Generates a Plotly table based on user chapter purchase or reading data.

        This asynchronous function processes DataFrames containing user activity data related to novel chapters.
        It dynamically selects the appropriate DataFrame based on the specified type, sorts the data,
        and generates a Plotly table displaying relevant information. The function handles different types 
        of data such as readers, purchases using coins, ads-coins, or ads.

        Args:
            types (str): The type of data to process and display. The possible values are:
                - 'reader': Users who read the chapters.
                - 'coin': Users who purchased chapters using coins.
                - 'ads-coin': Users who purchased chapters using ads-coins.
                - 'ads': Users who purchased chapters using ads.

        Returns:
            str: A JSON-encoded Plotly table showing relevant user activity based on the specified type.
                In case of errors, returns a JSON string with an 'error' message.

        Example:
            >>> await user_pembeli_chapter(types="reader")
            # Returns a JSON-encoded Plotly table for users who read the chapters.

        Notes:
            - If the selected DataFrame is empty, a JSON error message is returned.
            - Data sorting is performed based on 'last_read_date' or 'last_purchase_date' depending on the type.
            - The function includes exception handling to manage empty data and general errors.
        """
        try:
            # Fetch Data
            if types == "reader":
                df = self.df_reader
            elif types == "coin":
                df = self.df_chapter_coin
            elif types == "ads-coin":
                df = self.df_chapter_adscoin
            elif types == "ads":
                df = self.df_chapter_ads

            # Handle Empty DataFrame
            if df.empty:
                return json.dumps({'error': 'No data available for the specified filters.'})

            # Sorting data
            if types == "reader":
                await asyncio.to_thread(lambda: df.sort_values(by='last_read_date', ascending=False, inplace=True))
            else:
                await asyncio.to_thread(lambda: df.sort_values(by='last_purchase_date', ascending=False, inplace=True))

            # Create Plotly Table
            fig = go.Figure(
                data=[go.Table(
                    columnorder = [1, 2, 3, 4, 5, 6, 7],
                    columnwidth = [40, 40, 80, 40, 40, 80, 40],
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
                )]
            )

            # Dynamically set the title based on coin_type
            title_mapping = {
                'reader': 'Users Novel Reader',
                'coin': 'Users Chapter Purchase With Coin',
                'ads-coin': 'Users Chapter Purchase With AdsCoin',
                'ads': 'Users Chapter Purchase With Ads'
            }
            fig.update_layout(title=title_mapping.get(types, 'Users Chapter Purchase'))

            chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

            return chart

        except ValueError as ve:
            return json.dumps({'error': str(ve)})

        except Exception as e:  # Catching general errors
            return json.dumps({'error': f'An error occurred while generating the table, {e}'})

    async def frequency_dataframe(self, data: str):
        """
        Generates a visualization or table representing the chapter reader frequency distribution.

        Args:
            data (str): The type of visualization to generate ("chart" or "table").

        Returns:
            str: A JSON string representation of the generated visualization or table.
        """
        df = self.df_frequency.copy()
        await asyncio.to_thread(lambda: df.sort_values(by="order", ascending=True, inplace=True))
        
        if data == 'chart':
            fig = go.Figure(
                go.Bar(
                    x=df["bab"],
                    y=df["total_pembaca"],
                    text=df["total_pembaca"].apply(lambda x: "{:,.0f}".format((x))),
                    textposition="inside"
                )
            )
            fig.update_layout(
                title="Chapter Reader Frequency Distribution Chart",
                xaxis_title="Chapter",
                yaxis_title="Total User"
            )
        elif data == "table":
            df = df.loc[:, ["bab", "total_pembaca"]].copy()
            df["total_pembaca"] = df["total_pembaca"].apply(lambda x: "{:,.0f}".format((x)))
            df.rename(columns={"bab": "Chapter", "total_pembaca": "Chapter Reader"}, inplace=True)
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
