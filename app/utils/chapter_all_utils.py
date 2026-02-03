import pandas as pd
import asyncio
import json
import plotly
import plotly.graph_objects as go
from typing import Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from datetime import datetime, timedelta
from sqlalchemy import func
from sqlalchemy.exc import ProgrammingError

from app.db.models.novel import GooddreamerNovel, GooddreamerNovelChapter, GooddreamerChapterTransaction
from app.db.models.novel import GooddreamerUserChapterAdmob, GooddreamerUserChapterProgression, DataCategory
from app.db.models.user import GooddreamerUserWalletItem, GooddreamerUserData
from app.db.models.data_source import ModelHasSources, Sources
pd.options.mode.copy_on_write = True


class DataChapter:
    """Class for processing chapter transaction data.

    This class provides methods to asynchronously retrieve and process chapter transaction data 
    from a database. It supports various aggregation periods ('daily' or 'monthly') and different 
    types of transactions, including chapter reads, coin transactions, and AdMob interactions.

    Attributes:
        session (AsyncSession): The asynchronous database session used for queries.
        period (str): The period for data aggregation ('daily' or 'monthly'). Defaults to 'daily'.
        from_date (datetime.date): The start date for data retrieval.
        to_date (datetime.date): The end date for data retrieval.
        data (str): Type of data to fetch ('all', 'chapter_read', 'chapter_coin', 'chapter_adscoin', or 'chapter_ads').
        df_read (pd.DataFrame, optional): DataFrame containing chapter read data. None by default.
        df_chapter_coin (pd.DataFrame, optional): DataFrame containing chapter coin transaction data 
                                                 (coin type: 'coin'). None by default.
        df_chapter_adscoin (pd.DataFrame, optional): DataFrame containing chapter coin transaction data 
                                                    (coin type: 'ads-coin'). None by default.
        df_chapter_ads (pd.DataFrame, optional): DataFrame containing chapter AdMob interaction data. 
                                                None by default.
    """

    def __init__(self, session: AsyncSession, from_date: datetime.date, to_date: datetime.date, period: str = 'daily', data: str = "all", read_is_completed: list = [True, False]):
        """Initialize the DataChapter object with specified parameters.

        Args:
            session (AsyncSession): The asynchronous database session used for queries.
            from_date (datetime.date): The start date for data retrieval.
            to_date (datetime.date): The end date for data retrieval.
            period (str, optional): Period for data aggregation ('daily' or 'monthly'). Defaults to 'daily'.
            data (str, optional): Type of data to fetch ('all', 'chapter_read', 'chapter_coin', 'chapter_adscoin', or 'chapter_ads').
                                  Defaults to 'all'.
        """
        self.session = session
        self.period = period
        self.from_date = from_date
        self.to_date = to_date
        self.data = data
        self.read_is_completed = read_is_completed
        self.df_chapter_read = pd.DataFrame()
        self.df_chapter_coin = pd.DataFrame()
        self.df_chapter_adscoin = pd.DataFrame()
        self.df_chapter_ads = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: datetime.date, to_date: datetime.date, period: str = "daily", data: str = "all", read_is_completed: list = [True, False]):
        """Load and process chapter transaction data asynchronously.

        This class method initializes an instance of DataChapter and fetches data based on the 
        provided parameters.

        Args:
            session (AsyncSession): The asynchronous database session used for queries.
            from_date (datetime.date): The start date for data retrieval.
            to_date (datetime.date): The end date for data retrieval.
            period (str, optional): The period for data aggregation ('daily' or 'monthly'). Defaults to 'daily'.
            data (str, optional): Type of data to fetch ('all', 'chapter_read', 'chapter_coin', 'chapter_adscoin', or 'chapter_ads').
                                  Defaults to 'all'.

        Returns:
            DataChapter: An instance of DataChapter with the fetched data.
        """
        instance = cls(session, from_date, to_date, period, data, read_is_completed)
        await instance._fetch_data(data=data, read_is_completed=read_is_completed)
        return instance

    async def _fetch_data(self, data: str = "all", read_is_completed: list = [True, False]):
        """Asynchronously fetch and store chapter transaction data based on specified data type.

        Depending on the provided `data` parameter, this method fetches and stores different types of 
        chapter transaction data, including chapter reads, coin transactions, ads-coin transactions, 
        and AdMob interactions.

        Args:
            data (str): The type of data to fetch ('all', 'chapter_read', 'chapter_coin', 'chapter_adscoin', or 'chapter_ads').
        """
        if data == "all":
            await self._read_db(from_date=self.from_date, to_date=self.to_date, types="chapter_read")
            await self._read_db(from_date=self.from_date, to_date=self.to_date, types="chapter_coin", coin="coin")
            await self._read_db(from_date=self.from_date, to_date=self.to_date, types="chapter_coin", coin="ads-coin")
            await self._read_db(from_date=self.from_date, to_date=self.to_date, types="chapter_ads")
        elif data == "chapter_read":
            await self._read_db(from_date=self.from_date, to_date=self.to_date, types="chapter_read", read_is_completed=read_is_completed)
        elif data == "chapter_coin":
            await self._read_db(from_date=self.from_date, to_date=self.to_date, types="chapter_coin", coin="coin")
        elif data == "chapter_adscoin":
            await self._read_db(from_date=self.from_date, to_date=self.to_date, types="chapter_coin", coin="ads-coin")
        elif data == "chapter_ads":
            await self._read_db(from_date=self.from_date, to_date=self.to_date, types="chapter_ads")

    async def _read_db(self, from_date: datetime, to_date: datetime.date, types: str, coin: str = 'coin', read_is_completed: list = [True, False]):
        """Asynchronously read data from the database based on transaction type and coin type.

        This method constructs a query to fetch specific types of chapter transaction data 
        (reads, coin transactions, ads-coin transactions, and AdMob interactions) within the 
        specified date range. It streams the results asynchronously and converts them into a 
        Pandas DataFrame for further processing.

        Args:
            from_date (datetime.date): The start date for the query.
            to_date (datetime.date): The end date for the query.
            types (str): The type of transaction to query ('chapter_coin', 'chapter_ads', or 'chapter_read').
            coin (str, optional): The type of coin used in transactions (relevant for 'chapter_coin' type). 
                                  Defaults to 'coin'.

        Returns:
            pd.DataFrame: A DataFrame containing the queried data, or an empty DataFrame if an error occurs.
        """
        delta = (self.to_date - self.from_date) + timedelta(days=1)
        BATCH_SIZE = 1000 if delta <= timedelta(days=7) else 15000
        try:
            if types == 'chapter_read':
                query = select(
                    func.date(GooddreamerUserChapterProgression.updated_at).label('tanggal'),
                    GooddreamerUserChapterProgression.user_id.label('user_id'),
                    GooddreamerUserData.is_guest.label('is_guest'),
                    GooddreamerNovelChapter.novel_id.label('novel_id'),
                    GooddreamerNovel.novel_title.label('novel_title'),
                    DataCategory.category_name.label('category_name'),
                    Sources.name.label('source'),
                    func.date(GooddreamerUserData.created_at).label('install_date'),
                    func.count(GooddreamerUserChapterProgression.id).label('chapter_count')
                ).join(
                    GooddreamerUserChapterProgression.model_has_sources
                ).join(
                    GooddreamerUserChapterProgression.gooddreamer_novel_chapter
                ).join(
                    GooddreamerUserChapterProgression.gooddreamer_user_data
                ).join(
                    GooddreamerNovelChapter.gooddreamer_novel
                ).join(
                    ModelHasSources.sources
                ).join(
                    GooddreamerNovel.data_category
                ).filter(
                    ModelHasSources.model_type == 'App\\Models\\ChapterProgression',
                    func.date(GooddreamerUserChapterProgression.updated_at).between(from_date, to_date),
                    GooddreamerUserChapterProgression.is_completed.in_(read_is_completed)
                ).group_by(
                    func.date(GooddreamerUserChapterProgression.updated_at),
                    GooddreamerUserChapterProgression.user_id,
                    GooddreamerNovelChapter.novel_id,
                    Sources.name
                )

            elif types == 'chapter_coin':
                query = select(
                    func.date(GooddreamerChapterTransaction.created_at).label('tanggal'),
                    GooddreamerChapterTransaction.user_id.label('user_id'),
                    GooddreamerChapterTransaction.novel_id.label('novel_id'),
                    GooddreamerNovel.novel_title.label('novel_title'),
                    DataCategory.category_name.label('category_name'),
                    GooddreamerUserWalletItem.coin_type.label('coin_type'),
                    Sources.name.label('source'),
                    func.date(GooddreamerUserData.created_at).label('install_date'),
                    func.sum(GooddreamerChapterTransaction.chapter_count).label('chapter_count'),
                ).join(
                    GooddreamerChapterTransaction.model_has_sources
                ).join(
                    ModelHasSources.sources
                ).join(
                    GooddreamerChapterTransaction.gooddreamer_novel
                ).join(
                    GooddreamerNovel.data_category
                ).join(
                    GooddreamerChapterTransaction.gooddreamer_user_data
                ).join(
                    GooddreamerChapterTransaction.gooddreamer_user_wallet_item
                ).filter(
                    ModelHasSources.model_type == 'App\\Models\\ChapterTransaction',
                    func.date(GooddreamerChapterTransaction.created_at).between(
                        from_date, to_date),
                    GooddreamerUserWalletItem.reffable_type == 'App\\Models\\ChapterTransaction',
                    GooddreamerUserWalletItem.coin_type == coin
                ).group_by(
                    func.date(GooddreamerChapterTransaction.created_at),
                    GooddreamerChapterTransaction.user_id,
                    GooddreamerChapterTransaction.novel_id,
                    GooddreamerUserWalletItem.coin_type,
                    Sources.name
                )
                
            elif types == 'chapter_ads':
                query = select(
                    func.date(GooddreamerUserChapterAdmob.created_at).label('tanggal'),
                    GooddreamerUserChapterAdmob.user_id.label('user_id'),
                    GooddreamerNovelChapter.novel_id.label('novel_id'),
                    GooddreamerNovel.novel_title.label('novel_title'),
                    DataCategory.category_name.label('category_name'),
                    Sources.name.label('source'),
                    func.date(GooddreamerUserData.created_at).label('install_date'),
                    func.count(GooddreamerUserData.id).label('chapter_count')
                ).join(
                    GooddreamerUserChapterAdmob.model_has_sources
                ).join(
                    ModelHasSources.sources
                ).join(
                    GooddreamerUserChapterAdmob.gooddreamer_novel_chapter
                ).join(
                    GooddreamerNovelChapter.gooddreamer_novel
                ).join(
                    GooddreamerNovel.data_category
                ).join(
                    GooddreamerUserChapterAdmob.gooddreamer_user_data
                ).filter(
                    ModelHasSources.model_type == 'App\\Models\\UserChapterAdmob',
                    func.date(GooddreamerUserChapterAdmob.created_at).between(from_date, to_date)
                ).group_by(
                    func.date(GooddreamerUserChapterAdmob.created_at),
                    GooddreamerUserChapterAdmob.user_id,
                    GooddreamerNovelChapter.novel_id,
                    Sources.name
                )

            # Stream the query results asynchronously
            results = await self.session.stream(query, execution_options={"yield_per": BATCH_SIZE, "stream_results": True})

            rows = []
            async for result in results:
                rows.append(result._asdict())  # Convert each row to a dictionary

            # Convert to DataFrame after processing all batches
            if types == "chapter_read":
                self.df_chapter_read = pd.DataFrame(rows)
            elif types == "chapter_coin":
                if coin == "coin":
                    self.df_chapter_coin = pd.DataFrame(rows)
                elif coin == "ads-coin":
                    self.df_chapter_adscoin= pd.DataFrame(rows)
            elif types == "chapter_ads":
                self.df_chapter_ads = pd.DataFrame(rows)

        except ProgrammingError:
            return pd.DataFrame()

    def _add_default_values(self, from_date: datetime.date, to_date: datetime.date, types: str):
        """Populates an empty DataFrame with default values and date ranges.

        This function checks if the provided DataFrame is empty. If it is, the function creates 
        a new DataFrame with default values for specified columns. It also creates a date range based on 
        the provided `from_date` and `to_date` and populates the 'tanggal' and 'install_date' columns 
        accordingly.

        Args:
            self: The instance of the class containing the DataFrame.
            df (pd.DataFrame): The DataFrame to potentially populate with default values.
            from_date (datetime.date): The start date for the data range.
            to_date (datetime.date): The end date for the data range.
        """
        df = self.__getattribute__(f"df_{types}")
        if df.empty:
            default_values = {
                'id': 0,
                'user_id': 0,
                'is_guest': 0,
                'novel_id': 0,
                'novel_title': '-',
                'category_name': '-',
                'chapter_count': 0,
                'coin_type': '-',
                'source': '-',
            }
            num_rows = (pd.to_datetime(to_date) - pd.to_datetime(from_date)).days + 1
            date_range = pd.date_range(from_date, to_date).date

            df = pd.DataFrame(default_values, index=range(num_rows))
            df['tanggal'] = date_range
            df['install_date'] = date_range

            if types == "chapter_read":
                self.df_chapter_read = df
            elif types == "chapter_coin":
                self.df_chapter_coin = df
            elif types == "chapter_adscoin":
                self.df_chapter_adscoin = df
            elif types == "chapter_ads":
                self.df_chapter_ads = df

    def _process_date_column(self, types):
        """Transforms date columns based on the chosen analysis period.

        This function adjusts the format of date columns ('tanggal' and 'install_date') in the DataFrame 
        depending on the specified analysis period ('daily' or other). It leverages pandas functionalities for 
        efficient date manipulation.

        - For daily analysis: Dates are converted to Python `datetime.date` objects.
        - For other analysis periods: Dates are converted to the start of the month (`datetime.date`) 
          by converting them to monthly periods first ('M') and then extracting the date component.

        Args:
            self: The instance of the class containing the DataFrame and period attribute.
            df (pd.DataFrame): The DataFrame containing the date columns to be processed.
        """
        
        date_columns = ['tanggal', 'install_date']  # Columns to process
        df = self.__getattribute__(f"df_{types}")
        if self.period == 'daily':
            # Convert to date objects for daily analysis
            for col in date_columns:
                df[col] = pd.to_datetime(df[col]).dt.date
        else:
            # Convert to start of month dates for other periods
            for col in date_columns:
                df[col] = pd.to_datetime(df[col]).dt.to_period('M')
                df[col] = df[col].dt.to_timestamp()  # Start of month
                df[col] = df[col].dt.date
    
    async def chapter_read(self, from_date: datetime.date, to_date: datetime.date, source: str = 'app') -> Dict[str, float]:
        """
        Asynchronously analyze chapter reading data based on specified criteria.

        Args:
            from_date (datetime): Start date for analysis.
            to_date (datetime): End date for analysis.
            source (str, optional): Data source filter 'app' or 'web' (default is 'app').

        Returns:
            dict: Processed chapter read data containing various metrics such as unique readers, new and old users,
                guest and registered user counts.
        """

        # Add default values and process the date column asynchronously
        await asyncio.to_thread(self._add_default_values, from_date=from_date, to_date=to_date, types="chapter_read")
        await asyncio.to_thread(self._process_date_column, types="chapter_read")

        # Filter data asynchronously based on source and date range
        df_read = await asyncio.to_thread(
            lambda: self.df_chapter_read[(self.df_chapter_read['source'] == source) & 
                                        (self.df_chapter_read["tanggal"] >= from_date) & 
                                        (self.df_chapter_read["tanggal"] <= to_date)].copy()
        )
        
        # Convert 'tanggal' column to datetime and extract day-related info asynchronously
        df_read['tanggal'] = await asyncio.to_thread(pd.to_datetime, df_read['tanggal'])
        df_read['day_num'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_of_week)
        df_read['day_name'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_name())

        # Filter for new and old users asynchronously
        df_new_user = await asyncio.to_thread(
            lambda: df_read[(df_read['install_date'] >= from_date) & (df_read['install_date'] <= to_date)].copy()
        )
        df_old_user = await asyncio.to_thread(
            lambda: df_read[df_read['install_date'] < from_date].copy()
        )

        # Filter for registered and guest users asynchronously
        df_register = await asyncio.to_thread(lambda: df_read[df_read['is_guest'] == 0].copy())
        df_guest = await asyncio.to_thread(lambda: df_read[df_read['is_guest'] == 1].copy())

        # Calculate various metrics asynchronously using pandas operations
        metrics = await asyncio.gather(
            asyncio.to_thread(lambda: df_read['user_id'].nunique()),  # pembaca_chapter_unique
            asyncio.to_thread(lambda: df_read['chapter_count'].sum()),  # pembaca_chapter_count
            asyncio.to_thread(lambda: df_new_user[df_new_user['is_guest'] == 0]['user_id'].nunique()),  # new_user_count
            asyncio.to_thread(lambda: df_old_user[df_old_user['is_guest'] == 0]['user_id'].nunique()),  # old_user_count
            asyncio.to_thread(lambda: df_new_user[df_new_user['is_guest'] == 1]['user_id'].nunique()),  # guest_new_user_count
            asyncio.to_thread(lambda: df_old_user[df_old_user['is_guest'] == 1]['user_id'].nunique()),  # guest_old_user_count
            asyncio.to_thread(lambda: df_register['user_id'].nunique()),  # unique_register_reader
            asyncio.to_thread(lambda: df_guest['user_id'].nunique()),  # unique_guest_reader
            asyncio.to_thread(lambda: df_register['chapter_count'].sum()),  # count_register_reader
            asyncio.to_thread(lambda: df_guest['chapter_count'].sum())  # count_guest_reader
        )

        # Create the result container with all the calculated metrics
        container = {
            'pembaca_chapter_unique': metrics[0],
            'pembaca_chapter_count': metrics[1],
            'new_user_count': metrics[2],
            'old_user_count': metrics[3],
            'guest_new_user_count': metrics[4],
            'guest_old_user_count': metrics[5],
            'unique_register_reader': metrics[6],
            'unique_guest_reader': metrics[7],
            'count_register_reader': metrics[8],
            'count_guest_reader': metrics[9]
        }

        return container

    async def chapter_ads(self, from_date: datetime.date, to_date: datetime.date, source: str = 'app') -> Dict[str, float]:
        """
        Asynchronously analyze chapter purchase data with ads based on specified criteria.

        Args:
            from_date (datetime): Start date for analysis.
            to_date (datetime): End date for analysis.
            source (str, optional): Data source filter 'app' or 'web' (default is 'app').

        Returns:
            dict: Processed purchase data with ads containing various metrics such as unique readers, new and old users
        """
        # Add default values and process the date column asynchronously
        await asyncio.to_thread(self._add_default_values, from_date=from_date, to_date=to_date, types="chapter_ads")
        await asyncio.to_thread(self._process_date_column, types="chapter_ads")
        
        # Filter data asynchronously based on source and date range
        df_filtered = await asyncio.to_thread(
            lambda: self.df_chapter_ads[
                (self.df_chapter_ads["source"] == source) &
                (self.df_chapter_ads["tanggal"] >= from_date) &
                (self.df_chapter_ads["tanggal"] <= to_date)
            ].copy()
        )
        
        # Convert 'tanggal' column to datetime and extract day-related info asynchronously
        df_filtered['tanggal'] = await asyncio.to_thread(pd.to_datetime, df_filtered["tanggal"])
        df_filtered['day_num'] = await asyncio.to_thread(lambda: df_filtered['tanggal'].dt.day_of_week) 
        df_filtered['day_name'] = await asyncio.to_thread(lambda: df_filtered['tanggal'].dt.day_name())
        
        # Filter for new and old users asynchronously
        df_new_user = await asyncio.to_thread(
            lambda: df_filtered[(df_filtered['install_date'] >= from_date) & (df_filtered['install_date'] <= to_date)].copy()
        )
        df_old_user = await asyncio.to_thread(
            lambda: df_filtered[df_filtered['install_date'] < from_date].copy()
        )

        # Calculate various metrics asynchronously using pandas operations
        metrics = await asyncio.gather(
            asyncio.to_thread(lambda: df_filtered["user_id"].nunique()), # chapter_unique
            asyncio.to_thread(lambda: df_filtered["chapter_count"].sum()), # chapter_count
            asyncio.to_thread(lambda: df_new_user["user_id"].nunique()), # new_user_count
            asyncio.to_thread(lambda: df_old_user["user_id"].nunique()) # old_user_count
        )
        
        # Create the result container with all the calculated metrics
        result = {
            'chapter_unique': int(metrics[0]),
            'chapter_count': int(metrics[1]),
            'new_user_count': int(metrics[2]),
            'old_user_count': int(metrics[3]),
        }

        return result

    async def chapter_coin(self, from_date: datetime.date, to_date: datetime.date, source: str = 'app') -> Dict[str, float]:
        """
        Asynchronously analyze chapter purchase data with coin based on specified criteria.

        Args:
            from_date (datetime): Start date for analysis.
            to_date (datetime): End date for analysis.
            source (str, optional): Data source filter 'app' or 'web' (default is 'app').

        Returns:
            dict: Processed purchase data with coin containing various metrics such as unique readers, new and old users
        """
        # Add default values and process the date column asynchronously
        await asyncio.to_thread(self._add_default_values, from_date=from_date, to_date=to_date, types="chapter_coin")
        await asyncio.to_thread(self._process_date_column, types="chapter_coin")

        # Filter data asynchronously based on source and date range
        df_read = await asyncio.to_thread(
            lambda: self.df_chapter_coin[
                (self.df_chapter_coin["source"] == source) &
                (self.df_chapter_coin["tanggal"] >= from_date) &
                (self.df_chapter_coin["tanggal"] <= to_date)
            ].copy()
        )

        # Convert 'tanggal' column to datetime and extract day-related info asynchronously
        df_read["tanggal"] = await asyncio.to_thread(pd.to_datetime, df_read["tanggal"])
        df_read['day_num'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_of_week)
        df_read['day_name'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_name())

        # Filter for new and old users asynchronously
        df_new_user = await asyncio.to_thread(
            lambda: df_read[(df_read['install_date'] >= from_date) & (df_read['install_date'] <= to_date)].copy()
        )
        df_old_user = await asyncio.to_thread(
            lambda: df_read[df_read['install_date'] < from_date].copy()
        )

        # Calculate various metrics asynchronously using pandas operations
        metrics = await asyncio.gather(
            asyncio.to_thread(lambda: df_read["user_id"].nunique()), # chapter_unique
            asyncio.to_thread(lambda: df_read["chapter_count"].sum()), # chapter-count
            asyncio.to_thread(lambda: df_new_user["user_id"].nunique()), # new_user_count
            asyncio.to_thread(lambda: df_old_user["user_id"].nunique()) # old_user_count
        )

        # Create the result container with all the calculated metrics
        container = {
            'chapter_unique': int(metrics[0]),
            'chapter_count': int(metrics[1]),
            'new_user_count': int(metrics[2]),
            'old_user_count': int(metrics[3]),
        }
        
        return container
    
    async def chapter_adscoin(self, from_date: datetime.date, to_date: datetime.date, source: str = 'app') -> Dict[str, float]:
        """
        Asynchronously analyze chapter purchase data with adscoin based on specified criteria.

        Args:
            from_date (datetime): Start date for analysis.
            to_date (datetime): End date for analysis.
            source (str, optional): Data source filter 'app' or 'web' (default is 'app').

        Returns:
            dict: Processed purchase data with adscoin containing various metrics such as unique readers, new and old users
        """
        # Add default values and process the date column asynchronously
        await asyncio.to_thread(self._add_default_values, from_date=from_date, to_date=to_date, types="chapter_adscoin")
        await asyncio.to_thread(self._process_date_column, types="chapter_adscoin")

        # Filter data asynchronously based on source and date range
        df_read = await asyncio.to_thread(
            lambda: self.df_chapter_adscoin[
                (self.df_chapter_adscoin["source"] == source) &
                (self.df_chapter_adscoin["tanggal"] >= from_date) &
                (self.df_chapter_adscoin["tanggal"] <= to_date)
            ].copy()
        )

        # Convert 'tanggal' column to datetime and extract day-related info asynchronously
        df_read["tanggal"] = await asyncio.to_thread(pd.to_datetime, df_read["tanggal"])
        df_read['day_num'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_of_week)
        df_read['day_name'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_name())

        # Filter for new and old users asynchronously
        df_new_user = await asyncio.to_thread(
            lambda: df_read[(df_read['install_date'] >= from_date) & (df_read['install_date'] <= to_date)].copy()
        )
        df_old_user = await asyncio.to_thread(
            lambda: df_read[df_read['install_date'] < from_date].copy()
        )

        # Calculate various metrics asynchronously using pandas operations
        metrics = await asyncio.gather(
            asyncio.to_thread(lambda: df_read["user_id"].nunique()), # chapter_unique
            asyncio.to_thread(lambda: df_read["chapter_count"].sum()), # chapter_count
            asyncio.to_thread(lambda: df_new_user["user_id"].nunique()), # new_user_count
            asyncio.to_thread(lambda: df_old_user["user_id"].nunique()) # old_user_count
        )

        # Create the result container with all the calculated metrics
        container = {
            'chapter_unique': int(metrics[0]),
            'chapter_count': int(metrics[1]),
            'new_user_count': int(metrics[2]),
            'old_user_count': int(metrics[3]),
        }

        return container

    def total_chapter_purchase(
            self, 
            metrics_1: str, 
            metrics_2: str, 
            chapter_coin_data: dict, 
            chapter_adscoin_data: dict, 
            chapter_ads_data: dict) -> Dict[str, float]:
        """
        Calculates total chapter purchases based on multiple data sources.

        This function combines data from different sources (chapter coin data, chapter adscoin data, and chapter ads data)
        to calculate the total number of unique chapter purchases and the total count of chapter purchases.

        Args:
            metrics_1: The key in the data dictionaries for the unique count.
            metrics_2: The key in the data dictionaries for the total count.
            chapter_coin_data: A dictionary containing chapter coin data.
            chapter_adscoin_data: A dictionary containing chapter adscoin data.
            chapter_ads_data: A dictionary containing chapter ads data.

        Returns:
            dict: A dictionary containing the total unique count and total count of chapter purchases.
        """

        # Data Retrieval (with Exception Handling)
        try:
            df_koin_unique = chapter_coin_data[metrics_1]
            df_koin_count = chapter_coin_data[metrics_2]
            df_adskoin_unique = chapter_adscoin_data[metrics_1]
            df_adskoin_count = chapter_adscoin_data[metrics_2]
            df_admob_unique = chapter_ads_data[metrics_1]
            df_admob_count = chapter_ads_data[metrics_2]
        except Exception as e:
            raise RuntimeError(f"Error retrieving chapter data: {e}") 

        # Return Result
        container = {
            'unique': df_koin_unique + df_adskoin_unique + df_admob_unique,
            'count': df_koin_count + df_adskoin_count + df_admob_count
        }
        return container

    def calculate_growth(self, current_data, last_week_data):
        """
        Calculates the growth percentage between two datasets.

        Args:
            current_data (dict): A dictionary containing the current data.
            last_week_data (dict): A dictionary containing the data from the previous week.

        Returns:
            dict: A dictionary containing the growth percentage for each metric in each key.

        Raises:
            ValueError: If the keys in the current and last week data are different.
        """
        # Check if both datasets have the same keys
        if set(current_data.keys()) != set(last_week_data.keys()):
            raise ValueError("Data from different periods must have the same keys")

        # Calculate growth percentage
        growth_percentage = {}
        for key, current_values in current_data.items():
            growth_data = {}
            for metric, current_value in current_values.items():
                last_week_value = last_week_data[key].get(metric, 0)
                if last_week_value > 0:
                    growth_rate = (current_value - last_week_value) / last_week_value
                else:
                    growth_rate = 0 if current_value == 0 else 100
                growth_data[metric] = round(growth_rate, ndigits=4)
            growth_percentage[key] = growth_data

        return growth_percentage

    async def daily_growth(self, from_date: datetime.date, to_date: datetime.date, source: str, metrics: list = []):
        """
        Calculates the daily growth of chapter data compared to the previous week.

        This function asynchronously calculates the daily growth of various chapter data 
        metrics (reads and chapter purchases) for the specified date range 
        compared to the data from the previous week. It retrieves data for both weeks 
        using separate asynchronous calls and then computes growth percentages for 
        individual metrics within each data category.

        Args:
            from_date (datetime.date): The start date for the current week's data retrieval.
            to_date (datetime.date): The end date for the current week's data retrieval.
            source (str): The source of the data (e.g., 'app' or 'web').

        Returns:
            dict: A dictionary containing growth percentages for different data categories 
                (chapter reads and chapter purchase) and their respective metrics.
        """
        # Calculate date range for the previous week
        delta = (to_date - from_date) + timedelta(days=1)
        fromdate_lastweek = from_date - delta
        todate_lastweek = to_date - delta

        # Gather current week data asynchronously
        current_data_chapter_read, current_data_chapter_coin, current_data_chapter_adscoin, current_data_chapter_ads = await asyncio.gather(
            self.chapter_read(from_date=from_date, to_date=to_date, source=source),
            self.chapter_coin(from_date=from_date, to_date=to_date, source=source),
            self.chapter_adscoin(from_date=from_date, to_date=to_date, source=source),
            self.chapter_ads(from_date=from_date, to_date=to_date, source=source)
        )

        # Gather last week data asynchronously
        lastweek_data_chapter_read, lastweek_data_chapter_coin, lastweek_data_chapter_adscoin, lastweek_data_chapter_ads = await asyncio.gather(
            self.chapter_read(from_date=fromdate_lastweek, to_date=todate_lastweek, source=source),
            self.chapter_coin(from_date=fromdate_lastweek, to_date=todate_lastweek, source=source),
            self.chapter_adscoin(from_date=fromdate_lastweek, to_date=todate_lastweek, source=source),
            self.chapter_ads(from_date=fromdate_lastweek, to_date=todate_lastweek, source=source)
        )

        # Calculate overall chapter purchase metrics for the current week
        current_data_overall_chapter_purchase, current_data_overall_oldnew_chapter_purchase = await asyncio.gather(
            asyncio.to_thread(
                self.total_chapter_purchase,
                chapter_coin_data=current_data_chapter_coin,
                chapter_adscoin_data=current_data_chapter_adscoin,
                chapter_ads_data=current_data_chapter_ads,
                metrics_1="chapter_unique",
                metrics_2="chapter_count"
            ),
            asyncio.to_thread(
                self.total_chapter_purchase,
                chapter_coin_data=current_data_chapter_coin,
                chapter_adscoin_data=current_data_chapter_adscoin,
                chapter_ads_data=current_data_chapter_ads,
                metrics_1="old_user_count",
                metrics_2="new_user_count"
            )
        )

        # Calculate overall chapter purchase metrics for the last week
        last_week_data_overall_chapter_purchase, last_week_data_overall_oldnew_chapter_purchase = await asyncio.gather(
            asyncio.to_thread(
                self.total_chapter_purchase,
                chapter_coin_data=lastweek_data_chapter_coin,
                chapter_adscoin_data=lastweek_data_chapter_adscoin,
                chapter_ads_data=lastweek_data_chapter_ads,
                metrics_1="chapter_unique",
                metrics_2="chapter_count"
            ),
            asyncio.to_thread(
                self.total_chapter_purchase,
                chapter_coin_data=lastweek_data_chapter_coin,
                chapter_adscoin_data=lastweek_data_chapter_adscoin,
                chapter_ads_data=lastweek_data_chapter_ads,
                metrics_1="old_user_count",
                metrics_2="new_user_count"
            )
        )

        # Structure the current and last week data
        current_data = {
            "chapter_read_data": current_data_chapter_read,
            "chapter_coin_data": current_data_chapter_coin,
            "chapter_adscoin_data": current_data_chapter_adscoin,
            "chapter_ads_data": current_data_chapter_ads,
            "overall_chapter_purchase": current_data_overall_chapter_purchase,
            "overall_oldnew_chapter_purchase": current_data_overall_oldnew_chapter_purchase
        }

        last_week_data = {
            "chapter_read_data": lastweek_data_chapter_read,
            "chapter_coin_data": lastweek_data_chapter_coin,
            "chapter_adscoin_data": lastweek_data_chapter_adscoin,
            "chapter_ads_data": lastweek_data_chapter_ads,
            "overall_chapter_purchase": last_week_data_overall_chapter_purchase,
            "overall_oldnew_chapter_purchase": last_week_data_overall_oldnew_chapter_purchase
        }

        growth_percentage = await asyncio.to_thread(self.calculate_growth, current_data, last_week_data)

        if metrics:
            growth_percentage = {k : growth_percentage[k] for k in metrics}

        return growth_percentage

    async def chapter_read_dataframe(self, from_date: datetime.date, to_date: datetime.date, source: str = 'app') -> Dict[str, pd.DataFrame]:
        """
        Asynchronously analyze chapter reading data based on specified criteria.

        Args:
            from_date (datetime): Start date for analysis.
            to_date (datetime): End date for analysis.
            source (str, optional): Data source filter 'app' or 'web' (default is 'app').

        Returns:
            dict: Processed chapter read data as a dictionary with DataFrames and integer values.
        """
        await asyncio.to_thread(self._add_default_values, from_date=from_date, to_date=to_date, types="chapter_read")
        await asyncio.to_thread(self._process_date_column, types="chapter_read")
        
        # Filter data asynchronously
        df_read = await asyncio.to_thread(
            lambda: self.df_chapter_read[
                (self.df_chapter_read['source'] == source) &
                (self.df_chapter_read['tanggal'] >= from_date) &
                (self.df_chapter_read['tanggal'] <= to_date)
            ].copy()
        )

        # Add day number and name columns asynchronously
        df_read["tanggal"] = await asyncio.to_thread(pd.to_datetime, df_read["tanggal"])
        df_read['day_num'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_of_week)
        df_read['day_name'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_name())

        # Group and aggregate data asynchronously
        df_new_user, df_old_user, df_new_user_group, df_old_user_group = await asyncio.gather(
            asyncio.to_thread(lambda: df_read[
                (df_read['install_date'] >= from_date) & 
                (df_read['install_date'] <= to_date)
            ]),
            asyncio.to_thread(lambda: df_read[
                df_read['install_date'] < from_date
            ]),
            asyncio.to_thread(lambda: df_read[
                (df_read['install_date'] >= from_date) & 
                (df_read['install_date'] <= to_date)
            ].groupby(['tanggal'])['user_id'].nunique().reset_index().rename(columns={'user_id': 'pembaca_chapter_new_user'})),
            asyncio.to_thread(lambda: df_read[
                df_read['install_date'] < from_date
            ].groupby(['tanggal'])['user_id'].nunique().reset_index().rename(columns={'user_id': 'pembaca_chapter_old_user'}))
        )

        # Merge old and new user data asynchronously
        df_old_new_user = await asyncio.to_thread(
            lambda: pd.merge(df_new_user_group, df_old_user_group, on='tanggal', how='outer')
        )

        # Calculate various DataFrame aggregations asynchronously
        df_unique_count, df_day, df_genre, df_novel = await asyncio.gather(
            asyncio.to_thread(lambda: df_read.groupby(['tanggal']).agg(
                pembaca_chapter_unique=('user_id', 'nunique'),
                pembaca_chapter_count=('chapter_count', 'sum')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['day_num', 'day_name']).agg(
                total_pembaca=('user_id', 'nunique')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['category_name']).agg(
                total_pembaca=('user_id', 'nunique')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['novel_id', 'novel_title']).agg(
                pembaca_chapter_unique=('user_id', 'nunique'),
                pembaca_chapter_count=('chapter_count', 'sum')
            ).reset_index())
        )

        # Calculate new and old user counts asynchronously
        new_user_count, old_user_count = await asyncio.gather(
            asyncio.to_thread(lambda: df_new_user['user_id'].nunique()),
            asyncio.to_thread(lambda: df_old_user['user_id'].nunique())
        )

        # Return all results in a dictionary
        container = {
            'dataframe': df_read.copy(),
            'df_unique_count': df_unique_count,
            'df_old_new': df_old_new_user,
            'df_day': df_day,
            'df_genre': df_genre,
            'df_novel': df_novel,
            'new_user_count': new_user_count,
            'old_user_count': old_user_count,
        }

        return container

    async def chapter_ads_dataframe(self, from_date: datetime.date, to_date: datetime.date, source: str = 'app') -> Dict[str, pd.DataFrame]:
        """
        Asynchronously analyze chapter ads data based on specified criteria.

        Args:
            from_date (datetime): Start date for analysis.
            to_date (datetime): End date for analysis.
            source (str, optional): Data source filter 'app' or 'web' (default is 'app').

        Returns:
            dict: Processed chapter ads data as a dictionary with DataFrames and integer values.
        """
        # Add default values and process the date column asynchronously
        await asyncio.to_thread(self._add_default_values, from_date=from_date, to_date=to_date, types="chapter_ads")
        await asyncio.to_thread(self._process_date_column, types="chapter_ads")
        
        # Filter data asynchronously
        df_read = await asyncio.to_thread(
            lambda: self.df_chapter_ads[
                (self.df_chapter_ads['source'] == source) &
                (self.df_chapter_ads['tanggal'] >= from_date) &
                (self.df_chapter_ads['tanggal'] <= to_date)
            ].copy()
        )

        # Add day number and day name asynchronously
        df_read["tanggal"] = await asyncio.to_thread(pd.to_datetime, df_read["tanggal"])
        df_read['day_num'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_of_week)
        df_read['day_name'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_name())

        # Group and aggregate data asynchronously
        df_new_user, df_old_user, df_new_user_group, df_old_user_group = await asyncio.gather(
            asyncio.to_thread(lambda: df_read[
                (df_read['install_date'] >= from_date) &
                (df_read['install_date'] <= to_date)
            ]),
            asyncio.to_thread(lambda: df_read[
                df_read['install_date'] < from_date
            ]),
            asyncio.to_thread(lambda: df_read[
                (df_read['install_date'] >= from_date) &
                (df_read['install_date'] <= to_date)
            ].groupby(['tanggal'])['user_id'].nunique().reset_index().rename(columns={'user_id': 'chapter_admob_new_user'})),
            asyncio.to_thread(lambda: df_read[
                df_read['install_date'] < from_date
            ].groupby(['tanggal'])['user_id'].nunique().reset_index().rename(columns={'user_id': 'chapter_admob_old_user'}))
        )

        # Merge old and new user data asynchronously
        df_old_new_user = await asyncio.to_thread(
            lambda: pd.merge(df_new_user_group, df_old_user_group, on='tanggal', how='outer')
        )

        # Perform the remaining group-by operations asynchronously
        df_unique_count, df_day, df_genre, df_novel = await asyncio.gather(
            asyncio.to_thread(lambda: df_read.groupby(['tanggal']).agg(
                chapter_admob_unique=('user_id', 'nunique'),
                chapter_admob_count=('chapter_count', 'sum')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['day_num', 'day_name']).agg(
                total_pembeli=('user_id', 'nunique')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['category_name']).agg(
                total_pembeli=('user_id', 'nunique')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['novel_id', 'novel_title']).agg(
                pembeli_chapter_admob_unique=('user_id', 'nunique'),
                pembeli_chapter_admob_count=('chapter_count', 'sum')
            ).reset_index())
        )

        # Calculate new and old user counts asynchronously
        new_user_count, old_user_count = await asyncio.gather(
            asyncio.to_thread(lambda: df_new_user['user_id'].nunique()),
            asyncio.to_thread(lambda: df_old_user['user_id'].nunique())
        )

        # Return all results in a dictionary
        container = {
            'dataframe': df_read.copy(),
            'df_unique_count': df_unique_count,
            'df_old_new': df_old_new_user,
            'df_day': df_day,
            'df_genre': df_genre,
            'df_novel': df_novel,
            'new_user_count': new_user_count,
            'old_user_count': old_user_count,
        }

        return container

    async def chapter_coin_dataframe(self, from_date: datetime.date, to_date: datetime.date, source: str = 'app') -> Dict[str, pd.DataFrame]:
        """
        Asynchronously analyze chapter coin data based on specified criteria.

        Args:
            from_date (datetime): Start date for analysis.
            to_date (datetime): End date for analysis.
            source (str, optional): Data source filter 'app' or 'web' (default is 'app').

        Returns:
            dict: Processed chapter coin data as a dictionary with DataFrames and integer values.
        """
        # Add default values and process the date column asynchronously
        await asyncio.to_thread(self._add_default_values, from_date=from_date, to_date=to_date, types="chapter_coin")
        await asyncio.to_thread(self._process_date_column, types="chapter_coin")

        # Filter data asynchronously
        df_read = await asyncio.to_thread(
            lambda: self.df_chapter_coin[
                (self.df_chapter_coin['source'] == source) &
                (self.df_chapter_coin['tanggal'] >= from_date) &
                (self.df_chapter_coin['tanggal'] <= to_date)
            ].copy()
        )

        # Add day number and day name asynchronously
        df_read["tanggal"] = await asyncio.to_thread(pd.to_datetime, df_read["tanggal"])
        df_read['day_num'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_of_week)
        df_read['day_name'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_name())

        # Group and aggregate data asynchronously
        df_new_user, df_old_user, df_new_user_group, df_old_user_group = await asyncio.gather(
            asyncio.to_thread(lambda: df_read[
                (df_read['install_date'] >= from_date) &
                (df_read['install_date'] <= to_date)
            ]),
            asyncio.to_thread(lambda: df_read[
                df_read['install_date'] < from_date
            ]),
            asyncio.to_thread(lambda: df_read[
                (df_read['install_date'] >= from_date) &
                (df_read['install_date'] <= to_date)
            ].groupby(['tanggal'])['user_id'].nunique().reset_index().rename(columns={'user_id': 'chapter_koin_new_user'})),
            asyncio.to_thread(lambda: df_read[
                df_read['install_date'] < from_date
            ].groupby(['tanggal'])['user_id'].nunique().reset_index().rename(columns={'user_id': 'chapter_koin_old_user'}))
        )

        # Merge old and new user data asynchronously
        df_old_new_user = await asyncio.to_thread(
            lambda: pd.merge(df_new_user_group, df_old_user_group, on='tanggal', how='outer')
        )

        # Perform the remaining group-by operations asynchronously
        df_unique_count, df_day, df_genre, df_novel = await asyncio.gather(
            asyncio.to_thread(lambda: df_read.groupby(['tanggal']).agg(
                chapter_koin_unique=('user_id', 'nunique'),
                chapter_koin_count=('chapter_count', 'sum')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['day_num', 'day_name']).agg(
                total_pembeli=('user_id', 'nunique')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['category_name']).agg(
                total_pembeli=('user_id', 'nunique')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['novel_id', 'novel_title']).agg(
                pembeli_chapter_koin_unique=('user_id', 'nunique'),
                pembeli_chapter_koin_count=('chapter_count', 'sum')
            ).reset_index())
        )

        # Calculate new and old user counts asynchronously
        new_user_count, old_user_count = await asyncio.gather(
            asyncio.to_thread(lambda: df_new_user['user_id'].nunique()),
            asyncio.to_thread(lambda: df_old_user['user_id'].nunique())
        )

        # Return all results in a dictionary
        container = {
            'dataframe': df_read.copy(),
            'df_unique_count': df_unique_count,
            'df_old_new': df_old_new_user,
            'df_day': df_day,
            'df_genre': df_genre,
            'df_novel': df_novel,
            'new_user_count': new_user_count,
            'old_user_count': old_user_count,
        }

        return container

    async def chapter_adscoin_dataframe(self, from_date: datetime.date, to_date: datetime.date, source: str = 'app') -> Dict[str, pd.DataFrame]:
        """
        Asynchronously analyze chapter adscoin data based on specified criteria.

        Args:
            from_date (datetime): Start date for analysis.
            to_date (datetime): End date for analysis.
            source (str, optional): Data source filter 'app' or 'web' (default is 'app').

        Returns:
            dict: Processed chapter adscoin data as a dictionary with DataFrames and integer values.
        """
        # Add default values and process the date column asynchronously
        await asyncio.to_thread(self._add_default_values, from_date=from_date, to_date=to_date, types="chapter_adscoin")
        await asyncio.to_thread(self._process_date_column, types="chapter_adscoin")

        # Filter data asynchronously
        df_read = await asyncio.to_thread(
            lambda: self.df_chapter_adscoin[
                (self.df_chapter_adscoin['source'] == source) &
                (self.df_chapter_adscoin['tanggal'] >= from_date) &
                (self.df_chapter_adscoin['tanggal'] <= to_date)
            ].copy()
        )

        # Add day number and day name asynchronously
        df_read["tanggal"] = await asyncio.to_thread(pd.to_datetime, df_read["tanggal"])
        df_read['day_num'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_of_week)
        df_read['day_name'] = await asyncio.to_thread(lambda: df_read['tanggal'].dt.day_name())

        # Group and aggregate data asynchronously
        df_new_user, df_old_user, df_new_user_group, df_old_user_group = await asyncio.gather(
            asyncio.to_thread(lambda: df_read[
                (df_read['install_date'] >= from_date) &
                (df_read['install_date'] <= to_date)
            ]),
            asyncio.to_thread(lambda: df_read[
                df_read['install_date'] < from_date
            ]),
            asyncio.to_thread(lambda: df_read[
                (df_read['install_date'] >= from_date) &
                (df_read['install_date'] <= to_date)
            ].groupby(['tanggal'])['user_id'].nunique().reset_index().rename(columns={'user_id': 'chapter_adskoin_new_user'})),
            asyncio.to_thread(lambda: df_read[
                df_read['install_date'] < from_date
            ].groupby(['tanggal'])['user_id'].nunique().reset_index().rename(columns={'user_id': 'chapter_adskoin_old_user'}))
        )

        # Merge old and new user data asynchronously
        df_old_new_user = await asyncio.to_thread(
            lambda: pd.merge(df_new_user_group, df_old_user_group, on='tanggal', how='outer')
        )

        # Perform the remaining group-by operations asynchronously
        df_unique_count, df_day, df_genre, df_novel = await asyncio.gather(
            asyncio.to_thread(lambda: df_read.groupby(['tanggal']).agg(
                chapter_adskoin_unique=('user_id', 'nunique'),
                chapter_adskoin_count=('chapter_count', 'sum')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['day_num', 'day_name']).agg(
                total_pembeli=('user_id', 'nunique')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['category_name']).agg(
                total_pembeli=('user_id', 'nunique')
            ).reset_index()),
            asyncio.to_thread(lambda: df_read.groupby(['novel_id', 'novel_title']).agg(
                pembeli_chapter_adskoin_unique=('user_id', 'nunique'),
                pembeli_chapter_adskoin_count=('chapter_count', 'sum')
            ).reset_index())
        )

        # Calculate new and old user counts asynchronously
        new_user_count, old_user_count = await asyncio.gather(
            asyncio.to_thread(lambda: df_new_user['user_id'].nunique()),
            asyncio.to_thread(lambda: df_old_user['user_id'].nunique())
        )

        # Return all results in a dictionary
        container = {
            'dataframe': df_read.copy(),
            'df_unique_count': df_unique_count,
            'df_old_new': df_old_new_user,
            'df_day': df_day,
            'df_genre': df_genre,
            'df_novel': df_novel,
            'new_user_count': new_user_count,
            'old_user_count': old_user_count,
        }

        return container


async def pembaca_pembeli_chapter_unique(
        chapter_coin_data: dict, 
        chapter_adscoin_data: dict, 
        chapter_ads_data: dict, 
        chapter_read_data: dict, 
        period: str = 'daily', 
        data: str = 'unique', 
        source: str = 'app') -> str:
    """
    Generate a chart showing unique or count chapter readers and buyers.

    Parameters:
    - chapter_coin_data (dict): dict for fetching chapter coin data.
    - chapter_adscoin_data (dict): dict for fetching chapter ads coin data.
    - chapter_admob_data (dict): dict for fetching chapter admob data.
    - chapter_read_data (dict): dict for fetching chapter read data.
    - period (str): Periodicity of the data ('daily' or 'monthly').
    - data (str): Data type to display ('unique' or 'count').
    - source (str): Data source ('app' or 'web').

    Returns:
    - chart (str): JSON-encoded Plotly chart.
    """
    
    try:
        # Validate period parameter
        if period not in ['daily', 'monthly']:
            raise ValueError("Invalid period. Choose either 'daily' or 'monthly'.")
        
        # Validate data parameter
        if data not in ['unique', 'count']:
            raise ValueError("Invalid data type. Choose either 'unique' or 'count'.")
        
        # Fetch data
        df_koin = chapter_coin_data['df_unique_count']
        df_adskoin = chapter_adscoin_data['df_unique_count']
        df_admob = chapter_ads_data['df_unique_count']
        df_pembca = chapter_read_data['df_unique_count']
        
        # Merge dataframes
        merge_1 = await asyncio.to_thread(
            lambda: pd.merge(df_pembca, df_admob, on='tanggal', how='outer')
        )
        merge_2 = await asyncio.to_thread(
            lambda: pd.merge(merge_1, df_koin, on='tanggal', how='outer')
        )
        df = await asyncio.to_thread(
            lambda: pd.merge(merge_2, df_adskoin, on='tanggal', how='outer')
        )
        
        # Process data
        await asyncio.to_thread(
            lambda: df.sort_values(by='tanggal', ascending=True, inplace=True))
        await asyncio.to_thread(
            lambda: df.fillna(0, inplace=True))
        df["chapter_koin_unique"] = await asyncio.to_thread(lambda: df["chapter_koin_unique"].astype(int))
        df["chapter_adskoin_unique"] = await asyncio.to_thread(lambda: df["chapter_adskoin_unique"].astype(int))
        df["chapter_admob_unique"] = await asyncio.to_thread(lambda: df["chapter_admob_unique"].astype(int))
        df["chapter_koin_count"] = await asyncio.to_thread(lambda: df["chapter_koin_count"].astype(int))
        df["chapter_adskoin_count"] = await asyncio.to_thread(lambda: df["chapter_adskoin_count"].astype(int))
        df["chapter_admob_count"] = await asyncio.to_thread(lambda: df["chapter_admob_count"].astype(int))

        df['total_pembeli_chapter_unique'] = await asyncio.to_thread(
            lambda: df['chapter_koin_unique'] + df['chapter_adskoin_unique'] + df['chapter_admob_unique'])
        df['total_pembeli_chapter_count'] = await asyncio.to_thread(
            lambda: df['chapter_koin_count'] + df['chapter_adskoin_count'] + df['chapter_admob_count'])
        df['persentase_chapter_koin_unique'] = await asyncio.to_thread(
            lambda: df['chapter_koin_unique'] / df['total_pembeli_chapter_unique'])
        df['persentase_chapter_adskoin_unique'] = await asyncio.to_thread(
            lambda: df['chapter_adskoin_unique'] / df['total_pembeli_chapter_unique'])
        df['persentase_chapter_admob_unique'] = await asyncio.to_thread(
            lambda: df['chapter_admob_unique'] / df['total_pembeli_chapter_unique'])
        df['persentase_chapter_koin_count'] = await asyncio.to_thread(
            lambda: df['chapter_koin_count'] / df['total_pembeli_chapter_count'])
        df['persentase_chapter_adskoin_count'] = await asyncio.to_thread(
            lambda: df['chapter_adskoin_count'] / df['total_pembeli_chapter_count'])
        df['persentase_chapter_admob_count'] = await asyncio.to_thread(
            lambda: df['chapter_admob_count'] / df['total_pembeli_chapter_count'])
        
        # Define ad source
        ads = 'Admob' if source == 'app' else 'Adsense'
        
        # Create traces based on data type
        if data == 'unique':
            traces = [
                go.Bar(x=df['tanggal'], y=df['pembaca_chapter_unique'], name='Chapter Reader', text=df['pembaca_chapter_unique'].apply(lambda x: "{:,.0f}".format(x)), textposition='inside', yaxis='y'),
                go.Bar(x=df['tanggal'], y=df['total_pembeli_chapter_unique'], name='Chapter Purchase', text=df['total_pembeli_chapter_unique'].apply(lambda x: "{:,.0f}".format(x)), textposition='inside', yaxis='y'),
                go.Scatter(x=df['tanggal'], y=df['persentase_chapter_koin_unique'], name='% Chapter With Koin', yaxis='y2'),
                go.Scatter(x=df['tanggal'], y=df['persentase_chapter_adskoin_unique'], name='% Chapter With Adskoin', yaxis='y2'),
                go.Scatter(x=df['tanggal'], y=df['persentase_chapter_admob_unique'], name=f'% Chapter With {ads}', yaxis='y2')
            ]
        else:
            traces = [
                go.Bar(x=df['tanggal'], y=df['pembaca_chapter_count'], name='Chapter Reader', text=df['pembaca_chapter_count'].apply(lambda x: "{:,.0f}".format(x)), textposition='inside', yaxis='y'),
                go.Bar(x=df['tanggal'], y=df['total_pembeli_chapter_count'], name='Chapter Purchase', text=df['total_pembeli_chapter_count'].apply(lambda x: "{:,.0f}".format(x)), textposition='inside', yaxis='y'),
                go.Scatter(x=df['tanggal'], y=df['persentase_chapter_koin_count'], name='% Chapter With Koin', yaxis='y2'),
                go.Scatter(x=df['tanggal'], y=df['persentase_chapter_adskoin_count'], name='% Chapter With Adskoin', yaxis='y2'),
                go.Scatter(x=df['tanggal'], y=df['persentase_chapter_admob_count'], name=f'% Chapter With {ads}', yaxis='y2')
            ]
        
        # Define the layout with a secondary y-axis
        layout = go.Layout(
            title=f'{source.capitalize()} Chapter Reader & Purchase {data.capitalize()}',
            yaxis=dict(title='Value'),
            yaxis2=dict(title='Percentage', overlaying='y', side='right', tickformat='.2%')
        )
        
        # Create the figure
        fig = go.Figure(data=traces, layout=layout)
        
        # Update x-axis based on period
        if period == 'daily':
            fig.update_xaxes(title='Date', dtick='D1')
        elif period == 'monthly':
            fig.update_xaxes(title='Date', dtick='M1')
        
        # Convert figure to JSON
        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        return chart
    
    except Exception as e:
        return json.dumps({'error': str(e)})


def total_pembeli_chapter(
        chapter_coin_data: dict, 
        chapter_adscoin_data: dict, 
        chapter_ads_data: dict, 
        data: str = 'unique', 
        metrics_1: str = 'chapter_unique', 
        metrics_2: str = 'chapter_count') -> int:
    """
    Calculates the total chapter purchases, either unique buyers or total count.

    Args:
        chapter_coin_data: Object providing chapter_coin data.
        chapter_adscoin_data: Object providing chapter_adscoin data.
        chapter_ads_data: Object providing chapter_admob data.
        data: Type of data to return ('unique' or 'count').
        metrics_1: Name of the metric for unique buyers ('chapter_unique' or 'old_user_count').
        metrics_2: Name of the metric for total count ('chapter_count' or 'new_user_count').

    Returns:
        int : Total chapter purchases (unique or count) based on the 'data' argument.
    """

    # Input Validation and Error Handling
    if not all([chapter_coin_data, chapter_adscoin_data, chapter_ads_data]):
        raise ValueError("All chapter data objects (object_1, object_2, object_3) are required.")

    if data not in ['unique', 'count']:
        raise ValueError("Invalid 'data' argument. Choose 'unique' or 'count'.")

    # Data Retrieval (with Exception Handling)
    try:
        df_koin_unique = chapter_coin_data[metrics_1]
        df_koin_count = chapter_coin_data[metrics_2]
        df_adskoin_unique = chapter_adscoin_data[metrics_1]
        df_adskoin_count = chapter_adscoin_data[metrics_2]
        df_admob_unique = chapter_ads_data[metrics_1]
        df_admob_count = chapter_ads_data[metrics_2]
    except Exception as e:
        raise RuntimeError(f"Error retrieving chapter data: {e}") 

    # Calculation (Ensure index alignment)
    total_unique = df_koin_unique + df_adskoin_unique + df_admob_unique
    total_count = df_koin_count + df_adskoin_count + df_admob_count

    # Return Result
    return total_unique if data == 'unique' else total_count


async def chart_total_chapter_purchase(
        chapter_coin_data: dict = None, 
        chapter_adscoin_data: dict = None, 
        chapter_ads_data: dict = None, 
        chapter_read_data: dict = None, 
        metrics_1: str = 'chapter_unique', 
        metrics_2: str = 'chapter_count') -> str:
    """
    Creates a pie chart illustrating total chapter purchases or reads based on different metrics.

    Args:
        chapter_coin_data: Object responsible for fetching chapter purchase data via coins.
        chapter_adscoin_data: Object responsible for fetching chapter purchase data via ad coins.
        chapter_ads_data: Object responsible for fetching chapter purchase data via AdMob.
        chapter_read_data: Object responsible for fetching chapter reading data.
        metrics_1 (str, optional): Primary metric to display ('chapter_unique', 'old_user_count', 'chapter_read'). Defaults to 'chapter_unique'.
        metrics_2 (str, optional): Secondary metric used for certain calculations. Defaults to 'chapter_count'.

    Returns:
        str: JSON representation of a Plotly pie chart, or an error message if data retrieval or processing fails.
    """

    try:
        # Data Retrieval and Processing
        if metrics_1 == 'chapter_unique':
            df_1 = await asyncio.to_thread(
                total_pembeli_chapter,
                chapter_coin_data=chapter_coin_data, chapter_adscoin_data=chapter_adscoin_data, chapter_ads_data=chapter_ads_data, 
                data='unique', metrics_1=metrics_1, metrics_2=metrics_2
            )
            df_2 = await asyncio.to_thread(
                total_pembeli_chapter,
                chapter_coin_data=chapter_coin_data, chapter_adscoin_data=chapter_adscoin_data, chapter_ads_data=chapter_ads_data, 
                data='count', metrics_1=metrics_1, metrics_2=metrics_2
            )
            labels = ['Unique Chapter Purchase', 'Count Chapter Purchase']
        elif metrics_1 == 'old_user_count':
            df_1 = await asyncio.to_thread(
                total_pembeli_chapter,
                chapter_coin_data=chapter_coin_data, chapter_adscoin_data=chapter_adscoin_data, chapter_ads_data=chapter_ads_data, 
                data='unique', metrics_1=metrics_1, metrics_2=metrics_2
            )
            df_2 = await asyncio.to_thread(
                total_pembeli_chapter,
                chapter_coin_data=chapter_coin_data, chapter_adscoin_data=chapter_adscoin_data, chapter_ads_data=chapter_ads_data, 
                data='count', metrics_1=metrics_1, metrics_2=metrics_2
            )
            labels = ['Old User Chapter Purchase', 'New User Chapter Purchase']
        elif metrics_1 == 'chapter_read':
            df_1 = chapter_read_data['old_user_count']
            df_2 = chapter_read_data['new_user_count']
            labels = ['Old User Chapter Read', 'New User Chapter Read']
        else:
            raise ValueError("Invalid metrics_1 value")
        
        df = pd.DataFrame({'User Count': [df_1, df_2]}, index=labels)

        # Visualization
        fig = go.Figure(go.Pie(labels=df.index, values=df['User Count']))
        title = {
            'chapter_unique': 'Total Chapter Purchase Unique & Count',
            'old_user_count': 'Total Chapter Purchase Old User & New User',
            'chapter_read': 'Total Chapter Read Old User & New User'
        }
        fig.update_layout(
            title=title[metrics_1]
        )  

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        return chart

    except Exception as e:
        return json.dumps({"error": str(e)})
    

async def old_new_user_pembaca_chapter(chapter_read_data: dict) -> str:
    """Generates a Plotly chart visualizing old and new user chapter readership.

    Args:
        chapter_read_data (Dictl): Data access Dict for chapter read data.

    Returns:
        str: JSON-serialized Plotly chart.
    """

    if not chapter_read_data:
        raise ValueError("chapter_read_data is required.")

    try:
        df = chapter_read_data['df_old_new']
    
        # Convert 'tanggal' to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['tanggal']):
            df['tanggal'] = pd.to_datetime(df['tanggal'], errors='coerce')

    except AttributeError:
        raise ValueError("chapter_read_obj does not have a chapter_read method.")

    # Ensure columns exist (with type casting for safety)
    for col in ['tanggal', 'pembaca_chapter_old_user', 'pembaca_chapter_new_user']:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in data.")
        
        if col != 'tanggal':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    trace1 = go.Scatter(
        yaxis='y',
        x=df['tanggal'],
        y=df['pembaca_chapter_new_user'],
        name='New User',
        text=df['pembaca_chapter_new_user'].apply(lambda x: "{:,.0f}".format((x))),
        textposition='top center')

    trace2 = go.Scatter(
        yaxis='y2',
        x=df['tanggal'],
        y=df['pembaca_chapter_old_user'],
        name='Old User',
        text=df['pembaca_chapter_old_user'].apply(lambda x: "{:,.0f}".format((x))),
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

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def old_new_user_pembeli_chapter(
        chapter_coin_data: dict,
        chapter_adscoin_data: dict,
        chapter_ads_data: dict) -> str:
    """
    Generate a chart showing chapter purchases by old and new users.

    Parameters:
    - chapter_coin_data (dict): dict for fetching chapter coin data.
    - chapter_adscoin_data (dict): dict for fetching chapter ads coin data.
    - chapter_admob_data (dict): dict for fetching chapter ads data.

    Returns:
    - chart (str): JSON-encoded Plotly chart.
    """
    try:
        # Fetch data
        df_koin = chapter_coin_data['df_old_new']
        df_adskoin = chapter_adscoin_data['df_old_new']
        df_admob = chapter_ads_data['df_old_new']
        
        # Merge dataframes
        merge_1 = await asyncio.to_thread(
            lambda: pd.merge(df_admob, df_adskoin, on='tanggal', how='outer'))
        df = await asyncio.to_thread(
            lambda: pd.merge(merge_1, df_koin, on='tanggal', how='outer'))
        
        # Process data
        await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
        df['total_new_user'] = await asyncio.to_thread(
            lambda: df['chapter_koin_new_user'] + df['chapter_adskoin_new_user'] + df['chapter_admob_new_user'])
        df['total_old_user'] = await asyncio.to_thread(
            lambda: df['chapter_koin_old_user'] + df['chapter_adskoin_old_user'] + df['chapter_admob_old_user'])
        
        # Create the figure
        trace1 = go.Scatter(
            yaxis='y',
            x=df['tanggal'],
            y=df['total_new_user'],
            name='New User',
            text=df['total_new_user'].apply(lambda x: "{:,.0f}".format((x))),
            textposition='top center')

        trace2 = go.Scatter(
            yaxis='y2',
            x=df['tanggal'],
            y=df['total_old_user'],
            name='Old User',
            text=df['total_old_user'].apply(lambda x: "{:,.0f}".format((x))),
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
        
        # Convert figure to JSON
        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        return chart
    
    except Exception as e:
        return json.dumps({'error': str(e)})


async def pembaca_chapter_by_day(
        chapter_read_data: dict) -> str:
    """
    Fetches chapter reader data and generates a Plotly bar chart for visualization.

    Args:
        chapter_read_data (dict): dict with a chapter_read method.

    Returns:
        str: JSON string representing the Plotly chart configuration.
    """

    if chapter_read_data is None:
        raise ValueError("chapter_read_obj must be provided.")

    df = chapter_read_data['df_day']

    fig = go.Figure(
        go.Bar(
            x=df['day_name'],
            y=df['total_pembaca'],
            name='Total Readers',  # Plural for clarity
            text=df['total_pembaca'].apply(lambda x: f"{x:,.0f}"),  # f-strings for formatting
            textposition='inside',
            hovertemplate='%{x}: %{y:,.0f} readers<extra></extra>',  # Enhanced hover info
        )
    )

    fig.update_layout(
        title='Chapter Readers Per Day',  # Consistent title
        xaxis_title='Day',
        yaxis_title='Total Readers',
    )

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def pembeli_chapter_by_day(
        chapter_coin_data: dict, 
        chapter_adscoin_data: dict, 
        chapter_ads_data: dict) -> str:
    """
    Retrieves and visualizes the total chapter purchases per day, aggregating data from various sources, with error handling and logging.

    Args:
        chapter_coin_data (dict): dict for fetching chapter purchase data via coins.
        chapter_adscoin_data (dict): dict for fetching chapter purchase data via ad coins.
        chapter_ads_data (dict): dict for fetching chapter purchase data via Ads.

    Returns:
        str: JSON representation of a Plotly bar chart depicting chapter purchases per day, 
             or an error message if data retrieval or processing fails.
    """

    try:
        # Data Retrieval
        df_koin = chapter_coin_data['df_day']
        df_adskoin = chapter_adscoin_data['df_day']
        df_admob = chapter_ads_data['df_day']

        # Data Merging and Processing
        merged_df = await asyncio.to_thread(
            lambda: pd.merge(df_admob, df_adskoin, on=['day_num', 'day_name'], how='outer')
        )
        df = await asyncio.to_thread(
            lambda: pd.merge(merged_df, df_koin, on=['day_num', 'day_name'], how='outer').fillna(0)
        )
        df['total_pembeli_all'] = df['total_pembeli_x'] + df['total_pembeli_y'] + df['total_pembeli']

        # Visualization
        fig = go.Figure(
            go.Bar(
                x=df['day_name'],
                y=df['total_pembeli_all'],
                name='Total Purchase',
                text=df['total_pembeli_all'].apply(lambda x: f"{x:,.0f}"),
                textposition='inside'
            )
        )
        fig.update_layout(title='Chapter Purchase Per Day')
        fig.update_xaxes(title='Day')
        fig.update_yaxes(title='Total Purchase')

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        return chart

    except Exception as e:
        return json.dumps({"error": str(e)})


async def pembaca_chapter_by_genre(chapter_read_data: dict):
    """
    Retrieves and visualizes the percentage of chapter readers per genre, with error handling and logging.

    Args:
        chapter_read_data (dict): dict responsible for fetching chapter reading data.

    Returns:
        str: JSON representation of a Plotly bar chart depicting chapter readers per genre,
             or an error message if data retrieval or processing fails.
    """

    try:
        # Data Retrieval
        df = chapter_read_data['df_genre']

        # Data Processing
        df['total'] = await asyncio.to_thread(
            lambda: df['total_pembaca'].sum())
        df['persentase'] = await asyncio.to_thread(
            lambda:  df['total_pembaca'] / df['total'])
        await asyncio.to_thread(
            lambda: df.sort_values(by='persentase', ascending=False, inplace=True))
        
        formated_values = await asyncio.to_thread(
            lambda: df["persentase"].apply(lambda x: f"{x:.2%}")
        )

        # Visualization
        fig = go.Figure(
            go.Bar(
                x=df['category_name'],
                y=df['persentase'],
                name='Total Reader',
                text=formated_values,
                textposition='inside'
            )
        )
        fig.update_layout(title='Chapter Readers Per Genre')  # Corrected title
        fig.update_xaxes(title='Genre')
        fig.update_yaxes(title='Percentage of Readers')

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        return chart

    except Exception as e:\
        return json.dumps({"error": str(e)})
    

async def pembeli_chapter_by_genre(
        chapter_coin_data: dict, 
        chapter_adscoin_data: dict, 
        chapter_ads_data: dict) -> str:
    """
    Retrieves and visualizes the percentage of chapter purchases per genre, aggregating data from multiple sources,
    with error handling and logging.

    Args:
        chapter_coin_data (dict): dict responsible for fetching chapter purchase data via coins.
        chapter_adscoin_data (dict): dict responsible for fetching chapter purchase data via ad coins.
        chapter_admob_data (dict): dict responsible for fetching chapter purchase data via ads.

    Returns:
        str: JSON representation of a Plotly bar chart depicting chapter purchases per genre,
             or an error message if data retrieval or processing fails.
    """
    try:
        # Data Retrieval
        df_koin = chapter_coin_data['df_genre']
        df_adskoin = chapter_adscoin_data['df_genre']
        df_admob = chapter_ads_data['df_genre']

        # Data Merging and Processing
        merged_df = await asyncio.to_thread(
            lambda: pd.merge(df_admob, df_adskoin, on='category_name', how='outer')
        )
        df = await asyncio.to_thread(
            lambda: pd.merge(merged_df, df_koin, on='category_name', how='outer').fillna(0)
        )
        df['total_pembeli_all'] = await asyncio.to_thread(
            lambda: df['total_pembeli_x'] + df['total_pembeli_y'] + df['total_pembeli']
        )
        df['total'] = await asyncio.to_thread( 
            lambda: df['total_pembeli_all'].sum()
        )
        df['persentase'] = await asyncio.to_thread(
            lambda: df['total_pembeli_all'] / df['total']
        )
        await asyncio.to_thread(
            lambda: df.sort_values(by='persentase', ascending=False, inplace=True)
        )

        formated_values = await asyncio.to_thread(lambda: df['persentase'].apply(lambda x: "{:,.2%}".format(x)))

        # Visualization
        fig = go.Figure(
            go.Bar(
                x=df['category_name'],
                y=df['persentase'],
                name='Total Purchase',
                text=formated_values,
                textposition='inside',
            )
        )

        fig.update_layout(
            title='Chapter Purchase Percentage Per Genre'
        )  # More accurate title
        fig.update_xaxes(title='Genre')
        fig.update_yaxes(title='Purchase Percentage')

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        return chart

    except Exception as e:
        return json.dumps({"error": str(e)})


async def pembaca_chapter_table(
        chapter_coin_data: dict, 
        chapter_adscoin_data: dict, 
        chapter_ads_data: dict, 
        chapter_read_data: dict, 
        sort_by: str = 'pembaca_chapter_unique', 
        ascending: bool = False, 
        source: str = 'app') -> str:
    """
    Generate a table showing chapter readers and purchases per novel.

    Parameters:
    - chapter_coin_data (dict): dict for fetching chapter coin data.
    - chapter_adscoin_data (dict): dict for fetching chapter ads coin data.
    - chapter_admob_data (dict): dict for fetching chapter admob data.
    - chapter_read_data (dict): dict for fetching chapter read data.
    - sort_by (str): Column name to sort by.
    - ascending (bool): Sort order (False for descending, True for ascending).
    - source (str): Data source ('app' or 'web').

    Returns:
    - chart (str): JSON-encoded Plotly chart.
    """
    try:
        # Fetch data
        df_baca = chapter_read_data['df_novel']
        df_koin = chapter_coin_data['df_novel']
        df_adskoin = chapter_adscoin_data['df_novel']
        df_admob = chapter_ads_data['df_novel']
        
        # Merge dataframes
        merge_1 = await asyncio.to_thread(
            lambda: pd.merge(df_baca, df_adskoin, on=['novel_id', 'novel_title'], how='left')
        )
        merge_2 = await asyncio.to_thread(
            lambda: pd.merge(merge_1, df_koin, on=['novel_id', 'novel_title'], how='left')
        )
        df = await asyncio.to_thread(
            lambda: pd.merge(merge_2, df_admob, on=['novel_id', 'novel_title'], how='left')
        )
        
        # Process data
        await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
        df["pembeli_chapter_koin_unique"] = await asyncio.to_thread(lambda: df["pembeli_chapter_koin_unique"].astype(int))
        df["pembeli_chapter_adskoin_unique"] = await asyncio.to_thread(lambda: df["pembeli_chapter_adskoin_unique"].astype(int))
        df["pembeli_chapter_admob_unique"] = await asyncio.to_thread(lambda: df["pembeli_chapter_admob_unique"].astype(int))

        df["pembeli_chapter_koin_count"] = await asyncio.to_thread(lambda: df["pembeli_chapter_koin_count"].astype(int))
        df["pembeli_chapter_adskoin_count"] = await asyncio.to_thread(lambda: df["pembeli_chapter_adskoin_count"].astype(int))
        df["pembeli_chapter_admob_count"] = await asyncio.to_thread(lambda: df["pembeli_chapter_admob_count"].astype(int))

        df['total_pembeli_chapter_unique'] = await asyncio.to_thread(
            lambda: df['pembeli_chapter_koin_unique'] + df['pembeli_chapter_adskoin_unique'] + df['pembeli_chapter_admob_unique']
        )
        df['total_pembeli_chapter_count'] = await asyncio.to_thread(
            lambda: df['pembeli_chapter_koin_count'] + df['pembeli_chapter_adskoin_count'] + df['pembeli_chapter_admob_count']
        )
        
        # Sort data
        await asyncio.to_thread(
            lambda: df.sort_values(by=sort_by, ascending=ascending, inplace=True)
        )
        
        # Set the ads variable
        ads = 'Admob' if source == 'app' else 'Adsense'
        
        formatted_values = await asyncio.to_thread(lambda: [
            df['novel_id'], df['novel_title'],
            df['pembaca_chapter_unique'].apply(lambda x: "{:,.0f}".format(x)),
            df['pembaca_chapter_count'].apply(lambda x: "{:,.0f}".format(x)),
            df['pembeli_chapter_adskoin_unique'].apply(lambda x: "{:,.0f}".format(x)),
            df['pembeli_chapter_adskoin_count'].apply(lambda x: "{:,.0f}".format(x)),
            df['pembeli_chapter_koin_unique'].apply(lambda x: "{:,.0f}".format(x)),
            df['pembeli_chapter_koin_count'].apply(lambda x: "{:,.0f}".format(x)),
            df['pembeli_chapter_admob_unique'].apply(lambda x: "{:,.0f}".format(x)),
            df['pembeli_chapter_admob_count'].apply(lambda x: "{:,.0f}".format(x)),
            df['total_pembeli_chapter_unique'].apply(lambda x: "{:,.0f}".format(x)),
            df['total_pembeli_chapter_count'].apply(lambda x: "{:,.0f}".format(x)),
        ])

        # Create the figure
        fig = go.Figure(
            go.Table(
                columnorder=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                columnwidth=[25, 150, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60],
                header=dict(
                    fill_color="grey",
                    line_color="black",
                    font=dict(color="black"),
                    values=[
                        'Novel Id', 'Novel Title', 'Chapter Reader Unique', 'Chapter Reader Count',
                        'Chapter Purchase Unique With AdsKoin', 'Chapter Purchase Count With AdsKoin',
                        'Chapter Purchase Unique With Koin', 'Chapter Purchase Count With Koin',
                        f'Chapter Purchase Unique With {ads}', f'Chapter Purchase Count With {ads}',
                        'Total Chapter Purchase Unique', 'Total Chapter Purchase Count'
                    ]),
                cells=dict(
                    fill_color="white",
                    line_color="black",
                    font=dict(color="black"),
                    values=formatted_values)
            )
        )
        
        # Update layout
        title = 'App Chapter Reader & Purchase Per Novel' if source == 'app' else 'Web Chapter Reader & Purchase Per Novel'
        fig.update_layout(title=title)
        
        # Convert figure to JSON
        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        return chart
    
    except Exception as e:
        return json.dumps({'error': str(e)})


async def user_activity(
        from_date:datetime.date, 
        to_date:datetime.date, 
        chapter_read_data:dict, 
        chapter_coin_data:dict, 
        chapter_adscoin_data: dict, 
        chapter_ads_data: dict, 
        source: str = 'app') -> str:
    """
    Generate a user activity chart showing various activities related to chapter reading and purchasing.

    This function creates a chart displaying user activity metrics such as chapter reading and purchases,
    segmented by different types of purchases (e.g., Admob, Adsense, Koin) over a specified time period.

    Args:
        from_date (datetime.date): The start date of the time period in 'YYYY-MM-DD' format. Defaults to None.
        to_date (datetime.date): The end date of the time period in 'YYYY-MM-DD' format. Defaults to None.
        chapter_read_data (dict): dict representing a data source for chapter reading activity.
        chapter_coin_data (dict): dict representing a data source for chapter coin purchases activity.
        chapter_adscoin_data (dict): dict representing a data source for chapter adscoin purchases activity.
        chapter_ads_data (dict): dict representing a data source for chapter admob purchases activity.
        source (str, optional): The source of the data ('app' or 'web'). Defaults to 'app'.

    Returns:
        str: A JSON string representing the user activity chart.

    Note:
        Requires data for chapter reading and different types of chapter purchases.

    Example:
        user_activity(from_date='2023-01-01', to_date='2023-01-31', object_1=chapter_read_object, object_2=chapter_coin_object, object_3=chapter_adscoin_object, object_4=chapter_admob_object, source='app')
    """

    # Preproccess chapter read data asynchronously
    chapter_read = chapter_read_data['df_unique_count']
    chapter_read = await asyncio.to_thread(lambda: chapter_read.loc[:, ['tanggal', 'pembaca_chapter_unique']])
    chapter_read['tanggal'] = await asyncio.to_thread(pd.to_datetime, chapter_read['tanggal'])
    chapter_read["tanggal"] = await asyncio.to_thread(lambda: chapter_read["tanggal"].dt.date)
    await asyncio.to_thread(
        lambda: chapter_read.rename(columns={'tanggal':'date', 'pembaca_chapter_unique':'pembaca_chapter'}, inplace=True)
    )

    # Preproccess chapter coin data asynchronously
    chapter_coin = chapter_coin_data['df_unique_count']
    chapter_coin = await asyncio.to_thread(lambda: chapter_coin.loc[:, ['tanggal', 'chapter_koin_unique']])
    chapter_coin['tanggal'] = await asyncio.to_thread(pd.to_datetime, chapter_coin['tanggal'])
    chapter_coin["tanggal"] = await asyncio.to_thread(lambda: chapter_coin["tanggal"].dt.date)
    await asyncio.to_thread(
        lambda:chapter_coin.rename(columns={'tanggal':'date', 'chapter_koin_unique':'pembeli_chapter_koin'}, inplace=True)
    )

    # Preproccess chapter ads coin data asynchronously
    chapter_adscoin = chapter_adscoin_data['df_unique_count']
    chapter_adscoin = await asyncio.to_thread(lambda: chapter_adscoin.loc[:, ['tanggal', 'chapter_adskoin_unique']])
    chapter_adscoin['tanggal'] = await asyncio.to_thread(pd.to_datetime,chapter_adscoin['tanggal'])
    chapter_adscoin["tanggal"] = await asyncio.to_thread(lambda: chapter_adscoin["tanggal"].dt.date)
    await asyncio.to_thread(
        lambda: chapter_adscoin.rename(columns={'tanggal':'date', 'chapter_adskoin_unique':'pembeli_chapter_adskoin'}, inplace=True)
    )

    # Preproccess chapter ads data asynchronously
    chapter_ads = chapter_ads_data['df_unique_count'] 
    chapter_ads = await asyncio.to_thread(lambda: chapter_ads.loc[:, ['tanggal', 'chapter_admob_unique']])
    chapter_ads['tanggal'] = await asyncio.to_thread(pd.to_datetime, chapter_ads['tanggal'])
    chapter_ads["tanggal"] = await asyncio.to_thread(lambda: chapter_ads["tanggal"].dt.date)
    await asyncio.to_thread(
        lambda: chapter_ads.rename(columns={'tanggal':'date', 'chapter_admob_unique':'pembeli_chapter_admob'}, inplace=True)
    )
    
    # merge the data
    df_merge_1 = await asyncio.to_thread(
        lambda: pd.merge(chapter_read, chapter_coin, on='date', how='outer')
    )
    df_merge_1['date'] = await asyncio.to_thread(pd.to_datetime, df_merge_1['date'])
    df_merge_1["date"] = await asyncio.to_thread(lambda: df_merge_1["date"].dt.date)
    await asyncio.to_thread(
        lambda: df_merge_1.fillna(0, inplace=True)
    )

    df_merge_2 = await asyncio.to_thread(
        lambda: pd.merge(df_merge_1, chapter_adscoin, on='date', how='outer')
    )
    df_merge_2['date'] = await asyncio.to_thread(pd.to_datetime, df_merge_2['date'])
    df_merge_2["date"] = await asyncio.to_thread(lambda: df_merge_2["date"].dt.date)
    await asyncio.to_thread(
        lambda: df_merge_2.fillna(0, inplace=True)
    )

    df = await asyncio.to_thread(
        lambda: pd.merge(df_merge_2, chapter_ads, on='date', how='outer')
    )
    df['date'] = await asyncio.to_thread(pd.to_datetime, df['date'])
    df["date"] = await asyncio.to_thread(lambda: df["date"].dt.date)
    await asyncio.to_thread(
        lambda: df.fillna(0, inplace=True)
    )

    # Calculate chapter reader to chapter buyet percentage
    df['pembaca_chapter'] = await asyncio.to_thread(lambda: df['pembaca_chapter'].astype(int))
    df['pembeli_chapter_koin'] = await asyncio.to_thread(lambda: df['pembeli_chapter_koin'].astype(int))
    df['pembeli_chapter_adskoin'] = await asyncio.to_thread(lambda: df['pembeli_chapter_adskoin'].astype(int))
    df['pembeli_chapter_admob'] = await asyncio.to_thread(lambda: df['pembeli_chapter_admob'].astype(int))
    df['total_pembeli_chapter'] =  await asyncio.to_thread(
        lambda: df['pembeli_chapter_admob'] + df['pembeli_chapter_adskoin'] + df['pembeli_chapter_koin'])
    df['persentase_pembaca_pembeli_chapter_koin'] = await asyncio.to_thread(
        lambda: df['pembeli_chapter_koin'] / df['total_pembeli_chapter'])
    df['persentase_pembaca_pembeli_chapter_adskoin'] = await asyncio.to_thread(
        lambda: df['pembeli_chapter_adskoin'] / df['total_pembeli_chapter'])
    df['persentase_pembaca_pembeli_chapter_admob'] = await asyncio.to_thread(
        lambda: df['pembeli_chapter_admob'] / df['total_pembeli_chapter'])
    df['persentase_pembaca_pembeli'] = await asyncio.to_thread(
        lambda: df['total_pembeli_chapter'] / df['pembaca_chapter'])

    await asyncio.to_thread(lambda: df.sort_values(by='date', ascending=True, inplace=True))

    if df.empty:
        df['date'] = pd.date_range(from_date, to_date).date
        df['pembaca_chapter'] = 0
        df['pembeli_chapter_koin'] = 0
        df['pembeli_chapter_adskoin'] = 0
        df['pembeli_chapter_admob'] = 0
        df['total_pembeli_chapter'] = 0
        df['persentase_pembaca_pembeli_chapter_koin'] = 0.0
        df['persentase_pembaca_pembeli_chapter_adskoin'] = 0.0
        df['persentase_pembaca_pembeli_chapter_admob']= 0.0
        df['persentase_pembaca_pembeli'] = 0.0

    if source == 'app':
        ads = 'Admob'
    else:
        ads = 'Adsense'
    
    # creating the chart
    trace0 = go.Bar(
        yaxis='y',
        x=df['date'], 
        y=df['pembeli_chapter_adskoin'], 
        name='Chapter Purchase With AdsKoin', 
        text=df['pembeli_chapter_adskoin'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='outside')
    trace1 = go.Bar(
        yaxis='y',
        x=df['date'], 
        y=df['pembeli_chapter_koin'], 
        name='Chapter Purchase With Koin', 
        text=df['pembeli_chapter_koin'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='inside')
    trace2 = go.Bar(
        yaxis='y',
        x=df['date'], 
        y=df['pembeli_chapter_admob'], 
        name=f'Chapter Purchase With {ads}', 
        text= df['pembeli_chapter_admob'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='inside')
    trace3 = go.Bar(
        yaxis='y',
        x=df['date'], 
        y=df['pembaca_chapter'], 
        name='Pembaca Chapter', 
        text= df['pembaca_chapter'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='inside')
    trace4 = go.Scatter(
        yaxis='y2',
        x=df['date'], 
        y=df['persentase_pembaca_pembeli_chapter_koin'], 
        name='% Chapter Purchase With Koin', 
        text=df['persentase_pembaca_pembeli_chapter_koin'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='top center', 
        hovertemplate='%{y:.2%}')
    trace5 = go.Scatter(
        yaxis='y2',
        x=df['date'], 
        y=df['persentase_pembaca_pembeli_chapter_admob'], 
        name=f'% Chapter Purchase With {ads}', 
        text=df['persentase_pembaca_pembeli_chapter_admob'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='top center', 
        hovertemplate='%{y:.2%}')
    trace6 = go.Scatter(
        yaxis='y2',
        x=df['date'], 
        y=df['persentase_pembaca_pembeli_chapter_adskoin'], 
        name='% Chapter Purchase With AdsKoin', 
        text=df['persentase_pembaca_pembeli_chapter_adskoin'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='top center', 
        hovertemplate='%{y:.2%}')
    trace7 = go.Scatter(
        yaxis='y2',
        x=df['date'], 
        y=df['persentase_pembaca_pembeli'], 
        name='% Chapter Read To Purchase', 
        text=df['persentase_pembaca_pembeli'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='top center', 
        hovertemplate='%{y:.2%}')

    layout = go.Layout(
        title='User Journey',
        yaxis1=dict(
            title='Pembeli Chapter'
        ),
        yaxis2=dict(
            overlaying='y',
            side='right',
            tickformat='.0%'
        ), barmode='group'
    )

    # Combine the traces and layout into a Figure object
    fig = go.Figure(data=[trace3, trace2, trace1, trace0,trace4, trace5, trace6, trace7], layout=layout)

    fig.update_layout(showlegend=True, barmode='stack')
    fig.update_xaxes(title='Date', dtick='D1')
    if df['date'].count() >= 31:
        fig.update_xaxes(title='Date', dtick='M1')

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart

