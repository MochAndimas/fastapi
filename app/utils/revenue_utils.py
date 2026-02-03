import logging
import json
import numpy as np
import pandas as pd
import asyncio
import plotly
import plotly.graph_objects as go
import re
from typing import Union, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import OperationalError
from sqlalchemy.future import select
from sqlalchemy import asc
from datetime import datetime, timedelta
from sqlalchemy import case as Case, func
from app.db.models.coin import GooddreamerTransaction, GooddreamerTransactionDetails, GooddreamerPaymentData
from app.db.models.acquisition import AdmobReportData, AdsenseReportData
from app.db.models.novel import GooddreamerUserChapterAdmob
from app.db.models.user import GooddreamerUserData
from app.db.models.data_source import  Sources, ModelHasSources
pd.options.mode.copy_on_write = True
pd.set_option('future.no_silent_downcasting', True)

# Set up logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RevenueData:
    """
    Represents a RevenueData object responsible for retrieving, processing, and manipulating revenue data
    from various sources such as Coin, Admob, Adsense, and Chapter Ads.

    This class leverages asynchronous SQLAlchemy sessions to query data from the database and perform 
    data processing tasks to generate DataFrames for each type of revenue source. It supports fetching 
    data on a daily or monthly basis within a specified date range.
    """

    def __init__(
            self, 
            session: AsyncSession, 
            sqlite_session: AsyncSession, 
            from_date: datetime.date,
            to_date: datetime.date,
            period: str ='daily'):
        """
        Initializes a RevenueData object with specified session, date range, and data aggregation period.

        Args:
            session (AsyncSession): SQLAlchemy asynchronous session for interacting with the main database.
            sqlite_session (AsyncSession): SQLAlchemy asynchronous session for interacting with the SQLite database.
            from_date (datetime.date): The start date of the data retrieval period.
            to_date (datetime.date): The end date of the data retrieval period.
            period (str, optional): Data aggregation period, either 'daily' or 'monthly'. Default is 'daily'.
        """
        self.session = session
        self.sqlite_session = sqlite_session
        self.from_date = from_date
        self.to_date = to_date
        self.period = period
        self.df_coin = pd.DataFrame()
        self.df_admob = pd.DataFrame()
        self.df_adsense = pd.DataFrame()
        self.df_chapter_ads = pd.DataFrame()

    @classmethod
    async def load_data(
        cls, 
        session: AsyncSession, 
        sqlite_session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        period: str = "daily"):
        """
        Creates an instance of RevenueData and asynchronously loads revenue data for the specified date range.

        Args:
            session (AsyncSession): SQLAlchemy asynchronous session for interacting with the main database.
            sqlite_session (AsyncSession): SQLAlchemy asynchronous session for interacting with the SQLite database.
            from_date (datetime.date): The start date of the data retrieval period.
            to_date (datetime.date): The end date of the data retrieval period.
            period (str, optional): Data aggregation period, either 'daily' or 'monthly'. Default is 'daily'.

        Returns:
            RevenueData: An instance of the class with data loaded into respective attributes.
        """
        instance = cls(session, sqlite_session, from_date, to_date, period)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self):
        """
        Asynchronously fetches revenue data from the database for each revenue type and stores it in class attributes.

        This method retrieves different types of revenue data, such as 'coin_data', 'admob_data', 'adsense_data', 
        and 'chapter_ads_data' within the specified date range, and stores the results as pandas DataFrames.
        """
        await self._read_db(from_date=self.from_date, to_date=self.to_date, data="coin_data")
        await self._read_db(from_date=self.from_date, to_date=self.to_date, data="admob_data")
        await self._read_db(from_date=self.from_date, to_date=self.to_date, data="adsense_data")
        await self._read_db(from_date=self.from_date, to_date=self.to_date, data="chapter_ads_data")

    async def _read_db(self, from_date: datetime.date, to_date: datetime.date, data: str = "coin_data"):
        """
        Asynchronously reads revenue data from the database and converts it into a pandas DataFrame.

        The method handles different types of revenue data based on the 'data' argument. The selected data 
        type defines the query to execute and the transformations applied to the retrieved data.

        Args:
            from_date (datetime.date): The start date of the data retrieval period.
            to_date (datetime.date): The end date of the data retrieval period.
            data (str): The type of revenue data to fetch, which could be one of:
                        - 'coin_data' for Coin revenue data
                        - 'admob_data' for AdMob revenue data
                        - 'adsense_data' for Adsense revenue data
                        - 'chapter_ads_data' for Chapter Ads revenue data

        Returns:
            None: The resulting data is stored in corresponding class attributes as pandas DataFrames.
        """
        delta = (to_date - from_date) + timedelta(days=1)
        BATCH_SIZE = 1000 if delta <= timedelta(days=7) else 15000

        if data == "coin_data":
            query = select(
                GooddreamerTransaction.id.label("id"),
                GooddreamerTransaction.user_id.label("user_id"),
                func.date(GooddreamerUserData.created_at).label("install_date"),
                func.date(GooddreamerUserData.registered_at).label("register_date"),
                GooddreamerUserData.email.label("email"),
                GooddreamerUserData.fullname.label("fullname"),
                func.date(GooddreamerTransaction.created_at).label("transaction_date"),
                GooddreamerTransaction.transaction_coin_value.label("coin_value"),
                (GooddreamerTransactionDetails.package_price - func.ifnull(GooddreamerTransactionDetails.discount_value,0)).label('revenue'),
                ((GooddreamerTransactionDetails.package_price + func.ifnull(GooddreamerTransactionDetails.package_fee, 0) - func.ifnull(GooddreamerTransactionDetails.discount_value,0))).label('amount'),
                func.ifnull(GooddreamerPaymentData.bank_code, '-').label('bank_code'),
                GooddreamerPaymentData.payment_gateway_name.label('payment_gateway'),
                Case(
                    (func.json_extract(GooddreamerPaymentData.meta, '$.payment_type') == 'cstore', func.json_extract(GooddreamerPaymentData.meta, '$.store')),
                    (GooddreamerPaymentData.payment_gateway_name == 'midtrans', func.json_extract(GooddreamerPaymentData.meta, '$.payment_type'))
                    , else_=GooddreamerPaymentData.payment_channel
                ).label('payment_channel'),
                Case((GooddreamerTransaction.transaction_status == 1, 'paid'), (GooddreamerTransaction.transaction_status == 2, 'expired'), else_='-').label('status'),
                GooddreamerPaymentData.paid_at.label('payment_date'),
                Sources.name.label('source')
            ).join(
                GooddreamerTransaction.model_has_sources
            ).join(
                ModelHasSources.sources
            ).join(
                GooddreamerTransaction.gooddreamer_user_data
            ).join(
                GooddreamerTransaction.gooddreamer_transaction_details
            ).join(
                GooddreamerTransaction.gooddreamer_payment_data
            ).filter(
                ModelHasSources.model_type == 'App\\Models\\Transaction',
                func.date(GooddreamerTransaction.created_at).between(from_date, to_date)
            ).execution_options(yield_per=BATCH_SIZE)

            # Stream the query results asynchronously
            results = await self.session.stream(query)

            rows = []
            async for result in results:
                rows.append(result._asdict())  # Convert each row to a dictionary
            
            self.df_coin = pd.DataFrame(rows)
        
        elif data == "admob_data":
            # Asynchronously read CSV file
            query = select(
                AdmobReportData.date.label("Date"),
                AdmobReportData.platform.label("Platform"),
                AdmobReportData.estimated_earnings.label("Estimated earnings"),
                AdmobReportData.impressions.label("Impressions"),
                AdmobReportData.observed_ecpm.label("Observed ECPM"),
                AdmobReportData.impression_ctr.label("Impression CTR"),
                AdmobReportData.clicks.label("Clicks"),
                AdmobReportData.ad_requests.label("Ad requests"),
                AdmobReportData.match_rate.label("Match rate"),
                AdmobReportData.match_requests.label("Matched requests")
            ).filter(
                AdmobReportData.date.between(from_date, to_date)
            ).execution_options(yield_per=BATCH_SIZE)

            # Stream the query results asynchronously
            results = await self.sqlite_session.stream(query)

            rows = []
            async for result in results:
                rows.append(result._asdict())  

            df = pd.DataFrame(rows)
            if df.empty:
                df = pd.DataFrame({
                    "Date": pd.date_range(to_date, to_date).date,
                    "Platform": ["-"],
                    "Estimated earnings": [0],
                    "Observed ECPM": [0],
                    "Impressions": [0],
                    "Clicks": [0],
                    "Ad requests": [0],
                    "Matched requests": [0],
                    "Impression CTR": [0],
                    "Match rate": [0],
                    "Show rate": [0]
                })

            df["Date"] = await asyncio.to_thread(pd.to_datetime, df["Date"])
            df["Date"] = await asyncio.to_thread(lambda: df["Date"].dt.date)
            await asyncio.to_thread(lambda: df.sort_values(by='Date', ascending=True, inplace=True))
            df["Estimated earnings"] = await asyncio.to_thread(lambda: df['Estimated earnings'] / 1000000)
            df["Observed ECPM"] = await asyncio.to_thread(lambda: df['Observed ECPM'] / 1000000)
            df['Show rate'] = await asyncio.to_thread(lambda: df['Impressions'] / df['Matched requests'])
            await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
            df['Matched requests'] = await asyncio.to_thread(lambda: df['Matched requests'].astype(int))
            self.df_admob = df.copy()
        
        elif data == "adsense_data":
            query = select(
                AdsenseReportData.date.label("Date"),
                AdsenseReportData.platform_type_name.label("Platform"),
                AdsenseReportData.ad_placement_name.label("Ad Placement Name"),
                AdsenseReportData.estimated_earnings.label("Estimated earnings"),
                AdsenseReportData.impressions.label("Impressions"),
                AdsenseReportData.clicks.label("Clicks"),
                AdsenseReportData.ad_requests.label("Ad requests"),
                AdsenseReportData.matched_ad_requests.label("Matched requests"),
                AdsenseReportData.impressions_rpm.label("Impression RPM"),
                AdsenseReportData.impressions_ctr.label("Impression CTR"),
            ).filter(
                AdsenseReportData.date.between(from_date, to_date)
            ).execution_options(yield_per=BATCH_SIZE)

            # Stream the query results asynchronously
            results = await self.sqlite_session.stream(query)

            rows = []
            async for result in results:
                rows.append(result._asdict())  
            
            df = pd.DataFrame(rows)
            if df.empty:
                df = pd.DataFrame({
                    "Date": pd.date_range(to_date, to_date).date,
                    "Ad Placement name": ["-"],
                    "Platform": ["-"],
                    "Estimated earnings": [0],
                    "Impression RPM": [0],
                    "Impressions": [0],
                    "Clicks": [0],
                    "Ad requests": [0],
                    "Matched requests": [0],
                    "Impression CTR": [0],
                    "Match rate": [0],
                    "Show rate": [0]
                })
            
            df['Date'] = await asyncio.to_thread(pd.to_datetime,df['Date'])
            df["Date"] = await asyncio.to_thread(lambda: df["Date"].dt.date)
            df["Match rate"] = await asyncio.to_thread(lambda: df["Matched requests"] / df["Ad requests"])
            df["Show rate"] = await asyncio.to_thread(lambda: df["Impressions"] / df["Matched requests"])
            self.df_adsense = df.copy()
        
        elif data == "chapter_ads_data":
            query = select(
                    func.date(GooddreamerUserChapterAdmob.created_at).label("tanggal"),
                    GooddreamerUserChapterAdmob.user_id.label("user_id"),
                    Sources.name.label("source"),
                    func.count(GooddreamerUserChapterAdmob.id).label("chapter_count")
                ).join(
                    GooddreamerUserChapterAdmob.model_has_sources
                ).join(
                    ModelHasSources.sources
                ).filter(
                    ModelHasSources.model_type == "App\\Models\\UserChapterAdmob",
                    func.date(GooddreamerUserChapterAdmob.created_at).between(from_date, to_date)
                ).group_by(
                    func.date(GooddreamerUserChapterAdmob.created_at),
                    GooddreamerUserChapterAdmob.user_id,
                    Sources.name
                ).execution_options(yield_per=BATCH_SIZE)

            # Stream the query results asynchronously
            results = await self.session.stream(query)

            rows = []
            async for result in results:
                rows.append(result._asdict())  # Convert each row to a dictionary
            
            df = pd.DataFrame(rows)
            if df.empty:
                df = pd.DataFrame({
                    "tanggal": pd.date_range(to_date,to_date).date,
                    "user_id": [0],
                    "source": ["-"],
                    "chapter_count": [0]
                })
            self.df_chapter_ads = df.copy()

    def _add_default_values(self, to_date: datetime.date):
        """
        Adds default values to the DataFrame if it is empty.
        """
        if self.df_coin.empty:
            self.df_coin = pd.DataFrame({
                'id': ['-'],
                'user_id': [0],
                "install_date": pd.date_range(to_date, to_date).date,
                "register_date": pd.date_range(to_date, to_date),
                "fullname": ["-"],
                "email": ["-"],
                "transaction_date": pd.date_range(to_date, to_date),
                "coin_value": [0],
                "revenue": [0],
                "amount": [0],
                "bank_code": [0],
                "payment_gateway": ["-"],
                "payment_channel": ["-"],
                "status": ["-"],
                "payment_date": pd.date_range(to_date, to_date).date,
                "source": ["-"]
            })

        if self.df_admob.empty:
            self.df_admob = pd.DataFrame({
                "Date": pd.date_range(to_date, to_date).date,
                "Platform": ["-"],
                "Estimated earnings": [0],
                "Observed ECPM": [0],
                "Impressions": [0],
                "Clicks": [0],
                "Ad requests": [0],
                "Matched requests": [0],
                "Impression CTR": [0],
                "Match rate": [0],
                "Show rate": [0]
            })

        if self.df_adsense.empty:
            self.df_adsense = pd.DataFrame({
                "Date": pd.date_range(to_date, to_date).date,
                "Ad Placement name": ["-"],
                "Platform": ["-"],
                "Estimated earnings": [0],
                "Impression RPM": [0],
                "Impressions": [0],
                "Clicks": [0],
                "Ad requests": [0],
                "Matched requests": [0],
                "Impression CTR": [0],
                "Match rate": [0],
                "Show rate": [0]
            })

        if self.df_chapter_ads.empty:
            self.df_chapter_ads = pd.DataFrame({
                "tanggal": pd.date_range(to_date,to_date).date,
                "user_id": [0],
                "source": ["-"],
                "chapter_count": [0]
            })

    def _process_date_column(self):
        """
        Processes date columns in the DataFrame based on the specified period.
        """
        if self.period == 'daily':
            # Convert date columns to date format
            self.df_coin['transaction_date'] = pd.to_datetime(self.df_coin['transaction_date']).dt.date
            self.df_coin['register_date'] = pd.to_datetime(self.df_coin['register_date']).dt.date
            self.df_coin['payment_date'] = pd.to_datetime(self.df_coin['payment_date']).dt.date
            self.df_coin['install_date'] = pd.to_datetime(self.df_coin['install_date']).dt.date
            self.df_admob["Date"] = pd.to_datetime(self.df_admob["Date"]).dt.date
            self.df_adsense["Date"] = pd.to_datetime(self.df_adsense["Date"]).dt.date
            self.df_chapter_ads["tanggal"] = pd.to_datetime(self.df_chapter_ads["tanggal"]).dt.date
        else:
            # Convert date columns to monthly period format
            self.df_coin['transaction_date'] = pd.to_datetime(self.df_coin['transaction_date']).dt.to_period('M')
            self.df_coin['transaction_date'] = self.df_coin['transaction_date'].dt.strftime('%Y-%m-01')
            self.df_coin['transaction_date'] = pd.to_datetime(self.df_coin['transaction_date']).dt.date

            self.df_admob["Date"] = pd.to_datetime(self.df_admob["Date"]).dt.to_period("M")
            self.df_admob["Date"] = self.df_admob["Date"].dt.strftime("%Y-%m-01")
            self.df_admob["Date"] = pd.to_datetime(self.df_admob["Date"]).dt.date

            self.df_adsense["Date"] = pd.to_datetime(self.df_adsense["Date"]).dt.to_period("M")
            self.df_adsense["Date"] = self.df_adsense["Date"].dt.strftime("%Y-%m-01")
            self.df_adsense["Date"] = pd.to_datetime(self.df_adsense["Date"]).dt.date

            self.df_chapter_ads["tanggal"] = pd.to_datetime(self.df_chapter_ads["tanggal"]).dt.to_period("M")
            self.df_chapter_ads["tanggal"] = self.df_chapter_ads["tanggal"].dt.strftime("%Y-%m-01")
            self.df_chapter_ads["tanggal"] = pd.to_datetime(self.df_chapter_ads["tanggal"]).dt.date

    async def revenue_data(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            metrics: list = [],
            source: str = 'app') -> Dict[str, float]:
        """
        Retrieve data related to revenue within a specified date range and source.

        Args:
            from_date (datetime.date): Start date of the date range (inclusive).
            to_date (datetime.date): End date of the date range (inclusive).
            metrics (list, optional): Filtering metrics to fetch
                - 'coin_count'
                - 'coin_unique'
                - 'count_coin_expired'
                - 'count_coin_success'
                - 'count_coin_total'
                - 'revenue'
                - 'gross_revenue'
                - 'arpu'
                - 'arpt'
                - 'first_purchase'
                - 'return_purchase'
                - 'unique_user'
                - 'Estimated'
                - 'Observed'
                - 'Impressions'
                - 'Estimated_impressions'
                - 'revenue_per_user'
                - 'impression_per_user'
                - 'overall_revenue'
            source (str, optional): Source of the transactions ('app', 'web', 'all'. default is 'app').

        Returns:
            dict: Dictionary containing the requested data based on the specified parameters.
        """
        # Add default values and process the date column asynchronously
        self._add_default_values(to_date=to_date)
        self._process_date_column()

        # Intialize dataframe & filter data asynchronously based on source and date range
        df_read = await asyncio.to_thread(
            lambda: self.df_coin[
                (self.df_coin["transaction_date"] >= from_date) &
                (self.df_coin["transaction_date"] <= to_date) &
                (self.df_coin['source'] == source)]) if source in ["app", "web"] else \
                    await asyncio.to_thread(
                            lambda: self.df_coin[
                                (self.df_coin["transaction_date"] >= from_date) &
                                (self.df_coin["transaction_date"] <= to_date)])
        
        df_unique_user = await asyncio.to_thread(
            lambda: self.df_chapter_ads[
                (self.df_chapter_ads["tanggal"] >= from_date) &
                (self.df_chapter_ads["tanggal"] <= to_date) &
                (self.df_chapter_ads["source"] == source)
            ]) if source in ["app", "web"] else \
                await asyncio.to_thread(
                    lambda: self.df_chapter_ads[
                        (self.df_chapter_ads["tanggal"] >= from_date) &
                        (self.df_chapter_ads["tanggal"] <= to_date)])
        
        unique_user = await asyncio.to_thread(
            lambda: df_unique_user["user_id"].nunique())
        
        df_admob = await asyncio.to_thread(
            lambda: self.df_admob[
                (self.df_admob["Date"] >= from_date) &
                (self.df_admob["Date"] <= to_date)
            ]
        )
        df_adsense = await asyncio.to_thread(
            lambda: self.df_adsense[
                (self.df_adsense["Date"] >= from_date) &
                (self.df_adsense["Date"] <= to_date)
            ]
        )
        df_ads = df_admob.copy() if source == "app" else df_adsense.copy()
        
        # Pre proccess data asynchronously
        df_paid = await asyncio.to_thread(
            lambda: df_read[df_read['status'] == 'paid'])
        df_paid["transaction_date"] = await asyncio.to_thread(pd.to_datetime, df_paid["transaction_date"])
        df_paid["day"] = await asyncio.to_thread(
            lambda: df_paid['transaction_date'].dt.day_name())
        df_expired = await asyncio.to_thread(
            lambda: df_read[df_read['status'] == 'expired'])
        df_user_purchase = await asyncio.to_thread(
            lambda: df_paid.groupby(['user_id'])['id'].count().reset_index())
        
        # Calculate various metrics asynchronously using pandas operations
        arpu = 0 if df_paid.empty else await asyncio.to_thread(lambda: int(df_paid['amount'].sum() / df_paid['user_id'].nunique()))
        arpt = 0 if df_paid.empty else await asyncio.to_thread(lambda: int(df_paid['amount'].mean()))
        observed_ecpm = await asyncio.to_thread(lambda: np.around(df_ads['Observed ECPM'].sum(), 2)) if source == "app" else 0
        estimated_impressions = await asyncio.to_thread(lambda: np.around(df_ads['Estimated earnings'].sum() / df_ads['Impressions'].sum(), 2)) if df_ads['Impressions'].sum() != 0 else 0
        revenue_per_user = await asyncio.to_thread(lambda: np.around(df_ads['Estimated earnings'].sum() / unique_user, 2)) if unique_user != 0 else 0
        impression_per_user = await asyncio.to_thread(lambda: np.around(df_ads["Impressions"].sum() / unique_user)) if unique_user != 0 else 0
        overall_revenue = await asyncio.to_thread(lambda: int(df_paid['amount'].sum()) + int(df_ads['Estimated earnings'].sum())) \
            if source in ["app", "web"] else \
                await asyncio.to_thread(lambda: int(df_paid['amount'].sum()) + int(df_admob['Estimated earnings'].sum()) + int(df_adsense['Estimated earnings'].sum()))
        
        metrics_data = await asyncio.gather(
            asyncio.to_thread(lambda: df_paid['user_id'].count()), # coin_count
            asyncio.to_thread(lambda: df_paid['user_id'].nunique()), # coin_unique
            asyncio.to_thread(lambda: df_expired['id'].count()), # count_coin_expired
            asyncio.to_thread(lambda: df_paid['id'].count()), # count_coin_success
            asyncio.to_thread(lambda: df_expired['id'].count() + df_paid['id'].count()), # count_coin_total
            asyncio.to_thread(lambda: df_paid['revenue'].sum()), # revenue
            asyncio.to_thread(lambda: df_paid['amount'].sum()), # gross_revenue
            asyncio.to_thread(lambda: df_user_purchase[df_user_purchase['id'] == 1]['id'].count()), # first_purchase
            asyncio.to_thread(lambda: df_user_purchase[df_user_purchase['id'] > 1]['id'].count()), # return_purchase
            asyncio.to_thread(lambda: np.around(df_ads['Estimated earnings'].sum(), 2)), # Estimated earnings
            asyncio.to_thread(lambda: np.around(df_ads['Impressions'].sum(), 2)) # Impressions
        )
        
        container = {
            'coin_count': int(metrics_data[0]),
            'coin_unique': int(metrics_data[1]),
            'count_coin_expired': int(metrics_data[2]),
            'count_coin_success':int( metrics_data[3]),
            'count_coin_total': int(metrics_data[4]),
            'revenue': int(metrics_data[5]),
            'gross_revenue': int(metrics_data[6]),
            'arpu': int(arpu),
            'arpt': int(arpt),
            'first_purchase': int(metrics_data[7]),
            'return_purchase': int(metrics_data[8]),
            'unique_user': int(unique_user),
            'Estimated earnings': int(metrics_data[9]),
            'Observed ECPM': int(observed_ecpm),
            'Impressions': int(metrics_data[10]),
            'Estimated_impressions': int(estimated_impressions),
            'revenue_per_user': int(revenue_per_user),
            'impression_per_user': int(impression_per_user),
            'overall_revenue': int(overall_revenue),
        }

        if metrics:
            container = {k: container[k] for k in metrics}
        
        return container

    async def revenue_dataframe(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date,
            metrics: list = [],
            source: str = 'app') -> Union[pd.DataFrame, str, float]:
        """
        Retrieve data related to revenue within a specified date range.

        Args:

            from_date (str): Start date of the date range (inclusive), format: 'YYYY-MM-DD'.
            to_date (str): End date of the date range (inclusive), format: 'YYYY-MM-DD'.
            metrics (list, optional): Filtering metrics to fetch
                - 'dataframe'
                - 'df_unique_count'
                - 'df_ads'
                - 'dataframe'
                - 'cost_revenue'
                - 'revenue_all_chart'
                - 'total_transaksi_coin_chart'
                - 'category_coin_chart'
                - 'revenue_days'
                - 'coin_days_chart'
                - 'old_new_df'
                - 'payment_channel'
            source (str, optional): Source of the transactions ('app', 'web', 'all'. default is 'app').

        Returns:
            pandas.DataFrame: DataFrame containing the requested data based on the specified parameters.
        """
        # Add default values and process the date column asynchronously
        self._add_default_values(to_date=to_date)
        self._process_date_column()

        # Initiate the data & filtering data asynchronously
        self.df_coin["transaction_date"] = await asyncio.to_thread(pd.to_datetime, self.df_coin["transaction_date"])
        self.df_coin["transaction_date"]  = await asyncio.to_thread(lambda: self.df_coin["transaction_date"].dt.date)
        df_read = await asyncio.to_thread(
            lambda: self.df_coin[
                (self.df_coin["transaction_date"] >= from_date) &
                (self.df_coin["transaction_date"] <= to_date) &
                (self.df_coin['source'] == source)]) if source in ["app", "web"] else \
                    await asyncio.to_thread(
                        lambda: self.df_coin[
                            (self.df_coin["transaction_date"] >= from_date) &
                            (self.df_coin["transaction_date"] <= to_date)])
        
        df_chapter_unique_count = await asyncio.to_thread(
            lambda: self.df_chapter_ads[
                (self.df_chapter_ads["tanggal"] >= from_date) &
                (self.df_chapter_ads["tanggal"] <= to_date) &
                (self.df_chapter_ads["source"] == source)
            ]) if source in ["app", "web"] else \
                await asyncio.to_thread(
                    lambda: self.df_chapter_ads[
                        (self.df_chapter_ads["tanggal"] >= from_date) &
                        (self.df_chapter_ads["tanggal"] <= to_date)])
        
        df_chapter_unique_count = await asyncio.to_thread(
            lambda: df_chapter_unique_count.groupby("tanggal").agg(
                chapter_admob_unique=("user_id", "nunique"),
                chapter_admob_count=("chapter_count", "sum")
            ).reset_index())

        df_paid = await asyncio.to_thread(lambda: df_read[df_read['status'] == 'paid'])
        df_paid["transaction_date"] = await asyncio.to_thread(pd.to_datetime, df_paid["transaction_date"])
        df_paid['day'] = await asyncio.to_thread(lambda: df_paid['transaction_date'].dt.day_name())
        df_expired = await asyncio.to_thread(lambda: df_read[df_read['status'] == 'expired'])
        df_ads = self.df_admob if source == "app" else self.df_adsense

        # Revenue Dataframe
        df_admob = await asyncio.to_thread(lambda: self.df_admob.loc[:, ["Date", "Estimated earnings"]])
        df_admob = await asyncio.to_thread( 
            lambda: df_admob.groupby(["Date"]).agg(
                admob_revenue=("Estimated earnings", "sum")
            ).reset_index())
        df_adsense = await asyncio.to_thread(lambda: self.df_adsense.loc[:, ["Date", "Estimated earnings"]])
        df_adsense = await asyncio.to_thread(
            lambda: df_adsense.groupby(["Date"]).agg(
                adsense_revenue=("Estimated earnings", "sum")
            ).reset_index())
        
        df_admob["Date"] = await asyncio.to_thread(pd.to_datetime, df_admob["Date"])
        df_admob["Date"] = await asyncio.to_thread(lambda: df_admob["Date"].dt.date)
        df_adsense["Date"] = await asyncio.to_thread(pd.to_datetime, df_adsense["Date"])
        df_adsense["Date"] = await asyncio.to_thread(lambda: df_adsense["Date"].dt.date)
        df_ads_revenue = await asyncio.to_thread(lambda: pd.merge(df_admob, df_adsense, how="outer", on="Date"))

        await asyncio.to_thread(lambda: df_ads_revenue.fillna(0, inplace=True))
        await asyncio.to_thread(lambda: df_ads_revenue.infer_objects(copy=True))

        df_ads_revenue["total_ads_revenue"] = await asyncio.to_thread(lambda: df_ads_revenue["admob_revenue"] + df_ads_revenue["adsense_revenue"])
        df_ads_revenue = await asyncio.to_thread(lambda: df_ads_revenue.loc[:, ["Date", "total_ads_revenue"]]) \
            if source not in ["app", "web"] else \
                await asyncio.to_thread(lambda: df_ads.groupby(["Date"]).agg(total_ads_revenue=("Estimated earnings", "sum")).reset_index())
        
        df_coin_revenue = await asyncio.to_thread(lambda: df_paid.groupby(["transaction_date"])["amount"].sum().reset_index())
        await asyncio.to_thread(lambda: df_coin_revenue.rename(columns={"transaction_date": "Date"}, inplace=True))
        
        df_coin_revenue["Date"] = await asyncio.to_thread(pd.to_datetime, df_coin_revenue["Date"])
        df_coin_revenue["Date"] = await asyncio.to_thread(lambda: df_coin_revenue["Date"].dt.date)
        df_ads_revenue["Date"] = await asyncio.to_thread(pd.to_datetime, df_ads_revenue["Date"])
        df_ads_revenue["Date"] = await asyncio.to_thread(lambda: df_ads_revenue["Date"].dt.date)

        df_revenue = await asyncio.to_thread(lambda: pd.merge(df_coin_revenue, df_ads_revenue, how="outer", on="Date"))
        await asyncio.to_thread(lambda: df_revenue.fillna(0, inplace=True))
        await asyncio.to_thread(lambda: df_revenue.infer_objects(copy=True))
        df_revenue["amount"] = await asyncio.to_thread(lambda: df_revenue["amount"].astype(int))
        df_revenue["total_ads_revenue"] = await asyncio.to_thread(lambda: df_revenue["total_ads_revenue"].astype(int))
        df_revenue["total_revenue"] = await asyncio.to_thread(lambda: df_revenue["amount"] + df_revenue["total_ads_revenue"])
        
        # dataframe coin transaction
        df_expired_group =await asyncio.to_thread(lambda:  df_expired.groupby(['transaction_date'])['status'].count().reset_index())
        await asyncio.to_thread(lambda: df_expired_group.rename(columns={'status':'coin_expired'}, inplace=True))
        df_paid_group = await asyncio.to_thread(lambda: df_paid.groupby(['transaction_date'])['status'].count().reset_index())
        await asyncio.to_thread(lambda: df_paid_group.rename(columns={'status':'coin_success'}, inplace=True))
        df_expired_group["transaction_date"] = await asyncio.to_thread(pd.to_datetime, df_expired_group["transaction_date"])
        df_expired_group["transaction_date"] = await asyncio.to_thread(lambda: df_expired_group["transaction_date"].dt.date)
        df_paid_group["transaction_date"] = await asyncio.to_thread(pd.to_datetime, df_paid_group["transaction_date"])
        df_paid_group["transaction_date"] = await asyncio.to_thread(lambda: df_paid_group["transaction_date"].dt.date)
        df_coin_transaction = await asyncio.to_thread(lambda: pd.merge(df_expired_group, df_paid_group, on='transaction_date', how='outer'))
        await asyncio.to_thread(lambda: df_coin_transaction.fillna(0, inplace=True))
        df_coin_transaction['coin_expired'] = await asyncio.to_thread(lambda: df_coin_transaction['coin_expired'].astype(int))
        df_coin_transaction['coin_success'] = await asyncio.to_thread(lambda: df_coin_transaction['coin_success'].astype(int))
        df_coin_transaction['total_transaction'] = await asyncio.to_thread(lambda: df_coin_transaction['coin_expired'] + df_coin_transaction['coin_success'])

        # dataframe payment channel
        df_channel = await asyncio.to_thread(lambda: df_paid.loc[:, ['id', 'payment_channel', 'bank_code']])
        df_channel['payment_channel'] = await asyncio.to_thread(lambda: df_channel['payment_channel'].str.replace('"', ''))
        df_channel['payment_channel'] = await asyncio.to_thread( 
            lambda: df_channel.apply(lambda row: row['bank_code'] if row['payment_channel'] == 'bank_transfer' else row['payment_channel'], axis=1))
        
        container = {
            'dataframe': df_read.copy(),
            'df_unique_count': df_chapter_unique_count.to_dict(),
            'df_ads': df_ads.to_dict(),
            'dataframe': df_read.to_dict(),
            'cost_revenue': df_paid.groupby(['transaction_date']).agg(
                total_rev_koin=('amount', 'sum')
            ).rename_axis(index={'transaction_date':'date_start'}).reset_index().to_dict(),
            'revenue_all_chart': df_revenue.to_dict(),
            'total_transaksi_coin_chart': df_coin_transaction.to_dict(),
            'category_coin_chart': df_paid.groupby(['coin_value']).agg(
                total_pembelian=('id', 'count')
            ).reset_index().to_dict(),
            'revenue_days': df_paid.groupby(['transaction_date']).agg(
                total_revenue=('amount', 'sum')
            ).rename_axis(index={'transaction_date':'date'}).reset_index().to_dict(),
            'coin_days_chart': df_paid.groupby(['day']).agg(
                total_pembelian=('id', 'count')
            ).reset_index().to_dict(),
            'old_new_df': df_paid.loc[:, ['transaction_date', 'user_id', 'email', 'fullname', 'install_date']].rename(
                columns={'transaction_date':'buy_date', 'install_date':'created_at'}
            ).to_dict(),
            'payment_channel': df_channel.groupby(['payment_channel']).agg(
                total_transaksi=('id', 'count')
            ).reset_index().sort_values(by='total_transaksi', ascending=False).to_dict(),
        }

        if metrics:
            container = {k: container[k] for k in metrics}
        
        return container

    async def daily_growth(
            self,
            from_date: datetime.date,
            to_date: datetime.date,
            source: str,
            metrics: list = [],) -> Dict[str, float]:
        """
        Calculates the daily growth percentage in revenue for a given date range and data source compared to the previous week.

        This function assumes the data for both the current and previous week have the same keys. 

        Args:
            from_date (datetime.date): The start date for the current week (inclusive).
            to_date (datetime.date): The end date for the current week (inclusive).
            metrics (list, optional): Filtering metrics to fetch
                - 'coin_count'
                - 'coin_unique'
                - 'count_coin_expired'
                - 'count_coin_success'
                - 'count_coin_total'
                - 'revenue'
                - 'gross_revenue'
                - 'arpu'
                - 'arpt'
                - 'first_purchase'
                - 'return_purchase'
                - 'unique_user'
                - 'Estimated'
                - 'Observed'
                - 'Impressions'
                - 'Estimated_impressions'
                - 'revenue_per_user'
                - 'impression_per_user'
                - 'overall_revenue'
            source (str): The data source to query for revenue information ('app', 'web', or 'all').

        Returns:
            Dict[str, float]: A dictionary where keys are the same as the data source keys and values represent the daily growth percentage (rounded to two decimal places) for each key.

        Raises:
            ValueError: If the data from the current and previous week have different keys.
        """
        # Calculate date range for the previous week
        delta = (to_date - from_date) + timedelta(days=1)
        fromdate_lastweek = from_date - delta
        todate_lastweek = to_date - delta
        
        # Fetch data for the current and previous week
        current_data = await self.revenue_data(from_date=from_date, to_date=to_date, source=source, metrics=metrics)
        last_week_data = await self.revenue_data(from_date=fromdate_lastweek, to_date=todate_lastweek, source=source, metrics=metrics)
        
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


async def returning_first_purchase(
    session: AsyncSession,
    data: str,
    from_date: datetime.date,
    to_date: datetime.date,
    source: str,
    date_format: str = '%Y-%m-%d'
) -> Union[pd.DataFrame, int ,str]:
    """
    Asynchronously calculate the number of first-time and returning purchases within a specified date range.

    Parameters:
    - session (AsyncSession): The SQLAlchemy asynchronous database session.
    - from_date (datetime.date): The start date for the range (inclusive) in 'YYYY-MM-DD' format.
    - to_date (datetime.date): The end date for the range (inclusive) in 'YYYY-MM-DD' format.
    - source (str): The source name to filter transactions.
    - date_format (str): The format to use for the 'period' column ('%Y-%m-%d' or '%Y-%m-01').

    Returns:
    - Union[pd.DataFrame, int, str]: The requested data as a DataFrame or integer sum.
    """
    try:
        # Subquery for identifying user purchase types
        coin_purchaser_subquery = select(
                GooddreamerTransaction.user_id.label('user_id'),
                Case(
                    (func.count(GooddreamerTransaction.id) == 1, 'first_purchase'),
                    (func.count(GooddreamerTransaction.id) > 1, 'returning_purchase'),
                    else_=None
                ).label('user_types')
            ).join(
                GooddreamerTransaction.model_has_sources
            ).join(
                ModelHasSources.sources
            ).filter(
                ModelHasSources.model_type == 'App\\Models\\Transaction',
                Sources.name == source,
                GooddreamerTransaction.transaction_status == 1
            ).group_by(
                GooddreamerTransaction.user_id
            ).subquery()

        # Subquery for transactions within the date range
        transaction_subquery = select(
                func.date(GooddreamerTransaction.created_at).label('date'),
                GooddreamerTransaction.user_id.label('user_id'),
                coin_purchaser_subquery.c.user_types.label('user_types')
            ).join(
                GooddreamerTransaction.model_has_sources
            ).join(
                ModelHasSources.sources
            ).join(
                coin_purchaser_subquery, GooddreamerTransaction.user_id == coin_purchaser_subquery.c.user_id
            ).filter(
                ModelHasSources.model_type == 'App\\Models\\Transaction',
                Sources.name == source,
                func.date(GooddreamerTransaction.created_at).between(from_date, to_date),
                GooddreamerTransaction.transaction_status == 1
            ).subquery()

        # Subqueries for first and returning purchases
        first_purchase_subquery = select(
                func.date_format(transaction_subquery.c.date, date_format).label('period'),
                func.count(transaction_subquery.c.user_id.distinct()).label('first_purchase')
            ).filter(
                transaction_subquery.c.user_types == 'first_purchase'
            ).group_by(
                'period'
            ).subquery()

        returning_purchase_subquery = select(
                func.date_format(transaction_subquery.c.date, date_format).label('period'),
                func.count(transaction_subquery.c.user_id.distinct()).label('returning_purchase')
            ).filter(
                transaction_subquery.c.user_types == 'returning_purchase'
            ).group_by(
                'period'
            ).subquery()
        

        # Main query combining first and returning purchases
        main_query = select(
                first_purchase_subquery.c.period.label('period'),
                first_purchase_subquery.c.first_purchase.label('first_purchase'),
                returning_purchase_subquery.c.returning_purchase.label('returning_purchase')
            ).outerjoin(
                returning_purchase_subquery, first_purchase_subquery.c.period == returning_purchase_subquery.c.period
            ).distinct().union_all(
                select(
                    returning_purchase_subquery.c.period.label('period'),
                    first_purchase_subquery.c.first_purchase.label('first_purchase'),
                    returning_purchase_subquery.c.returning_purchase.label('returning_purchase')
                ).outerjoin(
                    first_purchase_subquery, returning_purchase_subquery.c.period == first_purchase_subquery.c.period
                ).distinct()
            ).order_by(asc('period'))

        # Execute the main query asynchronously
        result = await session.execute(main_query)
        rows = result.all()
        df = pd.DataFrame(rows, columns=['period', 'first_purchase', 'returning_purchase']).drop_duplicates()
        
        if df.empty:
            # Handle the case where no data is found
            default_value = {
                'period': pd.date_range(to_date, to_date).date,
                'first_purchase': 0,
                'returning_purchase': 0
            }
            df = pd.DataFrame(default_value, index=[0])

        # Prepare the result
        first_purchase_result = await session.scalar(
            select(func.count(transaction_subquery.c.user_id.distinct()))
            .filter(transaction_subquery.c.user_types == 'first_purchase')
        )

        returning_purchase_result = await session.scalar(
            select(func.count(transaction_subquery.c.user_id.distinct()))
            .filter(transaction_subquery.c.user_types == 'returning_purchase')
        )
        
        result_dict = {
            'first_purchase': first_purchase_result,
            'returning_purchase': returning_purchase_result
        }

        return result_dict if data != "df" else df.copy()
    except Exception as e:
        return f"An error occurred: {e}"


async def dg_returning_first_purchase(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        source: str, 
        date_format: str = '%Y-%m-%d') -> Dict[str, float]:
    """
    Calculates the daily growth rate of a specified returning_first_purchase metric over two periods.

    Args:
        session (AsyncSession): The SQLAlchemy asynchronous session.
        from_date (datetime.date, optional): Start date of the current period.
        to_date (datetime.date, optional): End date of the current period.
        source (str): The source name to filter transactions.
        date_format (str): The format to use for the 'period' column ('%Y-%m-%d' or '%Y-%m-01').

    Returns:
        str: The formatted daily growth rate as a percentage (e.g., '10.50%'), or 'N/A' if the previous period's value is zero.
    """

    try:
        # Calculate dates for the previous period
        delta = (to_date - from_date)+timedelta(1)
        fromdate_lastweek = from_date - delta
        todate_lastweek = to_date - delta

        # Fetch returning_first_purchase data
        current_data, last_week_data = await asyncio.gather(
            returning_first_purchase(session=session, data="value", from_date=from_date, to_date=to_date, source=source, date_format=date_format),
            returning_first_purchase(session=session, data="value", from_date=fromdate_lastweek, to_date=todate_lastweek, source=source, date_format=date_format)
        )

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

    except Exception as e:
        return f"Error : {e}"


async def total_transaksi_coin(
        to_date: datetime.date, 
        revenue_data: Dict,
        period: str = "daily") -> str:
    """
    Generate a chart showing total coin transactions.

    Args:
        to_date (str, optional): End date of the period (inclusive), format: 'YYYY-MM-DD'.
        revenue_data (Dict): Dict to retrieve revenue data.
        period (str): The period od data to fetch ('daily' or 'monthly').

    Returns:
        str: JSON representation of the Plotly chart.
    """
    # Retrieve total transaction data for coin chart
    data = revenue_data['total_transaksi_coin_chart']
    df = pd.DataFrame(data)
    await asyncio.to_thread(
        lambda: df.sort_values(by='transaction_date', ascending=True, inplace=True))
    
    # If DataFrame is empty, fill with zero values for the given period
    if df.empty:
        df['transaction_date'] = pd.date_range(to_date, to_date).date
        df['coin_expired'] = 0
        df['coin_success'] = 0
        df['total_transaction'] = 0

    # Create a Plotly bar chart
    fig = go.Figure(data=[
        go.Bar(name='Expired Coin Transaction', x=df['transaction_date'], y=df['coin_expired'],
               marker=dict(color='red'), text=df['coin_expired'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside'),
        go.Bar(name='Success Coin Transaction', x=df['transaction_date'], y=df['coin_success'],
               marker=dict(color='green'), text=df['coin_success'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside')]
    )

    # Update layout of the chart
    fig.update_xaxes(autorange=True, title='Date', dtick='D1' if period == "daily" else "M1")
    if df['transaction_date'].count() >= 31:
        fig.update_xaxes(autorange=True, title='Date', dtick='M1')
    fig.update_yaxes(title='Coin Transaction')
    fig.add_traces(go.Scatter(x=df['transaction_date'], y=df['total_transaction'],
                              line=dict(color='yellow'), name='Total Coin Transaction'))
    fig.update_layout(title='Coin Transaction', barmode='group')
    fig.update_layout(
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ))

    # Convert chart to JSON
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def transaksi_koin(
        revenue_data: Dict, 
        period: str ='all_time') -> pd.DataFrame:
    """
    Get transaction coin data.

    Args:
        revenue_data (Dict): Dict to retrieve revenue data.
        period (str): Period for grouping transactions ('all_time', 'monthly').

    Returns:
        pandas.DataFrame: DataFrame containing transaction coin data.
    """
    # Retrieve transaction data
    data = revenue_data['total_transaksi_coin_chart']
    df_full_merged = pd.DataFrame(data)
    await asyncio.to_thread(
        lambda: df_full_merged.rename(columns={
            'transaction_date': 'date',
            'coin_expired': 'transaksi_gagal',
            'coin_success': 'transaksi_sukses',
            'total_transaction': 'total_transaksi'
        }, inplace=True))

    if period == 'all_time':
        # Group data by month
        df_full_merged["date"] = await asyncio.to_thread(pd.to_datetime, df_full_merged["date"], format="%Y-%m-01")
        df_full_merged['date'] = await asyncio.to_thread(lambda: df_full_merged['date'].dt.date)
        df_group = await asyncio.to_thread(lambda: df_full_merged.groupby(['date'])['transaksi_sukses'].sum().reset_index())
        df_group_1 = await asyncio.to_thread(lambda: df_full_merged.groupby(['date'])['transaksi_gagal'].sum().reset_index())
        df_group_2 = await asyncio.to_thread(lambda: df_full_merged.groupby(['date'])['total_transaksi'].sum().reset_index())
        df_merge = await asyncio.to_thread(lambda: pd.merge(df_group, df_group_1, how='outer', on='date'))
        df_full_merged = await asyncio.to_thread(lambda: pd.merge(df_group_2, df_merge, how='outer', on='date'))
        df_full_merged['date'] = await asyncio.to_thread(lambda: df_full_merged['date'].astype(str))
        df_full_merged['date'] = await asyncio.to_thread(pd.to_datetime, df_full_merged['date'])
        df_full_merged["date"] = await asyncio.to_thread(lambda: df_full_merged["date"].dt.date)

    # Calculate success rate and expired rate
    df_full_merged['success_rate'] = await asyncio.to_thread(lambda: df_full_merged['transaksi_sukses'] / df_full_merged['total_transaksi'])
    df_full_merged['expired_rate'] = await asyncio.to_thread(lambda: df_full_merged['transaksi_gagal'] / df_full_merged['total_transaksi'])

    return df_full_merged


async def persentase_koin_gagal_sukses(
        revenue_data: Dict, 
        period: str = 'all_time') -> str:
    """
    Generate a line chart showing the percentage of failed and successful coin transactions over time.

    Args:
        revenue_data (Dict): Dict to retrieve revenue data.
        period (str): Period for grouping transactions ('all_time', 'daily').

    Returns:
        str: JSON representation of the Plotly line chart.
    """
    # Retrieve transaction data
    df = await transaksi_koin(revenue_data=revenue_data, period=period)

    # If there are more than 30 data points, use 'all_time' period for better visualization
    if df['date'].count() > 30:
        df = await transaksi_koin(revenue_data=revenue_data, period='all_time')
    else:
        df = await transaksi_koin(revenue_data=revenue_data, period=period)

    await asyncio.to_thread(lambda: df.sort_values(by='date', ascending=True, inplace=True))
    
    # Create traces for success rate and expired rate
    trace1 = go.Scatter(
        x=df['date'],
        y=df['success_rate'],
        name='Success Rate',
        yaxis='y',
        mode='lines+markers',
        line=dict(color='blue'),
    )

    trace2 = go.Scatter(
        x=df['date'],
        y=df['expired_rate'],
        name='Expired Rate',
        yaxis='y',
        mode='lines+markers',
        line=dict(color='red'),
    )

    # Define layout with a secondary y-axis
    layout = go.Layout(
        title='Expired & Success Coin Transaction Percentage',
        yaxis=dict(
            title='Success Coin Transaction',
            tickformat='.0%'  # Format y-axis as percentage
        ),
        yaxis2=dict(
            title='Expired Coin Transaction',
            overlaying='y',
            side='right',
            tickformat='.0%'  # Format y-axis as percentage
        )
    )

    # Combine traces and layout into a Figure object
    fig = go.Figure(data=[trace1, trace2], layout=layout)
    fig.update_layout(
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ))
    # Update x-axis tick labels based on the period
    if period == 'all_time':
        fig.update_xaxes(title='Date', dtick='M1')
    else:
        if df['date'].count() > 7:
            fig.update_xaxes(title='Date', dtick='M1')
        else:
            fig.update_xaxes(title='Date', dtick='D1')

    
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def category_coin(revenue_data: Dict) -> str:
    """
    Generate a chart showing coin transactions by category.

    Args:
        revenue_data (Dict): Dict to retrieve revenue data.

    Returns:
        str: JSON representation of the Plotly chart.
    """
    # Retrieve category-wise transaction data for coin chart
    data = revenue_data['category_coin_chart']
    category_df = pd.DataFrame(data)

    # If DataFrame is empty, fill with zero values for the given period
    if category_df.empty:
        category_df['coin_value'] = 0
        category_df['total_pembelian'] = 0
        
    # Sort values by coin_value
    await asyncio.to_thread(lambda: category_df.sort_values(by='coin_value', ascending=True, inplace=True))
    category_df['coin_value'] = await asyncio.to_thread(lambda: category_df['coin_value'].astype(str))

    # Create a Plotly horizontal bar chart
    fig = go.Figure(
        go.Bar(y=category_df.coin_value,
               x=category_df.total_pembelian, orientation='h',
               text=category_df['total_pembelian'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside',
               marker=dict(showscale=True, colorscale='bluered_r', color=category_df.total_pembelian))
    )

    # Update layout of the chart
    fig.update_layout(title='Coin Transaction Per Value')
    fig.update_xaxes(title='Total Transaction')
    fig.update_yaxes(title='Coin Value')

    # Convert chart to JSON
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def revenue_days(
        from_date: datetime.date, 
        to_date: datetime.date, 
        revenue_data: Dict,
        chart_types: str = "line") -> str:
    """
    Generate a chart showing total revenue per day.

    Args:
        from_date (str, optional): Start date of the period (inclusive), format: 'YYYY-MM-DD'.
        to_date (str, optional): End date of the period (inclusive), format: 'YYYY-MM-DD'.
        revenue_data (Dict): Dict to retrieve revenue data.
        chart_types (str): The chart types to return ('line' or 'bar')

    Returns:
        str: JSON representation of the Plotly chart.
    """
    # Retrieve revenue data per day
    data = revenue_data['revenue_days']
    revenue_df = pd.DataFrame(data)

    # If DataFrame is empty, fill with zero revenue for the given period
    if revenue_df.empty:
        revenue_df['date'] = pd.date_range(from_date, to_date)
        revenue_df['total_revenue'] = 0

    # Create a Plotly line chart
    plot = go.Bar(
        x=revenue_df.date, 
        y=revenue_df.total_revenue, 
        text=revenue_df.total_revenue.apply(lambda x: "Rp. {:,f}".format(x)), 
        textposition='inside') if chart_types == "bar" else \
            go.Scatter(
                x=revenue_df.date, 
                y=revenue_df.total_revenue)
        
    fig = go.Figure(
        plot
    )

    # Update layout of the chart
    fig.update_layout(title='Total Revenue Per Days' if chart_types == "line" else "Total Revenue per Month")
    fig.update_xaxes(title='Months', dtick='D1' if chart_types == "line" else "M1")
    if revenue_df['date'].count() >= 31:
        fig.update_xaxes(title='Months', dtick='M1')
    fig.update_yaxes(title='Total Revenue')
    if chart_types == "line":
        fig.update_traces(text=revenue_df.total_revenue, textposition='top center')

    # Convert chart to JSON
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def coin_days(
        from_date: datetime.date, 
        to_date: datetime.date,
        revenue_data: Dict) -> str:
    """
    Generate a chart showing coin purchases per day of the week.

    Args:
        from_date (str, optional): Start date of the period (inclusive), format: 'YYYY-MM-DD'.
        to_date (str, optional): End date of the period (inclusive), format: 'YYYY-MM-DD'.
        revenue_data (Dict): Dict to retrieve transaction data.

    Returns:
        str: JSON representation of the Plotly chart.
    """
    # Retrieve coin purchase data per day
    data = revenue_data['coin_days_chart']
    df = pd.DataFrame(data)

    # If DataFrame is empty, fill with zero values for the given period
    if df.empty:
        df['date'] = pd.date_range(from_date, to_date)
        df['day'] = pd.to_datetime(df['date']).dt.day_name()
        df['total_pembelian'] = 0

    # Create a Plotly bar chart
    fig = go.Figure(
        go.Bar(y=df.total_pembelian, x=df.day, text=df['total_pembelian'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside',
               marker=dict(showscale=True, colorscale='emrld', color=df.total_pembelian))
    )

    # Configure figure
    fig.update_layout(title='Coin Purchase Per Days')
    fig.update_xaxes(title='Day', dtick='D1', tickformat='%b', categoryorder='array', categoryarray=[
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
    ])
    fig.update_yaxes(title='Total Coin Transaction')

    # Convert chart to JSON
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def transaksi_koin_details(
        revenue_data: Dict, 
        filters: str ='') -> str:
    """
    Generate a table showing transaction details for coin purchases.

    Args:
        revenue_data (object): Object with beli_coin method to retrieve revenue data.
        filters (str, optional): Filter for transaction status ('paid', 'expired', 'pending' .default is '').

    Returns:
        str: JSON representation of the Plotly table.
    """
    # Retrieve transaction data for coin purchases
    data = revenue_data['dataframe']
    df = pd.DataFrame(data)
    to_date = datetime.now().date() - timedelta(days=1)
    if df.empty:
        df = pd.DataFrame({
                'id': ['-'],
                'user_id': [0],
                "install_date": pd.date_range(to_date, to_date).date,
                "register_date": pd.date_range(to_date, to_date),
                "fullname": ["-"],
                "email": ["-"],
                "transaction_date": pd.date_range(to_date, to_date),
                "coin_value": [0],
                "revenue": [0],
                "amount": [0],
                "bank_code": ["-"],
                "payment_gateway": ["-"],
                "payment_channel": ["-"],
                "status": ["-"],
                "payment_date": pd.date_range(to_date, to_date).date,
                "source": ["-"]
            })
        
    # Concatenate payment_channel and bank_code, remove unnecessary characters
    df['payment_channel'] = await asyncio.to_thread(lambda: df['payment_channel'] + "-" + df['bank_code'])
    df['payment_channel'] = await asyncio.to_thread(lambda: df['payment_channel'].replace(re.compile(r'[\"\-]'), '', regex=True))
    df['payment_channel'] = await asyncio.to_thread(lambda: df['payment_channel'].replace(re.compile(r'bank_transfer'), '', regex=True))
    await asyncio.to_thread(lambda: df.fillna('-', inplace=True))
    await asyncio.to_thread(lambda: df.sort_values(by='transaction_date', ascending=False, inplace=True))
    df['status'] = await asyncio.to_thread(lambda: df['status'].apply(str.lower))
    df['status'] = await asyncio.to_thread(lambda: df['status'].replace({'expire': 'expired', 'settlement': 'paid'}))
    
    # Apply filters if provided
    if filters == '':
        df_filter = df.copy()
    else:
        df_filter = await asyncio.to_thread(lambda: df[df['status'] == filters])
    
    # Create a Plotly table
    fig = go.Figure(
        go.Table(
            header=dict(
                fill_color="grey",
                line_color="black",
                font=dict(color="black"),
                values=['Transaction Date', 'Status', 'Payment Gateway', 'Payment Channel', 'Amount', 'Payment Date', 'User ID', 'Fullname', 'Register Date']),
            cells=dict(
                fill_color="white",
                line_color="black",
                font=dict(color="black"),
                values=[df_filter['transaction_date'], df_filter['status'], df_filter['payment_gateway'], df_filter['payment_channel'], df_filter['amount'].apply(lambda x: "Rp. {:,.0f}".format((x))), df_filter['payment_date'], df_filter['user_id'], df_filter['fullname'], df_filter['register_date']])
        )
    )

    # Update layout of the table
    fig.update_layout(title='Coin Transaction Details')

    # Convert table to JSON
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    return chart


async def old_new_user_pembeli_koin(
        from_date: datetime.date, 
        to_date: datetime.date, 
        revenue_data: Dict) -> pd.DataFrame:
    """
    Calculate statistics of old and new users who purchased coins.

    Args:
        from_date (str, optional): Start date of the period (inclusive), format: 'YYYY-MM-DD'.
        to_date (str, optional): End date of the period (inclusive), format: 'YYYY-MM-DD'.
        revenue_data (Dict): Dict to retrieve revenue data.

    Returns:
        pandas.DataFrame: DataFrame containing statistics of old and new users.
    """
    # Retrieve data for old and new users who purchased coins
    data = revenue_data['old_new_df']
    df = pd.DataFrame(data)
    
    # If DataFrame is empty, fill with default values
    if df.empty:
        df['buy_date'] = pd.date_range(from_date, to_date)
        df['user_id'] = 0
        df['email'] = '-'
        df['fullname'] = '-'
        df['created_at'] = pd.date_range(from_date, to_date)
        df['created_at'] = pd.to_datetime(df['created_at']).dt.date
    
    # Separate new and old users based on buy_date
    new_user = await asyncio.to_thread(lambda: df[(df['created_at'] >= from_date) & (df['created_at'] <= to_date)])
    old_user = await asyncio.to_thread(lambda: df[(df['created_at'] < from_date)])

    # Group by buy_date to count unique new and old users
    new_user_group = await asyncio.to_thread(lambda: new_user.groupby(['buy_date'])['user_id'].nunique().reset_index())
    await asyncio.to_thread(lambda: new_user_group.rename(columns={'user_id':'new_user'}, inplace=True))
    old_user_group = await asyncio.to_thread(lambda: old_user.groupby(['buy_date'])['user_id'].nunique().reset_index())
    await asyncio.to_thread(lambda: old_user_group.rename(columns={'user_id':'old_user'}, inplace=True))

    # Merge new and old user data
    df_merged = await asyncio.to_thread(lambda: pd.merge(new_user_group, old_user_group, how='outer', on='buy_date'))
    await asyncio.to_thread(lambda: df_merged.fillna(0, inplace=True))
    df_merged['new_user'] = await asyncio.to_thread(lambda: df_merged['new_user'].astype(int))
    df_merged['old_user'] = await asyncio.to_thread(lambda: df_merged['old_user'].astype(int))
    
    return df_merged


async def old_new_user_pembeli_koin_chart(
        from_date: datetime.date, 
        to_date: datetime.date, 
        revenue_data: Dict) -> str:
    """
    Generate a chart showing old and new user purchasers of coins.

    Args:
        from_date (str, optional): Start date of the period (inclusive), format: 'YYYY-MM-DD'.
        to_date (str, optional): End date of the period (inclusive), format: 'YYYY-MM-DD'.
        revenue_data (Dict): Dict to retrieve revenue data.

    Returns:
        str: JSON representation of the Plotly chart.
    """
    # Calculate old and new user statistics
    df = await old_new_user_pembeli_koin(from_date=from_date, to_date=to_date, revenue_data=revenue_data)
    await asyncio.to_thread(lambda: df.sort_values(by='buy_date', ascending=True, inplace=True))

    # Create a Plotly bar chart
    fig = go.Figure(data=[
        go.Bar(x=df['buy_date'], y=df['old_user'], name='Old Coin Purchaser', text=df['old_user'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside'),
        go.Bar(x=df['buy_date'], y=df['new_user'], name='New Coin Purchaser', text=df['new_user'].apply(lambda x: "{:,.0f}".format((x))), textposition='inside')
    ])

    # Update layout of the chart
    fig.update_layout(title='Old & New Coin Purchaser')
    fig.update_xaxes(title='Date', dtick='D1')
    if df['buy_date'].count() >= 31:
        fig.update_xaxes(title='Date', dtick='M1')
    fig.update_yaxes(title='Value')

    # Convert chart to JSON
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def chart_returning_first_purchase(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date:datetime.date, 
        source: str, 
        date_format: datetime.date = '%Y-%m-%d') -> str:
    """
    Generates a plotly chart depicting first purchase vs returning purchase vs total transactions over a date range for a given data source.

    Args:
        session (AsyncSession): An asynchronous database session object.
        from_date (datetime.date): The start date for the date range (inclusive).
        to_date (datetime.date): The end date for the date range (inclusive).
        source (str): The data source to query for purchase information.
        date_format (datetime.date, optional): The format string used to parse date strings from the data source. Defaults to '%Y-%m-%d'.

    Returns:
        str: A JSON-encoded string representing the plotly chart data.

    Raises:
        Exception: An exception if an error occurs while generating the chart.
    """
    try:
        df = await returning_first_purchase(session=session, data='df', from_date=from_date, to_date=to_date, source=source, date_format=date_format)
        df["period"] = await asyncio.to_thread(pd.to_datetime, df["period"])
        df['period'] = await asyncio.to_thread(lambda: df['period'].dt.date)
        await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
        df = await asyncio.to_thread(lambda: df.infer_objects(copy=True))
        df['total_transaction'] = await asyncio.to_thread(lambda: df['first_purchase'] + df['returning_purchase'])
        await asyncio.to_thread(lambda: df.sort_values(by='period', ascending=True, inplace=True))

        trace1 = go.Bar(
            x=df['period'],
            y=df['first_purchase'],
            name='First Purchase',
            text=df['first_purchase'].apply(lambda x: "{:,.0f}".format((x))),
            textposition='inside',
            yaxis='y',
            marker=dict(color='red'))
        trace2 = go.Bar(
            x=df['period'],
            y=df['returning_purchase'],
            name='Returning Purchase',
            text=df['returning_purchase'].apply(lambda x: "{:,.0f}".format((x))),
            textposition='inside',
            yaxis='y',
            marker=dict(color='green'))
        trace3 = go.Bar(
            x=df['period'],
            y=df['total_transaction'],
            name='Total Transaction',
            text=df['total_transaction'].apply(lambda x: "{:,.0f}".format((x))),
            textposition='inside',
            yaxis='y',
            marker=dict(color='blue'))

        layout = go.Layout(
            title='First & Returning Purchase Coin',
            yaxis=dict(
                title='Total User'
            )
        )

        fig = go.Figure(data=[trace1, trace2, trace3], layout=layout)

        fig.update_layout(
            barmode='group', 
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        return chart

    except Exception as e:
        return f"An error occurred: {e}"


async def unique_count_users_admob(
        revenue_data: Dict, 
        periode: str = 'month') -> str:
    """
    Generate a bar chart showing unique and count users from AdMob data.

    Args:
        revenue_data (Dict): Dict to retrieve revenue data.
        periode (str): Period for grouping data ('month', 'daily').

    Returns:
        str: JSON representation of the Plotly bar chart.
    """
    # Retrieve data
    data = revenue_data['df_unique_count']
    df = pd.DataFrame(data)
    await asyncio.to_thread(
        lambda: df.rename(
            columns={
                'tanggal':'date', 
                'chapter_admob_unique':'unique', 
                'chapter_admob_count':'count'}, 
            inplace=True))

    # Create bar traces for count and unique users
    trace1 = go.Bar(
        yaxis='y',
        x=df['date'], 
        y=df['count'], 
        name='Users Count', 
        text=df['count'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='inside')
    trace2 = go.Bar(
        yaxis='y',
        x=df['date'], 
        y=df['unique'], 
        name='Users Unique', 
        text= df['unique'].apply(lambda x: "{:,.0f}".format((x))), 
        textposition='outside')

    # Define layout
    layout = go.Layout(
        title=f'Admob Users Unique & Count',
        yaxis1=dict(
            title='Value'
        ),
        yaxis2=dict(
            overlaying='y',
            side='right',
            tickformat='.0%'
        ), 
        barmode='group'
    )

    # Combine traces and layout into a Figure object
    fig = go.Figure(data=[trace1, trace2], layout=layout)

    # Set barmode to stack and adjust legend position
    fig.update_layout(
        barmode='stack',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ))
    
    # Update x-axis tick labels based on the period
    if periode == 'month':
        fig.update_xaxes(dtick='M1')
    else:
        fig.update_xaxes(dtick='D1')

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def impression_revenue_chart(revenue_data: Dict, period: str = "daily") -> str:
    """
    Fetches AdMob data for a given period, aggregates impressions and estimated earnings by date,
    and creates a Plotly chart visualizing the relationship between impressions and revenue.

    Args:
        revenue_data (Dict): Dict to retrieve revenue DataFrame.
        period (str): The period o fthe data ('daily' or ' monthly')

    Returns:
        str: JSON representation of a Plotly chart showing impressions and estimated earnings.
    """

    try:
        data = revenue_data["df_ads"]
        df = pd.DataFrame(data)

        # Aggregate data
        df_agg = await asyncio.to_thread(lambda: df.groupby('Date')[['Impressions', 'Estimated earnings']].sum().reset_index())

        # Create Plotly figure
        fig = go.Figure(data=[
            go.Bar(
                x=df_agg['Date'],
                y=df_agg['Impressions'],
                name='Impressions',
                yaxis='y',
                text=df_agg['Impressions'].apply(lambda x: "{:,.0f}".format((x))),
                textposition='auto',
                hovertemplate='Impressions: %{y}<extra></extra>'  # Custom hover text
            ),
            go.Scatter(
                x=df_agg['Date'],
                y=df_agg['Estimated earnings'],
                name='Estimated Earnings',
                yaxis='y2',
                text=df_agg['Estimated earnings'].apply(lambda x: f'Rp. {x:,.2f}'),
                textposition='top center',
                hovertemplate='Revenue: %{text}<extra></extra>'  # Custom hover text
            )
        ])

        # Layout configuration
        fig.update_layout(
            title='AdMob Impressions to Revenue',
            xaxis_title='Date',
            yaxis=dict(title='Impressions'),
            yaxis2=dict(title='Revenue (Rp)', overlaying='y', side='right')
        )
        fig.update_xaxes(dtick='D1' if period == "daily" else "M1") 

        return await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    except Exception as e:
        return json.dumps({'error': f'An error occurred while generating the chart, {e}'})


async def ads_details(
        revenue_data: Dict, 
        source: str = "app") -> str:
    """
    Fetches AdMob details data for a specified period, sorts it, and generates a Plotly table visualization.

    Args:
        revenue_data (Dict): Dict to retrieve revenue data.
        source (str): The Source of data ('app' or 'web').

    Returns:
        str: JSON representation of a Plotly table displaying the AdMob details.
    """

    try:
        data = revenue_data["df_ads"]
        df = pd.DataFrame(data)

        # Sort the DataFrame for better presentation (optional)
        await asyncio.to_thread(lambda: df.sort_values(by=['Date', 'Platform'], ascending=True, inplace=True))
        
        # Format numeric columns (using f-strings for better readability)
        df['Estimated earnings'] = await asyncio.to_thread(lambda: df['Estimated earnings'].apply(lambda x: f'Rp. {x:,.2f}'))
        df["Impressions"] = await asyncio.to_thread(lambda: df["Impressions"].apply(lambda x: f'{x:,.0f}'))
        df["Clicks"] = await asyncio.to_thread(lambda: df["Clicks"].apply(lambda x: f'{x:,.0f}'))
        df["Ad requests"] = await asyncio.to_thread(lambda: df["Ad requests"].apply(lambda x: f'{x:,.0f}'))
        df["Matched requests"] = await asyncio.to_thread(lambda: df["Matched requests"].apply(lambda x: f'{x:,.0f}'))
        df['Impression CTR'] = await asyncio.to_thread(lambda: df['Impression CTR'].apply(lambda x: f'{x:.2%}'))
        df['Match rate'] = await asyncio.to_thread(lambda: df['Match rate'].apply(lambda x: f'{x:.2%}'))
        df['Show rate'] = await asyncio.to_thread(lambda: df['Show rate'].apply(lambda x: f'{x:.2%}'))
        if source == "app":
            df['Observed ECPM'] = await asyncio.to_thread(lambda: df['Observed ECPM'].apply(lambda x: f'Rp. {x:,.2f}'))
        elif source == "web":
            df['Impression RPM'] = await asyncio.to_thread(lambda: df['Impression RPM'].apply(lambda x: f'Rp. {x:,.2f}'))

        # Create the Plotly table
        fig = go.Figure(
            go.Table(
                header=dict(
                    fill_color="grey",
                    line_color="black",
                    font=dict(color="black"),
                    values=df.columns),
                cells=dict(
                    fill_color="white",
                    line_color="black",
                    font=dict(color="black"),
                    values=df.values.T)  # Transpose for easier column-wise access
            )
        )

        # Update the layout (optional)
        fig.update_layout(
            title='Ads Details',
            autosize=True,  # Adjust table size automatically for better fit
        )
        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart

    except Exception as e:
        return json.dumps({'error': f'An error occurred while generating the table, {e}'})


async def frequency_distribution_admob_df(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        types: str = 'chart', 
        source: str = 'app') -> Union[str, pd.DataFrame]:
    """
    Calculates the frequency distribution of AdMob chapter opens per user within a specified date range and source.

    Args:
        session (AsyncSession): An asynchronous database session object.
        from_date (datetime.date): Start date of the period.
        to_date (datetime.date): End date of the period.
        types (str, optional): Output type ('chart' or 'dataframe'). Defaults to 'chart'.
        source (str, optional): Data source ('app' or 'web'). Defaults to 'app'.

    Returns:
        str or DataFrame: 
            - If `types` is 'chart', returns a JSON string of a Plotly bar chart.
            - If `types` is 'dataframe', returns the underlying DataFrame with the frequency distribution data.
    """

    try:
        try :
            frequency_subquery = select(
                GooddreamerUserChapterAdmob.user_id.label('user_id'),
                func.count(GooddreamerUserChapterAdmob.id).label('total_buka_chapter')
            ).join(
                GooddreamerUserChapterAdmob.model_has_sources
            ).join(
                ModelHasSources.sources
            ).filter(
                ModelHasSources.model_type == 'App\\Models\\UserChapterAdmob',
                func.date(GooddreamerUserChapterAdmob.created_at).between(from_date, to_date),
                Sources.name == source
            ).group_by(GooddreamerUserChapterAdmob.user_id).subquery()

            max_query = select(
                func.max(frequency_subquery.c.total_buka_chapter).label('max_count')
            )
            max_query_result = await session.execute(max_query)
            
            df_max = pd.DataFrame(max_query_result.fetchall())

            if df_max.empty:
                df_max['max_count'] = 0 

            # Main query
            query = select(
                    Case(
                        (frequency_subquery.c.total_buka_chapter == 1, 1),
                        (frequency_subquery.c.total_buka_chapter == 2, 2),
                        (frequency_subquery.c.total_buka_chapter == 3, 3),
                        (frequency_subquery.c.total_buka_chapter == 4, 4),
                        (frequency_subquery.c.total_buka_chapter == 5, 5),
                        (frequency_subquery.c.total_buka_chapter.between(6, 9), 6),
                        (frequency_subquery.c.total_buka_chapter.between(10, 11), 7),
                        (frequency_subquery.c.total_buka_chapter.between(12, 17), 8),
                        (frequency_subquery.c.total_buka_chapter.between(18, 25), 9),
                        (frequency_subquery.c.total_buka_chapter.between(26, 46), 10),
                        (frequency_subquery.c.total_buka_chapter.between(47, 76), 11),
                        (frequency_subquery.c.total_buka_chapter >= 77, 12),
                        else_=None
                    ).label('frequency_distribution_group_number'),
                    Case(
                        (frequency_subquery.c.total_buka_chapter == 1, '1 Times'),
                        (frequency_subquery.c.total_buka_chapter == 2, '2 Times'),
                        (frequency_subquery.c.total_buka_chapter == 3, '3 Times'),
                        (frequency_subquery.c.total_buka_chapter == 4, '4 Times'),
                        (frequency_subquery.c.total_buka_chapter == 5, '5 Times'),
                        (frequency_subquery.c.total_buka_chapter.between(6, 9), '6 - 9 Times'),
                        (frequency_subquery.c.total_buka_chapter.between(10, 11), '10 - 11 Times'),
                        (frequency_subquery.c.total_buka_chapter.between(12, 17), '12 - 17 Times'),
                        (frequency_subquery.c.total_buka_chapter.between(18, 25), '18 - 25 Times'),
                        (frequency_subquery.c.total_buka_chapter.between(26, 46), '26 - 46 Times'),
                        (frequency_subquery.c.total_buka_chapter.between(47, 76), '47 - 76 Times'),
                        (frequency_subquery.c.total_buka_chapter >= 77, f'77 - {df_max["max_count"].item()} Times'),
                        else_=None
                    ).label('frequency_distribution_group'),
                    Case(
                        (frequency_subquery.c.total_buka_chapter == 1, func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter == 2, func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter == 3, func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter == 4, func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter == 5, func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter.between(6, 9), func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter.between(10, 11), func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter.between(12, 17), func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter.between(18, 25), func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter.between(26, 46), func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter.between(47, 76), func.count(frequency_subquery.c.total_buka_chapter)),
                        (frequency_subquery.c.total_buka_chapter >= 77, func.count(frequency_subquery.c.total_buka_chapter)),
                        else_=None
                    ).label('user_count')
                ).group_by(
                    'frequency_distribution_group_number'
                ).order_by(asc('frequency_distribution_group_number'))
            
            result = await session.execute(query)
            df = pd.DataFrame(result.fetchall())

        except OperationalError:
            default_values = {
                "frequency_distribution_group_number" : 0,
                "frequency_distribution_group": 0,
                "user_count": 0
            }
            df = pd.DataFrame(default_values, index=[0])
        
        if df.empty:
            df['frequency_distribution_group_number'] = 0
            df['frequency_distribution_group'] = 0
            df['user_count'] = 0

        if types == 'chart':
            # Create and return the Plotly chart
            fig = go.Figure(go.Bar(
                x=df['frequency_distribution_group'],
                y=df['user_count'],
                text=df['user_count'].apply(lambda x: "{:,.0f}".format((x))),
                textposition='outside'
            ))
            fig.update_layout(
                title='Frequency Distribution Group Ads Transaction',
                xaxis_title='Frequency Distribution Group',
                yaxis_title='User Count'
            )
            chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
            return chart
        else:
            return df

    except Exception as e:
        return f"Error: {e}"


async def frequency_admob_table(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        source: str = 'app') -> str:
    """
    Generates a Plotly table visualizing the frequency distribution of AdMob chapter opens.

    Args:
        session (AsyncSession): An asynchronous database session object.
        from_date (datetime.date): Start date of the period.
        to_date (datetime.date): End date of the period.
        source (str, optional): Data source ('app' or 'web'). Defaults to 'app'.

    Returns:
        str: JSON representation of a Plotly table showing frequency distribution groups and user counts.
    """

    try:
        # Fetch Frequency Distribution Data
        df = await frequency_distribution_admob_df(
            session=session, from_date=from_date, to_date=to_date, types='df', source=source
        )

        # Create Plotly Table
        fig = go.Figure(
            go.Table(
                header=dict(
                    fill_color="grey",
                    line_color="black",
                    font=dict(color="black"),
                    values=['Frequency Distribution Group', 'User Count']),
                cells=dict(
                    fill_color="white",
                    line_color="black",
                    font=dict(color="black"),
                    values=[df['frequency_distribution_group'], df['user_count']])
            )
        )

        # Update Layout
        fig.update_layout(
            title='Frequency Distribution Group Table',
            autosize=True,  # Auto-adjust table size
            # Additional layout options as needed
        )
        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        return chart

    except Exception as e:  # Catch any unexpected errors
        return json.dumps({'error': f'An error occurred while generating the table, {e}'})


async def revenue_all_chart(
        revenue_data: Dict, 
        period: str = 'monthly') -> str:
    """
    Generates a revenue chart combining app and web revenue from various sources.

    Args:
        revenue_data (Dict): An Dict that fetching web revenue data.
        period (str): The period for aggregation, either 'monthly' or 'daily'. Default is 'monthly'.

    Returns:
        str: A JSON string representing the Plotly chart.
    """
    
    # Fetch and preprocess app revenue data
    data = revenue_data["revenue_all_chart"]
    df = pd.DataFrame(data)

    await asyncio.to_thread(lambda: df.rename(columns={"Date": "periods"}, inplace=True))
    df["periods"] = await asyncio.to_thread(pd.to_datetime, df["periods"])
    df['periods'] = await asyncio.to_thread(lambda: df['periods'].dt.date)
    await asyncio.to_thread(lambda: df.sort_values(by='periods', ascending=True, inplace=True))
    
    # Define traces for the Plotly chart
    trace1 = go.Bar(
        x=df['periods'],
        y=df['total_revenue'],
        name='Total Revenue',
        text=df['total_revenue'].apply(lambda x: "Rp. {:,.0f}".format((x))),
        textposition='inside',
        yaxis='y'
    )

    trace2 = go.Scatter(
        x=df['periods'],
        y=df['amount'],
        name='Revenue Coin',
        yaxis='y2'
    )
    
    trace3 = go.Scatter(
        x=df['periods'],
        y=df['total_ads_revenue'],
        name='Revenue Ads',
        yaxis='y2'
    )

    # Define the layout with a secondary y-axis
    layout = go.Layout(
        title='Revenue All Time',
        yaxis=dict(
            title='Total Revenue'
        ),
        yaxis2=dict(
            title='Revenue Components',
            overlaying='y',
            side='right'
        )
    )

    # Combine the traces and layout into a Figure object
    fig = go.Figure(data=[trace1, trace2, trace3], layout=layout)

    if period == 'monthly':
        fig.update_xaxes(dtick='M1')
    else:
        fig.update_xaxes(dtick='D1')

    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart
