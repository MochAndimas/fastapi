"""function file new install"""
import pandas as pd
from datetime import datetime, timedelta
import json
import plotly
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
import plotly.graph_objects as go
from sqlalchemy import func, select
from app.db.models.acquisition import GoogleAdsData, FacebookAdsData, TiktokAdsData, AsaData, Currency


class InstallData:
    """
    A class to process and store install data for a mobile application from various sources, 
    including Google Ads, Facebook Ads, TikTok Ads, Apple Search Ads (ASA), organic installs, 
    and unidentified referrals.

    This class provides an interface to load, process, and access install data asynchronously 
    using an SQLAlchemy AsyncSession, and it supports date-based filtering for fetching the install data.

    Attributes:
        session (AsyncSession): The asynchronous SQLAlchemy session used to interact with the database.
        from_date (datetime.date): The start date for the data retrieval.
        to_date (datetime.date): The end date for the data retrieval.
        df_google_install (Optional[DataFrame]): DataFrame containing install data from Google Ads.
        df_facebook_install (Optional[DataFrame]): DataFrame containing install data from Facebook Ads.
        df_tiktok_install (Optional[DataFrame]): DataFrame containing install data from TikTok Ads.
        df_asa_install (Optional[DataFrame]): DataFrame containing install data from Apple Search Ads (ASA).
        df_organic (Optional[DataFrame]): DataFrame containing organic install data.
        df_undetected_referrals (Optional[DataFrame]): DataFrame containing install data for unidentified referrals.

    Methods:
        __init__(session, from_date, to_date):
            Initializes an `InstallData` instance with the given database session and date range.
        
        load_data(session, from_date, to_date):
            Class method to create an instance of `InstallData` and load the install data 
            for the specified date range asynchronously.

        _fetch_data():
            Internal method to fetch data from the database for all sources asynchronously 
            based on the specified date range.

        _read_db(data, from_date, to_date):
            Placeholder for a method to read the data from the database for a specific data source 
            based on the specified date range.
    """
    def __init__(
            self, 
            session: AsyncSession, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Initializes an `InstallData` object with the provided SQLAlchemy session and date range.

        Args:
            session (AsyncSession): The asynchronous SQLAlchemy session used for database operations.
            from_date (datetime.date): The start date for filtering the install data.
            to_date (datetime.date): The end date for filtering the install data.
        """
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_google_install = pd.DataFrame()
        self.df_facebook_install = pd.DataFrame()
        self.df_tiktok_install = pd.DataFrame()
        self.df_asa_install = pd.DataFrame()
        self.df_organic = pd.DataFrame()
        self.df_undetected_refferals = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: datetime.date, to_date: datetime.date):
        """
        Asynchronously loads and processes install data for a given date range.

        Creates an `InstallData` instance and fetches the install data from multiple sources asynchronously 
        based on the specified date range.

        Args:
            session (AsyncSession): The asynchronous SQLAlchemy session used for database operations.
            from_date (datetime.date): The start date for filtering the install data.
            to_date (datetime.date): The end date for filtering the install data.

        Returns:
            InstallData: An instance of `InstallData` with the fetched data.
        """
        instance = cls(session, from_date, to_date)
        await instance._fetch_data()
        return instance
    
    async def _fetch_data(self):
        """
        Asynchronously fetches install data from various sources for the specified date range.

        This method fetches install data for each source, including Google Ads, Facebook Ads, 
        TikTok Ads, Apple Search Ads (ASA), organic installs, and unidentified referrals. It uses 
        a helper method `_read_db` to retrieve data from the database for each source.

        Note:
            This method is intended to be used internally by the class.
        """
        await self._read_db(data="google", from_date=self.from_date, to_date=self.to_date)
        await self._read_db(data="facebook", from_date=self.from_date, to_date=self.to_date)
        await self._read_db(data="tiktok", from_date=self.from_date, to_date=self.to_date)
        await self._read_db(data="asa", from_date=self.from_date, to_date=self.to_date)
        await self._read_db(data="organic", from_date=self.from_date, to_date=self.to_date)
        await self._read_db(data="undetected_refferals", from_date=self.from_date, to_date=self.to_date)

    async def _read_db(
            self, 
            data: str, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Reads install data from the database for a specific data source 
        (e.g., Google Ads, Facebook Ads, etc.) within the specified date range.

        Args:
            data (str): The data source to read data from (e.g., "google", 
                "facebook", "tiktok", "asa", "organic", "undetected_refferals".).
            from_date (datetime.date): The start date for the data query.
            to_date (datetime.date): The end date for the data query.
        """

        if data == "google":
            query = select(
                GoogleAdsData.date.label("date"),
                func.sum(GoogleAdsData.conversions).label("google_install")
            ).filter(
                GoogleAdsData.date.between(from_date, to_date),
                GoogleAdsData.campaign_name.in_(["UA - App Install - Android - ID (Major Cities)"])
            ).group_by(
                "date"
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df = pd.DataFrame(data)

            if df.empty:
                df = pd.DataFrame({
                    "date": pd.date_range(to_date, to_date).date,
                    "google_install": [0]
                })

            df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
            df["date"] = await asyncio.to_thread(lambda: df["date"].dt.date)
            df["google_install"] = await asyncio.to_thread(lambda: df["google_install"].astype(int))
            df["Media Source"] = "Google Ads"
            self.df_google_install = df.copy()
        
        elif data == "facebook":
            query = select(
                FacebookAdsData.date_start.label("date"),
                func.sum(FacebookAdsData.unique_actions_mobile_app_install).label("facebook_install")
            ).filter(
                FacebookAdsData.date_start.between(from_date, to_date),
                FacebookAdsData.campaign_name.in_(["AAA"])
            ).group_by(
                "date"
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df = pd.DataFrame(data)

            if df.empty:
                df = pd.DataFrame({
                    "date": pd.date_range(to_date, to_date).date,
                    "facebook_install": [0]
                })

            df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
            df["date"] = await asyncio.to_thread(lambda: df["date"].dt.date)
            df["facebook_install"] = await asyncio.to_thread(lambda: df["facebook_install"].astype(int))
            df["Media Source"] = "Facebook Ads"
            self.df_facebook_install = df.copy()

        elif data == "tiktok":
            query  = select(
                TiktokAdsData.date.label("date"),
                func.sum(TiktokAdsData.conversion).label("tiktok_install")
            ).filter(
                TiktokAdsData.date.between(from_date, to_date),
                TiktokAdsData.campaign_name.in_(["UA - App Install - Android - ID"])
            ).group_by(
                "date"
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df = pd.DataFrame(data)

            if df.empty:
                df = pd.DataFrame({
                    "date": pd.date_range(to_date, to_date).date,
                    "tiktok_install": [0]
                })

            df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
            df["date"] = await asyncio.to_thread(lambda: df["date"].dt.date)
            df["tiktok_install"] = await asyncio.to_thread(lambda: df["tiktok_install"].astype(int))
            df["Media Source"] = "Tiktok Ads"
            self.df_tiktok_install = df.copy()

        elif data == "asa":
            query = select(
                AsaData.date.label("date"),
                func.sum(AsaData.installs).label("asa_install")
            ).filter(
                AsaData.date.between(from_date, to_date)            
            ).group_by(
                "date"
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df = pd.DataFrame(data)

            if df.empty:
                df = pd.DataFrame({
                    "date": pd.date_range(to_date, to_date).date,
                    "asa_install": [0]
                })
            
            df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
            df["date"] = await asyncio.to_thread(lambda: df["date"].dt.date)
            df["asa_install"] = await asyncio.to_thread(lambda: df["asa_install"].astype(int))
            df["Media Source"] = "Apple Search Ads"
            self.df_asa_install = df.copy()

        elif data == "organic":
            # Load and preprocess Android organic data
            android_organic_read = pd.read_csv('./csv/organic_play_console.csv', delimiter=',', index_col=False)
            await asyncio.to_thread(
                lambda: android_organic_read.rename(columns={
                    'Date': 'date',
                    'Store listing acquisitions: Google Play search': 'google_play_search',
                    'Store listing acquisitions: Google Play explore': 'google_play_explore',
                    'Store listing acquisitions: All traffic sources': 'all_traffic_sources',
                    'Store listing acquisitions: Ads and referrals': 'ads_and_referrals'
                }, inplace=True))
            
            # Select relevant columns and clean the data
            android_organic_read["date"] = await asyncio.to_thread(
                pd.to_datetime,
                android_organic_read["date"], 
                format="mixed", 
                dayfirst=True)
            android_organic_read["date"] = await asyncio.to_thread(lambda: android_organic_read["date"].dt.date)
            await asyncio.to_thread(lambda: android_organic_read.replace(",", "", inplace=True))
            android_organic_read = await asyncio.to_thread(
                lambda: android_organic_read[
                    (android_organic_read["date"] >= from_date) & 
                    (android_organic_read["date"] <= to_date)])
            android_organic_read["android_organic_install"] = await asyncio.to_thread(lambda: android_organic_read["google_play_search"] + android_organic_read["google_play_explore"])
            df_android_organic = await asyncio.to_thread(lambda: android_organic_read.loc[:, ['date', 'android_organic_install']])
            
            # Load and preprocess iOS data from Apple Connect
            apple_connect_read = pd.read_csv('./csv/apple_total_download.csv')
            df_apple_connect = pd.DataFrame(apple_connect_read)
            await asyncio.to_thread(lambda: df_apple_connect.rename(columns={"Date": "date"}, inplace=True))
            df_apple_connect['date'] = await asyncio.to_thread(pd.to_datetime, df_apple_connect['date'], format='%m/%d/%y')
            df_apple_connect["date"] = await asyncio.to_thread(lambda: df_apple_connect["date"].dt.date)
            df_apple_connect = await asyncio.to_thread(
                lambda: df_apple_connect[
                    (df_apple_connect['date'] >= from_date) & 
                    (df_apple_connect['date'] <= to_date)])

            query = select(
                AsaData.date.label("date"),
                func.sum(AsaData.installs).label("install")
            ).filter(
                AsaData.date.between(from_date, to_date)            
            ).group_by(
                "date"
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df_asa = pd.DataFrame(data)
            if df_asa.empty:
                df_asa = pd.DataFrame({
                    "date": pd.date_range(to_date,to_date).date,
                    "install": [0]
                })

            df_asa["date"] = await asyncio.to_thread(pd.to_datetime, df_asa["date"])
            df_asa["date"] = await asyncio.to_thread(lambda: df_asa["date"].dt.date)

            df_apple = await asyncio.to_thread(lambda: pd.merge(df_asa, df_apple_connect, on="date", how="outer"))
            await asyncio.to_thread(lambda: df_apple.fillna(0, inplace=True))
            df_apple["apple_organic_install"] = await asyncio.to_thread(lambda: df_apple["Total Downloads"] + df_apple["install"])
            df_apple = await asyncio.to_thread(lambda: df_apple[(df_apple["date"] >= from_date) & (df_apple["date"] <= to_date)])
            df_apple_organic = await asyncio.to_thread(lambda: df_apple.loc[:, ["date", "apple_organic_install"]])

            df = await asyncio.to_thread(lambda: pd.merge(df_android_organic, df_apple_organic, on="date", how="outer"))

            if df.empty:
                df = pd.DataFrame({
                    "date": pd.date_range(to_date, to_date).date,
                    "android_organic_install": [0],
                    "apple_organic_install": [0]
                })

            df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
            df["date"] = await asyncio.to_thread(lambda: df["date"].dt.date)
            df["Media Source"] = "Organic"
            self.df_organic = df.copy()

        elif data == "undetected_refferals":
            store_listing_read = pd.read_csv('./csv/organic_play_console.csv', delimiter=',', index_col=False)
            await asyncio.to_thread(lambda: store_listing_read.rename(columns={
                'Date': 'date',
                'Store listing acquisitions: Google Play search': 'google_play_search',
                'Store listing acquisitions: Google Play explore': 'google_play_explore',
                'Store listing acquisitions: All traffic sources': 'all_traffic_sources',
                'Store listing acquisitions: Ads and referrals': 'ads_and_referrals'
            }, inplace=True))
            
            # Select relevant columns and clean the data
            store_listing_read["date"] = await asyncio.to_thread(pd.to_datetime, store_listing_read["date"], format="mixed", dayfirst=True)
            store_listing_read["date"] = await asyncio.to_thread(lambda: store_listing_read["date"].dt.date)
            store_listing_read = await asyncio.to_thread(lambda: store_listing_read.loc[:, ["date", "all_traffic_sources", "ads_and_referrals", "google_play_search", "google_play_explore"]])
            store_listing_read = await asyncio.to_thread(lambda: store_listing_read[(store_listing_read["date"] >= from_date) & (store_listing_read["date"] <= to_date)])
            store_listing_read["date"] = await asyncio.to_thread(pd.to_datetime, store_listing_read["date"])
            store_listing_read["date"] = await asyncio.to_thread(lambda: store_listing_read["date"].dt.date)
            
            google_subquery = select(
                GoogleAdsData.date.label("date"),
                func.sum(GoogleAdsData.conversions).label("google_install")
            ).filter(
                GoogleAdsData.date.between(from_date, to_date),
                GoogleAdsData.campaign_name.in_(["UA - App Install - Android - ID (Major Cities)"])
            ).group_by(
                "date"
            ).subquery()

            facebook_subquery = select(
                FacebookAdsData.date_start.label("date"),
                func.sum(FacebookAdsData.unique_actions_mobile_app_install).label("facebook_install")
            ).filter(
                FacebookAdsData.date_start.between(from_date, to_date),
                FacebookAdsData.campaign_name.in_(["AAA"])
            ).group_by(
                "date"
            ).subquery()

            tiktok_subquery = select(
                TiktokAdsData.date.label("date"),
                func.sum(TiktokAdsData.conversion).label("tiktok_install")
            ).filter(
                TiktokAdsData.date.between(from_date, to_date),
                TiktokAdsData.campaign_name.in_(["UA - App Install - Android - ID"])
            ).group_by(
                "date"
            ).subquery()

            query = select(
                google_subquery.c.date.label("date"),
                (func.ifnull(google_subquery.c.google_install, 0) + \
                 func.ifnull(facebook_subquery.c.facebook_install, 0) + \
                    func.ifnull(tiktok_subquery.c.tiktok_install, 0)).label("android_ads_install")
            ).join(
                facebook_subquery, google_subquery.c.date == facebook_subquery.c.date, isouter=True
            ).join(
                tiktok_subquery, google_subquery.c.date == tiktok_subquery.c.date, isouter=True
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df_android_ads_install = pd.DataFrame(data)
            if df_android_ads_install.empty:
                df_android_ads_install = pd.DataFrame({
                "date": pd.date_range(to_date,to_date).date,
                "android_ads_install": [0]
            })
            df_android_ads_install["android_ads_install"] = await asyncio.to_thread(lambda: df_android_ads_install["android_ads_install"].astype(int))
            df_undetected_refferals = await asyncio.to_thread(lambda: pd.merge(df_android_ads_install, store_listing_read, on="date", how="outer"))

            if from_date not in store_listing_read["date"].values or to_date not in store_listing_read["date"].values:
                df_undetected_refferals["refferals"] = 0
                df_undetected_refferals["undetected"] = 0
            else:
                df_undetected_refferals["ads_and_referrals"] = await asyncio.to_thread(lambda: df_undetected_refferals["ads_and_referrals"].str.replace(",", ""))
                df_undetected_refferals["all_traffic_sources"] = await asyncio.to_thread(lambda: df_undetected_refferals["all_traffic_sources"].str.replace(",", ""))
                await asyncio.to_thread(lambda: df_undetected_refferals.fillna(0, inplace=True))
                df_undetected_refferals["all_traffic_sources"] = await asyncio.to_thread(lambda: df_undetected_refferals["all_traffic_sources"].astype(int))
                df_undetected_refferals["ads_and_referrals"] = await asyncio.to_thread(lambda: df_undetected_refferals["ads_and_referrals"].astype(int))
                df_undetected_refferals["android_organic"] = await asyncio.to_thread(lambda: df_undetected_refferals["google_play_search"] + df_undetected_refferals["google_play_explore"])
                df_undetected_refferals['refferals'] = await asyncio.to_thread(lambda: df_undetected_refferals["android_ads_install"] - df_undetected_refferals['ads_and_referrals'])
                df_undetected_refferals.loc[df_undetected_refferals['refferals'] < 0, 'refferals'] = 0
                df_undetected_refferals['all_channel'] = await asyncio.to_thread(lambda: (df_undetected_refferals["android_organic"] + df_undetected_refferals["android_ads_install"]) - df_undetected_refferals['refferals'])
                df_undetected_refferals['undetected'] = await asyncio.to_thread(lambda: df_undetected_refferals['all_traffic_sources'] - df_undetected_refferals['all_channel'])

            df_undetected_refferals = await asyncio.to_thread(lambda: df_undetected_refferals.loc[:, ["date", "refferals", "undetected"]])
            
            df_undetected_refferals["date"] = await asyncio.to_thread(pd.to_datetime, df_undetected_refferals["date"])
            df_undetected_refferals["date"] = await asyncio.to_thread(lambda: df_undetected_refferals["date"].dt.date)

            if df_undetected_refferals.empty:
                df_undetected_refferals = pd.DataFrame({
                    "date": pd.date_range(to_date, to_date).date,
                    "refferals": [0],
                    "undetected": [0]
                })
            
            df_undetected_refferals["Media Source"] = "Undetected & Referrals"
            self.df_undetected_refferals = df_undetected_refferals.copy()

    async def overall_install(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            metrics: list = []):
        """
        Calculates the total number of installs from various sources within the specified date range.

        Args:
            from_date (datetime.date): The start date for the install data query.
            to_date (datetime.date): The end date for the install data query.
            metrics (list): If metrics is given, it's filtering the returning metrics.
                - 'undetected_install'
                - 'android_organic'
                - 'apple_organic'
                - 'total_organic'
                - 'android_referrals'
                - 'fb_install'
                - 'google_install'
                - 'tiktok_install'
                - 'asa_install'
                - 'android_install'
                - 'apple_install'
                - 'total_install'

        Returns:
            dict: A dictionary containing the total number of installs for each source and platform.
        """
        df_google_install = await asyncio.to_thread(lambda: 
            self.df_google_install[(self.df_google_install["date"] >= from_date) & (self.df_google_install["date"] <= to_date)])
        df_facebook_install = await asyncio.to_thread(lambda: 
            self.df_facebook_install[(self.df_facebook_install["date"] >= from_date) & (self.df_facebook_install["date"] <= to_date)])
        df_tiktok_install = await asyncio.to_thread(lambda: 
            self.df_tiktok_install[(self.df_tiktok_install["date"] >= from_date) & (self.df_tiktok_install["date"] <= to_date)])
        df_asa_install = await asyncio.to_thread(lambda: 
            self.df_asa_install[(self.df_asa_install["date"] >= from_date) & (self.df_asa_install["date"] <= to_date)])
        df_organic = await asyncio.to_thread(lambda: 
            self.df_organic[(self.df_organic["date"] >= from_date) & (self.df_organic["date"] <= to_date)])
        df_undetected_refferals = await asyncio.to_thread(lambda: 
            self.df_undetected_refferals[(self.df_undetected_refferals["date"] >= from_date) & (self.df_undetected_refferals["date"] <= to_date)])
        
        undetected_install = await asyncio.to_thread(lambda: df_undetected_refferals["undetected"].sum())
        android_organic = await asyncio.to_thread(lambda: df_organic["android_organic_install"].sum())
        apple_organic = await asyncio.to_thread(lambda: df_organic["apple_organic_install"].sum())
        total_organic = android_organic + apple_organic
        android_referrals = await asyncio.to_thread(lambda: df_undetected_refferals["refferals"].sum())
        fb_install = await asyncio.to_thread(lambda: df_facebook_install["facebook_install"].sum())
        google_install = await asyncio.to_thread(lambda: df_google_install["google_install"].sum())
        tiktok_install = await asyncio.to_thread(lambda: df_tiktok_install["tiktok_install"].sum())
        asa_install = await asyncio.to_thread(lambda: df_asa_install["asa_install"].sum())
        android_install = google_install + fb_install + tiktok_install +android_organic
        apple_install = asa_install + apple_organic
        total_install = android_install + apple_install

        container = {
            "undetected_install": int(undetected_install),  
            "android_organic": int(android_organic),
            "apple_organic": int(apple_organic),
            "total_organic": int(total_organic),
            "android_referrals": int(android_referrals),
            "fb_install": int(fb_install),
            "google_install": int(google_install),
            "tiktok_install": int(tiktok_install),
            "asa_install": int(asa_install),
            "android_install": int(android_install),
            "apple_install": int(apple_install),
            "total_install": int(total_install)
        }
        
        if metrics:
            container = {k : container[k] for k in metrics}

        return container
    
    async def daily_growth(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            metrics: list = []):
        """
        Calculates the daily growth percentage for various install metrics compared to the previous week.

        Args:
            from_date (datetime.date): The start date for the current week's data.
            to_date (datetime.date): The end date for the current week's data.
            metrics (list): If metrics is given, it's filtering the returning metrics.
                - 'undetected_install'
                - 'android_organic'
                - 'apple_organic'
                - 'total_organic'
                - 'android_referrals'
                - 'fb_install'
                - 'google_install'
                - 'tiktok_install'
                - 'asa_install'
                - 'android_install'
                - 'apple_install'
                - 'total_install'

        Returns:
            dict: A dictionary containing the daily growth percentage for each install metric.
        """
        # Calculate date range for the previous week
        delta = (to_date - from_date) + timedelta(days=1)
        fromdate_lastweek = from_date - delta
        todate_lastweek = to_date - delta

        current_data = await self.overall_install(from_date=from_date, to_date=to_date, metrics=metrics)
        last_week_data = await self.overall_install(from_date=fromdate_lastweek, to_date=todate_lastweek, metrics=metrics)

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

    async def dataframe(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            group_by: str = "Media Source"):
        """
        Creates a pandas DataFrame summarizing install data from various sources, grouped by the specified column.

        Args:
            from_date (datetime.date): The start date for the install data query.
            to_date (datetime.date): The end date for the install data query.
            group_by (str, optional): The column to group the data by ("Media Source" or "date").

        Returns:
            pandas.DataFrame: A DataFrame containing the grouped install data.
        """
        df_google_install = await asyncio.to_thread(lambda: 
            self.df_google_install[(self.df_google_install["date"] >= from_date) & (self.df_google_install["date"] <= to_date)])
        df_facebook_install = await asyncio.to_thread(lambda: 
            self.df_facebook_install[(self.df_facebook_install["date"] >= from_date) & (self.df_facebook_install["date"] <= to_date)])
        df_tiktok_install = await asyncio.to_thread(lambda: 
            self.df_tiktok_install[(self.df_tiktok_install["date"] >= from_date) & (self.df_tiktok_install["date"] <= to_date)])
        df_asa_install = await asyncio.to_thread(lambda: 
            self.df_asa_install[(self.df_asa_install["date"] >= from_date) & (self.df_asa_install["date"] <= to_date)])
        df_organic = await asyncio.to_thread(lambda: 
            self.df_organic[(self.df_organic["date"] >= from_date) & (self.df_organic["date"] <= to_date)])
        df_undetected_refferals = await asyncio.to_thread(lambda: 
            self.df_undetected_refferals[(self.df_undetected_refferals["date"] >= from_date) & (self.df_undetected_refferals["date"] <= to_date)])

        # Final data preparation
        df_google = await asyncio.to_thread(lambda: df_google_install.loc[:, [f'{group_by}', 'google_install']])
        fb_df = await asyncio.to_thread(lambda: df_facebook_install.loc[:, [f'{group_by}', 'facebook_install']])
        df_tiktok = await asyncio.to_thread(lambda: df_tiktok_install.loc[:, [f'{group_by}', 'tiktok_install']])
        df_asa = await asyncio.to_thread(lambda: df_asa_install.loc[:, [f'{group_by}', 'asa_install']])
        df_organic = await asyncio.to_thread(lambda: df_organic.loc[:, [f'{group_by}', 'android_organic_install', 'apple_organic_install']])
        df_undetected_refferals = await asyncio.to_thread(lambda: df_undetected_refferals.loc[:, [f'{group_by}', 'undetected', 'refferals']])
        
        merge_1 = await asyncio.to_thread(lambda: pd.merge(fb_df, df_google, on=f'{group_by}', how='outer'))
        await asyncio.to_thread(lambda: merge_1.fillna(0, inplace=True))
        
        merge_2 = await asyncio.to_thread(lambda: pd.merge(df_asa, df_tiktok, on=f'{group_by}', how='outer'))
        await asyncio.to_thread(lambda: merge_2.fillna(0, inplace=True))
        
        merge_3 = await asyncio.to_thread(lambda: pd.merge(df_undetected_refferals, df_organic, on=f'{group_by}', how='outer'))
        await asyncio.to_thread(lambda: merge_3.fillna(0, inplace=True))
        
        df_merged = await asyncio.to_thread(lambda: pd.merge(merge_1, merge_2, on=f'{group_by}', how='outer'))
        await asyncio.to_thread(lambda: df_merged.fillna(0, inplace=True))
        
        full_merged = await asyncio.to_thread(lambda: pd.merge(df_merged, merge_3, on=f'{group_by}', how='outer'))
        await asyncio.to_thread(lambda: full_merged.fillna(0, inplace=True))
        
        full_merged['total_install'] = await asyncio.to_thread(
            lambda: (
                full_merged['facebook_install'].astype(int) + full_merged['google_install'].astype(int) + 
                full_merged['tiktok_install'].astype(int) + full_merged['asa_install'].astype(int) + 
                full_merged['undetected'].astype(int) + full_merged['android_organic_install'].astype(int) + 
                full_merged['refferals'].astype(int) + full_merged["apple_organic_install"]))
        
        df_group = await asyncio.to_thread(lambda: full_merged.groupby([f'{group_by}'])['total_install'].sum().reset_index())
        df_group["total_install"] = await asyncio.to_thread(lambda: df_group["total_install"].astype(int))
        
        return df_group
    
    async def install_source_table(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Calculate and visualize installs by source within a given date range.

        Parameters:
        from_date (datetime.date, optional): Start date for the data.
        to_date (datetime.date, optional): End date for the data.

        Returns:
        dict or str: Depending on the value of 'data' parameter, it returns either a chart JSON string, a DataFrame, or specific install data.
        """
        df = await self.dataframe(from_date=from_date, to_date=to_date, group_by="Media Source")
        await asyncio.to_thread(lambda: df.sort_values(by='total_install', ascending=False, inplace=True))

        # Create a table figure using Plotly
        fig = go.Figure(
            go.Table(
                header=dict(
                    fill_color="grey",
                    line_color="black",
                    font=dict(color='black'),
                    values=['Media Source', 'Installs'],
                    align='center'),
                cells=dict(
                    fill_color="white",
                    line_color="black",
                    font=dict(color="black"),
                    values=[
                        df['Media Source'], 
                        df['total_install'].apply(lambda x: "{:,.0f}".format((x)))],
                        align='center'))
            )
        fig.update_layout(title='Install Source')

        # Convert the figure to JSON
        chart = await asyncio.to_thread(lambda: json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder))

        return chart

    async def install_source_chart(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Create a pie chart showing the distribution of installs by source within a given date range.

        Parameters:
        from_date (datetime.date, optional): Start date for the data.
        to_date (datetime.date, optional): End date for the data.

        Returns:
        str: A JSON string representing the pie chart.
        """
        df = await self.dataframe(from_date=from_date, to_date=to_date)
        await asyncio.to_thread(lambda: df.sort_values(by='total_install', ascending=False, inplace=True))
        
        # Create a pie chart using Plotly
        fig = go.Figure(
            go.Pie(labels=df['Media Source'], values=df['total_install'])
        )
        fig.update_layout(title='New Install Source')

        # Convert the figure to JSON
        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        return chart
    
    async def aso_chart(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Creates a bar chart visualizing organic installs (Android and iOS) over the specified date range.

        Args:
            from_date (datetime.date): The start date for the install data query.
            to_date (datetime.date): The end date for the install data query.

        Returns:
            str: A JSON string representation of the Plotly bar chart.
        """
        df = await asyncio.to_thread(lambda: 
            self.df_organic[(self.df_organic["date"] >= from_date) & (self.df_organic["date"] <= to_date)])
        await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
        df["android_organic_install"] = await asyncio.to_thread(lambda: df["android_organic_install"].round(0))
        df["apple_organic_install"] = await asyncio.to_thread(lambda: df["apple_organic_install"].round(0))
        
        fig = go.Figure(data=[
            go.Bar(x=df['date'], y=df['android_organic_install'], name='Android', text=df['android_organic_install'].round(0), textposition='inside'),
            go.Bar(x=df['date'], y=df['apple_organic_install'], name='IOS', text=df['apple_organic_install'].round(0), textposition='outside')
        ])

        fig.update_layout(title='Organic Install Per Days', barmode='stack')
        fig.update_xaxes(title='Date', dtick='D1')
        fig.update_yaxes(title='Total Install')

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        return chart


class AcquisitionData:
    """
    This class is used to process and analyze acquisition data for a mobile application 
    from various advertising platforms, including Google Ads, Facebook Ads, TikTok Ads, 
    Apple Search Ads (ASA), and organic installs. It allows querying data based 
    on specific campaign names and date ranges.

    Attributes:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date for the install data query.
        to_date (datetime.date): The end date for the install data query.
        df_google (pandas.DataFrame, optional): A pandas DataFrame containing 
            processed Google Ads data (impressions, clicks, spend, installs), 
            optionally filtered by a list of campaign names. Defaults to None.
        df_facebook (pandas.DataFrame, optional): A pandas DataFrame containing 
            processed Facebook Ads data (impressions, clicks, spend, installs), 
            optionally filtered by a list of campaign names. Defaults to None.
        df_tiktok (pandas.DataFrame, optional): A pandas DataFrame containing 
            processed TikTok Ads data (impressions, clicks, spend, installs), 
            optionally filtered by a list of campaign names. Defaults to None.
        df_asa (pandas.DataFrame, optional): A pandas DataFrame containing 
            processed Apple Search Ads data (impressions, clicks, spend, installs). 
            Defaults to None.
    """
    def __init__(
            self, 
            session: AsyncSession, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Initializes an `AcquisitionData` object.

        Args:
            session (AsyncSession): The asynchronous SQLite session.
            from_date (datetime.date): The start date for the install data query.
            to_date (datetime.date): The end date for the install data query.
        """
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_google = pd.DataFrame()
        self.df_facebook = pd.DataFrame()
        self.df_tiktok = pd.DataFrame()
        self.df_asa = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: datetime.date, to_date: datetime.date):
        """
        Asynchronously loads and processes acquisition data for a given date range.

        Creates an `AcquisitionData` instance and fetches the acquisition data from multiple sources asynchronously 
        based on the specified date range.

        Args:
            session (AsyncSession): The asynchronous SQLAlchemy session used for database operations.
            from_date (datetime.date): The start date for filtering the install data.
            to_date (datetime.date): The end date for filtering the install data.

        Returns:
            AcquisitionData: An instance of `AcquisitionData` with the fetched data.
        """
        instance = cls(session, from_date, to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self):
        """
        Fetch the data from database.

        Parameters:
            from_date (datetime.date): The start date of data to fetch.
            to_date (datetime.date): The end date of data to fetch.
        """
        await self._read_db(data="google", from_date=self.from_date, to_date=self.to_date)
        await self._read_db(data="facebook", from_date=self.from_date, to_date=self.to_date)
        await self._read_db(data="tiktok", from_date=self.from_date, to_date=self.to_date)
        await self._read_db(data="asa", from_date=self.from_date, to_date=self.to_date)
        
    async def _read_db(
            self, 
            data: str, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            list_campaign: list = []):
        """
        Reads and processes acquisition data for a specific platform within the specified date range,
        optionally filtering by a list of campaign names.

        Args:
            data (str): The data source (e.g., "google", "facebook", "tiktok", "asa").
            from_date (datetime.date): The start date for the data query.
            to_date (datetime.date): The end date for the data query.
            list_campaign (list, optional): A list of campaign names to filter the data by. 
                An empty list (`[]`) retrieves data for all campaigns. Defaults to an empty list.

        Returns:
            pandas.DataFrame: A DataFrame containing the processed data with columns like 
                "date", "campaign_name", "impressions", "clicks", "spend", "install", 
                and "cost/install".

        Raises:
            ValueError: If an unsupported data source is provided.
        """
        if data == "google":
            campaign_name = list_campaign if list_campaign else ['UA - App Install - Android - ID (Major Cities)']
            query = select(
                GoogleAdsData.date.label("date"),
                GoogleAdsData.campaign_name.label("campaign_name"),
                func.sum(GoogleAdsData.impressions).label("impressions"),
                func.sum(GoogleAdsData.clicks).label("clicks"),
                func.sum(GoogleAdsData.spend).label("spend"),
                func.sum(GoogleAdsData.conversions).label("install")
            ).filter(
                func.date(GoogleAdsData.date).between(from_date, to_date),
                GoogleAdsData.campaign_name.in_(campaign_name)
            ).group_by(
                "date",
                "campaign_name"
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df = pd.DataFrame(data)
            if df.empty:
                df = pd.DataFrame({
                    "date": pd.date_range(self.to_date, self.to_date).date,
                    "campaign_name": ["-"],
                    "impressions": [0],
                    "clicks": [0],
                    "spend": [0],
                    "install": [0]
                })

            df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
            df["date"] = await asyncio.to_thread(lambda: df["date"].dt.date)
            df["cost/install"] = await asyncio.to_thread(lambda: df["spend"] / df["install"])
            self.df_google = df.copy()
        
        elif data == "facebook":
            campaign_name = list_campaign if list_campaign else ["AAA"]
            query = select(
                FacebookAdsData.date_start.label("date"),
                FacebookAdsData.campaign_name.label("campaign_name"),
                func.sum(FacebookAdsData.impressions).label("impressions"),
                func.sum(FacebookAdsData.clicks).label("clicks"),
                func.sum(FacebookAdsData.spend).label("spend"),
                func.sum(FacebookAdsData.unique_actions_mobile_app_install).label("install")
            ).filter(
                FacebookAdsData.date_start.between(from_date, to_date),
                FacebookAdsData.campaign_name.in_(campaign_name)
            ).group_by(
                "date",
                "campaign_name"
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df = pd.DataFrame(data)
            if df.empty:
                df = pd.DataFrame({
                    "date": pd.date_range(self.to_date, self.to_date).date,
                    "campaign_name": ["-"],
                    "impressions": [0],
                    "clicks": [0],
                    "spend": [0],
                    "install": [0]
                })

            df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
            df["date"] = await asyncio.to_thread(lambda: df["date"].dt.date)
            df["cost/install"] = await asyncio.to_thread(lambda: df["spend"] / df["install"])
            self.df_facebook = df.copy()

        elif data == "tiktok":
            campaign_name = list_campaign if list_campaign else ["UA - App Install - Android - ID"]
            query = select(
                TiktokAdsData.date.label("date"),
                TiktokAdsData.campaign_name.label("campaign_name"),
                func.sum(TiktokAdsData.impressions).label("impressions"),
                func.sum(TiktokAdsData.clicks).label("clicks"),
                func.sum(TiktokAdsData.spend).label("spend"),
                func.sum(TiktokAdsData.conversion).label("install")
            ).filter(
                TiktokAdsData.date.between(from_date, to_date),
                TiktokAdsData.campaign_name.in_(campaign_name)
            ).group_by(
                "date",
                "campaign_name"
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df = pd.DataFrame(data)
            if df.empty:
                df = pd.DataFrame({
                    "date": pd.date_range(self.to_date, self.to_date).date,
                    "campaign_name": ["-"],
                    "impressions": [0],
                    "clicks": [0],
                    "spend": [0],
                    "install": [0]
                })

            df["date"] = await asyncio.to_thread(pd.to_datetime, df["date"])
            df["date"] = await asyncio.to_thread(lambda: df["date"].dt.date)
            df["cost/install"] = await asyncio.to_thread(lambda: df["spend"] / df["install"])
            self.df_tiktok = df.copy()
        
        elif data == "asa":
            query = select(
                AsaData.date.label("date"),
                AsaData.campaign_name.label("campaign_name"),
                func.sum(AsaData.impressions).label("impressions"),
                func.sum(AsaData.taps).label("clicks"),
                func.sum(AsaData.local_spend * Currency.idr).label("spend"),
                func.sum(AsaData.installs).label("install")
            ).join(
                Currency, AsaData.date == Currency.date, isouter=True
            ).filter(
                AsaData.date.between(from_date, to_date)
            ).group_by(
                AsaData.date,
                "campaign_name"
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            df = pd.DataFrame(data)
            if df.empty:
                df = pd.DataFrame({
                    "date": pd.date_range(self.to_date, self.to_date).date,
                    "campaign_name": ["-"],
                    "impressions": [0],
                    "clicks": [0],
                    "spend": [0],
                    "install": [0]
                })

            df["date"]  = await asyncio.to_thread(pd.to_datetime, df["date"])
            df["date"]= await asyncio.to_thread(lambda: df["date"].dt.date)
            df["cost/install"] = await asyncio.to_thread(lambda: df["spend"] / df['install'])
            self.df_asa = df.copy()

    async def metrics(
            self, 
            data: str, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Calculates key metrics (impressions, clicks, spend, installs, cost per install) for a specific data source within the specified date range.

        Args:
            data (str): The data source (e.g., "google", "facebook", "tiktok", "asa").
            from_date (datetime.date): The start date for the data query.
            to_date (datetime.date): The end date for the data query.

        Returns:
            dict: A dictionary containing the calculated metrics for the specified data source.
        """
        df = self.__getattribute__(f"df_{data}")
        df = await asyncio.to_thread(
            lambda: df[
                (df["date"] >= from_date) &
                (df["date"] <= to_date)
            ]
        )

        metrics = await asyncio.gather(
            asyncio.to_thread(lambda: df["impressions"].sum()),
            asyncio.to_thread(lambda: df["clicks"].sum()),
            asyncio.to_thread(lambda: df["spend"].sum()),
            asyncio.to_thread(lambda: df["install"].sum()),
        )
        cost_install = await asyncio.to_thread(lambda: df["spend"].sum() / df["install"].sum()) if df["install"].sum() != 0 else 0

        container = {
            "impressions":metrics[0],
            "clicks": metrics[1],
            "spend": metrics[2],
            "install": metrics[3],
            "cost_install": round(cost_install, 0),
        }

        return container
    
    async def daily_growth(
            self, 
            data: str, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Calculates the daily growth percentage for key metrics (impressions, clicks, spend, installs, cost per install)
        for a specific data source compared to the previous week.

        Args:
            data (str): The data source (e.g., "google", "facebook", "tiktok", "asa").
            from_date (datetime.date): The start date for the current week's data.
            to_date (datetime.date): The end date for the current week's data.

        Returns:
            dict: A dictionary containing the daily growth percentage for each metric.
        """
        # Calculate date range for the previous week
        delta = (to_date - from_date) + timedelta(days=1)
        fromdate_lastweek = from_date - delta
        todate_lastweek = to_date - delta

        current_data = await self.metrics(data=data, from_date=from_date, to_date=to_date)
        last_week_data = await self.metrics(data=data, from_date=fromdate_lastweek, to_date=todate_lastweek)

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
    
    async def dataframe(
            self, 
            data: str, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Creates a pandas DataFrame containing the acquisition data for a specific data source, 
        grouped by date and campaign name.

        Args:
            data (str): The data source (e.g., "google", "facebook", "tiktok", "asa").
            from_date (datetime.date): The start date for the data query.
            to_date (datetime.date): The end date for the data query.

        Returns:
            dict: A dictionary containing two DataFrames:
                - `df_date`: A DataFrame grouped by date with columns for impressions, clicks, spend, installs, and cost per install.
                - `df_campaign_name`: The original DataFrame with all columns.
        """
        df = self.__getattribute__(f"df_{data}")
        df = await asyncio.to_thread(
            lambda: df[
                (df["date"] >= from_date) &
                (df["date"] <= to_date)
            ]
        )
        await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
        
        # Data group by date
        df_group_date = await asyncio.to_thread(lambda: df.groupby(["date"]).agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            spend=("spend", "sum"),
            install=("install", "sum")
        ).reset_index())
        df_group_date["cost/install"] = await asyncio.to_thread(lambda: df_group_date["spend"]/df_group_date["install"])
        await asyncio.to_thread(lambda: df_group_date.fillna(0, inplace=True))

        container = {
            "df_date": df_group_date.copy(),
            "df_campaign_name": df.copy()
        }

        return container
    

async def cost_install_chart(data: dict):
    """
    Creates a Plotly chart visualizing the cost and cost per install for a given data source.

    Args:
        data (dict): A dictionary containing a DataFrame with acquisition data grouped by date.
            - The DataFrame is expected to have columns like "date", "spend", and "cost/install".

    Returns:
        str: A JSON string representation of the Plotly chart.
    """
    df = data["df_date"]

    # Create a Bar trace for the cost data
    trace1 = go.Bar(
        x=df['date'],
        y=df['spend'],
        name='Cost',
        text=df['spend'].apply(lambda x: "Rp. {:,.0f}".format((x))),
        textposition='inside',
        yaxis='y'
    )

    # Create a Scatter trace for the cost per install data
    trace2 = go.Scatter(
        x=df['date'],
        y=df['cost/install'],
        name='Cost Per Install',
        yaxis='y2'
    )

    # Define the layout with a secondary y-axis
    layout = go.Layout(
        title='Cost To Install',
        yaxis=dict(
        title='Cost'
    ),
    yaxis2=dict(
        title='Cost To Install',
        overlaying='y',
        side='right'
        )
    )

    # Combine the traces and layout into a Figure object
    fig = go.Figure(data=[trace1, trace2], layout=layout)

    # Update the layout for better legend positioning and x-axis tick frequency
    fig.update_layout(legend=dict(
        yanchor="top",
        y=0.99,
        xanchor="left",
        x=0.01
    ))
    fig.update_xaxes(title='Date')

    # Convert the Figure object to a JSON representation for Plotly
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def ads_install_chart(data: dict):
    """
    Visualize Acquisition install data using a bar chart.

    Parameters:
    data (dict): A dictionary containing a DataFrame with acquisition data grouped by date.

    Returns:
    str: JSON representation of the Plotly chart.
    """

    # Get Facebook install data from the DataFrame for the specified date range
    df = data["df_date"]

    # Create a Bar trace for the install data
    fig = go.Figure(
        go.Bar(
            x=df['date'], 
            y=df['install'], 
            name='Install', 
            text=df['install'].apply(lambda x: "{:,.0f}".format((x))), 
            textposition='inside')
    )

    # Update layout with title and axis labels
    fig.update_layout(title='Install By Periods')
    fig.update_xaxes(title='Date')
    fig.update_yaxes(title='Total Install')

    # Convert the Figure object to a JSON representation for Plotly
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def campaign_details_table(data: dict):
    """
    Generate a table showing Facebook campaign data.

    Parameters:
    data (dict): A dictionary containing a DataFrame with acquisition data.

    Returns:
    str: JSON representation of the Plotly table.
    """

    # Read Facebook ads data from CSV file
    df = data["df_campaign_name"]

    # Create a Plotly table
    fig = go.Figure(
        go.Table(
            header=dict(
                fill_color="grey",
                line_color="black",
                font=dict(color="black"),
                values=list(df.columns),
                align='center'),
            cells=dict(
                fill_color="white",
                line_color="black",
                font=dict(color="black"),
                values=[
                    df['date'],
                    df['campaign_name'], 
                    df['impressions'].apply(lambda x: "{:,.0f}".format((x))), 
                    df['clicks'].apply(lambda x: "{:,.0f}".format((x))), 
                    df['spend'].apply(lambda x: "Rp. {:,.0f}".format((x))), 
                    df['install'].apply(lambda x: "{:,.0f}".format((x))), 
                    df['cost/install'].apply(lambda x: "Rp. {:,.0f}".format((x)))],
                align='center'))
        )
    fig.update_layout(title='Ads Campaign Details')
    
    # Convert the Figure object to a JSON representation for Plotly
    chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

    return chart


async def cost(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        data: str = "scalar"):
    """
    Calculate the overall cost incurred during a specified time period.

    This function retrieves cost data from various advertising platforms such as Facebook Ads, Google Ads, Apple Search Ads, and TikTok Ads.
    It then calculates the total spend by summing up the costs from each platform for the specified time period.

    Args:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date of the time period in 'YYYY-MM-DD' format. Defaults to None.
        to_date (datetime.date): The end date of the time period in 'YYYY-MM-DD' format. Defaults to None.
        data (str): The data to return ('scalar' or 'dataframe')

    Returns:
        int: The total cost incurred during the specified time period.

    Note:
        Requires data files for Facebook Ads, Google Ads, Apple Search Ads, and TikTok Ads.

    Example:
        cost(from_date='2023-01-01', to_date='2023-01-31')
    """
    google_campaign = [
        'UA - App Install - Android - ID (Major Cities)',
        'SEM - Brand',
        'SEM - Generic',
        'UA - GDN - Generic - Jan2024'
    ]
    google_ads_subquery = select(
        func.date(GoogleAdsData.date).label("date"),
        func.sum(func.round(GoogleAdsData.spend)).label("google_spend")
    ).filter(
        func.date(GoogleAdsData.date).between(from_date, to_date),
        GoogleAdsData.campaign_name.in_(google_campaign)
    ).group_by("date")
    google_ads = await session.execute(google_ads_subquery)
    google_ads_data = google_ads.fetchall()
    df_google_ads  = pd.DataFrame(google_ads_data)

    if df_google_ads.empty:
        df_google_ads = pd.DataFrame({
            "date": pd.date_range(to_date, to_date).date,
            "google_spend": [0]
        })

    facebook_campaign = [
        'AAA',
        'FB-BA_UA-Traffic_Web-ID-AON'
    ]
    facebook_ads_subquery = select(
        func.date(FacebookAdsData.date_start).label("date"),
        func.sum(func.round(FacebookAdsData.spend)).label("facebook_spend")
    ).filter(
        func.date(FacebookAdsData.date_start).between(from_date, to_date),
        FacebookAdsData.campaign_name.in_(facebook_campaign)
    ).group_by("date")
    facebook_ads = await session.execute(facebook_ads_subquery)
    facebook_ads_data = facebook_ads.fetchall()

    df_facebook_ads  = pd.DataFrame(facebook_ads_data)
    if df_facebook_ads.empty:
        df_facebook_ads = pd.DataFrame({
            "date": pd.date_range(to_date, to_date).date,
            "facebook_spend": [0]
        })

    tiktok_campaign = [
        'UA - App Install - Android - ID'
    ]
    tiktok_ads_subquery = select(
        func.date(TiktokAdsData.date).label("date"),
        func.sum(func.round(TiktokAdsData.spend)).label("tiktok_spend")
    ).filter(
        func.date(TiktokAdsData.date).between(from_date, to_date),
        TiktokAdsData.campaign_name.in_(tiktok_campaign)
    ).group_by("date")
    tiktok_ads = await session.execute(tiktok_ads_subquery)
    tiktok_ads_data = tiktok_ads.fetchall()
    df_tiktok_ads  = pd.DataFrame(tiktok_ads_data)

    if df_tiktok_ads.empty:
        df_tiktok_ads = pd.DataFrame({
            "date": pd.date_range(to_date, to_date).date,
            "tiktok_spend": [0]
        })

    asa_ads_subquery = select(
        func.date(AsaData.date).label("date"),
        func.sum(func.round(AsaData.local_spend * Currency.idr)).label("asa_spend")
    ).join(
        Currency, AsaData.date == Currency.date, isouter=True
    ).filter(
        func.date(AsaData.date).between(from_date, to_date)
    ).group_by(AsaData.date)
    asa_ads = await session.execute(asa_ads_subquery)
    asa_ads_data = asa_ads.fetchall()
    df_asa_ads  = pd.DataFrame(asa_ads_data)

    if df_asa_ads.empty:
        df_asa_ads = pd.DataFrame({
            "date": pd.date_range(to_date, to_date).date,
            "asa_spend": [0]
        })

    df_google_ads["date"] = await asyncio.to_thread(pd.to_datetime, df_google_ads["date"])
    df_google_ads["date"] = await asyncio.to_thread(lambda: df_google_ads["date"].dt.date)
    df_facebook_ads["date"] = await asyncio.to_thread(pd.to_datetime, df_facebook_ads["date"])
    df_facebook_ads["date"] = await asyncio.to_thread(lambda: df_facebook_ads["date"].dt.date)
    df_tiktok_ads["date"] = await asyncio.to_thread(pd.to_datetime, df_tiktok_ads["date"])
    df_tiktok_ads["date"] = await asyncio.to_thread(lambda: df_tiktok_ads["date"].dt.date)
    df_asa_ads["date"] = await asyncio.to_thread(pd.to_datetime, df_asa_ads["date"])    
    df_asa_ads["date"] = await asyncio.to_thread(lambda: df_asa_ads["date"].dt.date)
    
    df_group1 = pd.merge(df_google_ads, df_facebook_ads, on="date", how="outer")
    df_group1["date"]  = await asyncio.to_thread(pd.to_datetime, df_group1["date"])
    df_group1["date"] = await asyncio.to_thread(lambda: df_group1["date"].dt.date)
    df_group2 = pd.merge(df_tiktok_ads, df_asa_ads, on="date", how="outer")
    df_group2["date"] = await asyncio.to_thread(pd.to_datetime, df_group2["date"])
    df_group2["date"] = await asyncio.to_thread(lambda: df_group2["date"].dt.date)
    df = await asyncio.to_thread(lambda: pd.merge(df_group1, df_group2, on="date", how="outer"))

    await asyncio.to_thread(lambda: df.fillna(0, inplace=True))
    df["google_spend"] = await asyncio.to_thread(lambda: df["google_spend"].astype(int))
    df["facebook_spend"] = await asyncio.to_thread(lambda: df["facebook_spend"].astype(int))
    df["tiktok_spend"] = await asyncio.to_thread(lambda: df["tiktok_spend"].astype(int))
    df["asa_spend"] = await asyncio.to_thread(lambda: df["asa_spend"].astype(int))
    df["total_spend"] = await asyncio.to_thread(lambda: df["google_spend"] + df["facebook_spend"] + df["tiktok_spend"] + df["asa_spend"])
    await asyncio.to_thread(
        lambda: df.rename(columns={
            "google_spend": "google",
            "facebook_spend": "facebook",
            "tiktok_spend": "tiktok",
            "asa_spend": "asa"
        }, inplace=True))

    return df["total_spend"].sum() if data == "scalar" else df.copy()


async def dg_cost(
        session: AsyncSession, 
        from_date: datetime.date, 
        to_date: datetime.date, 
        data: str = "scalar"):
    """
    Calculate the daily growth of cost spent during a specified time period.

    This function calculates the percentage change in the total cost spent between two periods (current and previous).
    It takes the total cost spent in the current period and the total cost spent in the previous period, and then computes
    the daily growth rate as (current period cost - previous period cost) / current period cost.

    Args:
        session (AsyncSession): The asynchronous SQLite session.
        from_date (datetime.date): The start date of the time period in 'YYYY-MM-DD' format. Defaults to None.
        to_date (datetime.date): The end date of the time period in 'YYYY-MM-DD' format. Defaults to None.
        data (str): The data to return ('scalar' or 'dataframe')

    Returns:
        str: The daily growth rate of cost spent as a percentage formatted string.

    Example:
        dg_cost(from_date='2023-01-01', to_date='2023-01-31')
    """

    delta = (to_date - from_date)+timedelta(1)
    fromdate_lastweek = from_date - delta
    todate_lastweek = to_date - delta

    # Get value for week 1 and week 2 
    w1 = await cost(session=session,from_date=from_date, to_date=to_date, data=data)
    w2 = await cost(session=session,from_date=fromdate_lastweek, to_date=todate_lastweek, data=data)

    # Calculate daily growth rate
    if w2 == 0:
        dg = 0
    else:
        dg = (w1-w2)/w2

    txt = float(round(dg, 4))

    return txt
