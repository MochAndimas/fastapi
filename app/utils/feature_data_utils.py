import pandas as pd
import plotly
import json
import asyncio
from datetime import datetime, timedelta
import plotly.graph_objects as go
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Case, desc, select, func
from app.db.models.novel import GooddreamerNovel as gn
from app.db.models.acquisition import Ga4EventData
from app.db.models.user import Codes as c, CodeRedeem as cr, Illustrations as i 
from app.db.models.user import IllustrationTransaction as it, GooddreamerUserData as gud
from app.db.models.data_source import ModelHasSources as mhs, Sources as s


class RedeemCode:
    """
    Class to handle redemption code data from the database.

    This class provides methods to read and process redemption code and redeemed code data 
    from the database. It filters the data based on a given date range and provides functionalities 
    to add default values and process date columns.
    """
    def __init__(
            self, 
            session: AsyncSession, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Initializes the RedeemCode class.

        Args:
            session (AsyncSession): The asynchronous database session used for queries.
            from_date (datetime.date): Start date for data filtering.
            to_date (datetime.date): End date for data filtering.
        """
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_codes = pd.DataFrame()
        self.df_redeem = pd.DataFrame()

    @classmethod
    async def laod_data(cls, session: AsyncSession, from_date: datetime.date, to_date: datetime.date):
        """
        Asynchronously loads and processes Redeem Code data for a given date range.

        Creates an `RedeemCode` instance and fetches the RedeemCode data from multiple sources asynchronously 
        based on the specified date range.

        Args:
            session (AsyncSession): The asynchronous SQLAlchemy session used for database operations.
            from_date (datetime.date): The start date for filtering the RedeemCode data.
            to_date (datetime.date): The end date for filtering the RedeemCode data.

        Returns:
            RedeemCode: An instance of `RedeemCode` with the fetched data.
        """
        instance = cls(session, from_date, to_date)
        await asyncio.gather( 
            instance._read_db(types="codes"),
            instance._read_db(types="redeemed_code")
        )
        return instance

    async def _read_db(self, types: str):
        """
        Reads data from the database based on the given type.

        Args:
            types (str): Data type to query ('codes' or 'redeemed_code').
        """
        if types == "codes":
            # Query for code data with total redeemed count
            query = select(
                c.id.label("codes_id"),
                c.name.label("voucher_name"),
                c.code.label("voucher_code"),
                c.ads_coin_amount.label("adscoin_amount"),
                Case(
                    (c.user_type == 1, "All User"),
                    (c.user_type == 2, "New User"),
                    (c.user_type == 3, "New Customer"),
                    (c.user_type == 1, "Old Customer")
                ).label("user_type"),
                Case(
                    (c.type == 1, "One-For-all"),
                    (c.type == 2, "personal")
                ).label("voucher_type"),
                func.concat(func.date(c.start_date), " - ", func.date(c.end_date)).label("period"),
                func.count(cr.user_id).label("total_redeemed_code")
            ).join(
                cr, c.id == cr.code_id
            ).filter(
                c.active == 1
            ).group_by(
                "codes_id",
                "voucher_name",
                "voucher_code",
                "adscoin_amount",
                "user_type",
                "voucher_type",
                "period"
            )
        elif types == 'redeemed_code':
            # Query for redeemed code data within the date range
            query = select(
                cr.id.label("redeemed_code_id"),
                cr.user_id.label("user_id"),
                c.name.label("voucher_name"),
                c.code.label("voucher_code"),
                c.ads_coin_amount.label("adscoin_amount"),
                Case(
                    (c.user_type == 1, "All User"),
                    (c.user_type == 2, "New User"),
                    (c.user_type == 3, "New Customer"),
                    (c.user_type == 1, "Old Customer")
                ).label("user_type"),
                Case(
                    (c.type == 1, "One-For-all"),
                    (c.type == 2, "personal")
                ).label("voucher_type"),
                func.concat(func.date(c.start_date), " - ", func.date(c.end_date)).label("period"),
                func.date(cr.created_at).label("redeemed_date")
            ).join(
                c, cr.code_id == c.id
            ).filter(
                func.date(cr.created_at).between(self.from_date, self.to_date),
                c.active == 1
            )
        
        result = await self.session.execute(query)
        data = result.fetchall()
        df = pd.DataFrame(data)

        if types == "codes":
            self.df_codes = df.copy()
        elif types == "redeemed_code":
            self.df_redeem = df.copy()

    def _add_default_value(self):
        """
        Adds a default row of values to the DataFrame if it's empty.

        Args:
            types (str): Data type to query ("codes" or "redeemed_code").
        """
        if self.df_codes.empty:
            # Default values for the "codes" data type
            default_values = {
                'codes_id': 0,
                'voucher_name': '-',
                'voucher_code': '-',
                'adscoin_amount': 0,
                'user_type': '-',
                'voucher_type': '-',
                'period': '-',
                'total_redeemed_code': 0
            }
            self.df_codes = pd.DataFrame(default_values, index=[0])
        elif self.df_redeem.empty:
            # Default values for the "redeemed_code" data type,
            # including a date range for 'redeemed_date'
            default_values = {
                'redeemed_code_id': 0,
                'user_id': 0,
                'voucher_name': '-',
                'voucher_code': '-',
                'adscoin_amount': 0,
                'user_type': '-',
                'voucher_type': '-',
                'period': '-',
                'redeemed_date': pd.date_range(self.to_date, self.to_date).date
            }
            self.df_redeem = pd.DataFrame(default_values, index=[0])

    async def _process_date_column(self, types: str):
        """
        Processes date columns in the DataFrame, specifically for 'redeemed_code' data.

        Args:
            types (str): Data type to query ("codes" or "redeemed_code").
        """
        date_columns = ['redeemed_date']  # Columns to process
        if types == 'redeemed_code':
            # Convert to date objects for daily analysis
            for col in date_columns:
                self.df_redeem[col] = await asyncio.to_thread(pd.to_datetime, self.df_redeem[col])
                self.df_redeem[col] = await asyncio.to_thread(lambda: self.df_redeem[col].dt.date)
        else:
            pass
    
    async def redeemed_details(
            self, 
            types: str, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            data: str = "metrics",
            codes: str = "", 
            user_type: str = ""):
        """
        Retrieves specific details about redeemed codes based on given criteria.

        This method filters and aggregates data related to redeemed codes based on parameters 
        like date range, voucher code, platform, and user type. It can return the filtered DataFrame,
        the count of redeemed codes, or the total amount of redeemed adscoins.

        Args:
            types (str): Data type to query ("codes" or "redeemed_code").
            from_date (datetime.date): Start date for data filtering.
            to_date (datetime.date): End date for data filtering.
            data (str): The data to fetch ('metrics' or 'dataframe').
            codes (str, optional): Voucher code to filter by (default is "").
            user_type (str, optional): User type to filter by (default is "").

        Returns:
            pandas.DataFrame, int, or float: 
                - DataFrame if `data` is "df" (filtered DataFrame).
                - Integer if `data` is "redeemed_count" (count of redeemed codes).
                - Float if `data` is "redeemed_adscoin" (sum of redeemed adscoins).
        """

        # Add default values if DataFrame is empty
        self._add_default_value()

        # Process date columns
        await self._process_date_column(types)

        # Filter data by date range
        df_read = await asyncio.to_thread(
            lambda: self.df_redeem[
                (self.df_redeem['redeemed_date'] >= from_date) & 
                (self.df_redeem['redeemed_date'] <= to_date)].copy())

        # Optional filtering
        if codes != '':
            df_read = await asyncio.to_thread(lambda: df_read[df_read['voucher_code'] == codes])
        if user_type != '':  # Currently unused, but you can implement it later
            df_read = await asyncio.to_thread(lambda: df_read[df_read['user_type'] == user_type])

        # Return requested data type
        if data == "df":
            df = df_read.copy()
        elif data == "metrics":
            df = {
                "redeemed_count": int(df_read['redeemed_code_id'].count()),
                "redeemed_adscoin": int(df_read['adscoin_amount'].sum()),
            }
        
        return df
    
    async def daily_growth(
            self, 
            types: str, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            codes: str = "", 
            user_type: str = ""):
        """
        Calculates the daily growth rate of redeemed codes or adscoins for a specific period.

        This method compares the value of the given data type (redeemed count or adscoin amount) 
        within a specified date range with the same data type in the previous period of the same length.
        It returns the growth rate as a percentage string.

        Args:
            types (str): Data type to query ("codes" or "redeemed_code").
            from_date (date or str): Start date of the current period.
            to_date (date or str): End date of the current period.
            codes (str, optional): Voucher code to filter by (default is "").
            user_type (str, optional): User type to filter by (default is "").

        Returns:
            str: Daily growth rate formatted as a percentage string (e.g., "25%"). If the previous period's 
                 value is zero, it returns "0".
        """
        # Calculate the start and end dates for the previous period
        delta = (to_date - from_date) + timedelta(1)
        fromdate_lastweek = from_date - delta
        todate_lastweek = to_date - delta

        # Get the values for the current and previous periods using redeemed_details method
        current_data = await self.redeemed_details(types=types, from_date=from_date, to_date=to_date, codes=codes, user_type=user_type)
        last_week_data = await self.redeemed_details(types=types, from_date=fromdate_lastweek, to_date=todate_lastweek, codes=codes, user_type=user_type)

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

    async def chart_table(
            self, 
            types: str, 
            from_date: datetime.date = None, 
            to_date: datetime.date = None, 
            codes: str = "", 
            user_type: str = "") -> str:
        """
        Generates an interactive table chart of redeemed code details.

        This method creates a Plotly table chart based on the specified data type (`codes` or `redeemed_code`).
        For 'redeemed_code', it allows filtering by date range, voucher code, and user type. The table displays
        relevant information such as voucher name, code, amount, and redemption details.

        Args:
            types (str): Data type to display ("codes" or "redeemed_code").
            from_date (date or str, optional): Start date for filtering (only for "redeemed_code").
            to_date (date or str, optional): End date for filtering (only for "redeemed_code").
            codes (str, optional): Voucher code to filter by (default is "").
            user_type (str, optional): User type to filter by (default is "").

        Returns:
            str: A JSON string representing the Plotly table chart configuration.
        """
        if types == 'codes':
            df = self.df_codes.copy()
            fig = go.Figure(
                go.Table(
                    header=dict(
                        fill_color="grey",
                        line_color="black",
                        font=dict(color="black"),
                        values=[
                            'Id',
                            'Voucher Name',
                            'Voucher Code',
                            'AdsCoin Amount',
                            'User Type',
                            'Voucher type',
                            'Period',
                            'Total Redeemed Code'
                        ]
                    ),
                    cells=dict(
                        fill_color="white",
                        line_color="black",
                        font=dict(color="black"),
                        values=[
                            df['codes_id'],
                            df['voucher_name'],
                            df['voucher_code'],
                            df['adscoin_amount'],
                            df['user_type'],
                            df['voucher_type'],
                            df['period'],
                            df['total_redeemed_code']
                        ]
                    )
                )
            )
            fig.update_layout(title='Redeemed Code Details')   
            chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)
        elif types == 'redeemed_code':
            df = await self.redeemed_details(types=types, from_date=from_date, to_date=to_date, data="df", codes=codes, user_type=user_type)
            print(df)
            fig = go.Figure(
                go.Table(
                    header=dict(
                        fill_color="grey",
                        line_color="black",
                        font=dict(color="black"),
                        values=[
                            'Id',
                            'User Id',
                            'Voucher Name',
                            'Voucher Code',
                            'AdsCoin Amount',
                            'User Type',
                            'Voucher Type',
                            'Period',
                            'Redeemed Date'
                        ]
                    ),
                    cells=dict(
                        fill_color="white",
                        line_color="black",
                        font=dict(color="black"),
                        values=[
                            df['redeemed_code_id'],
                            df['user_id'],
                            df['voucher_name'],
                            df['voucher_code'],
                            df['adscoin_amount'],
                            df['user_type'],
                            df['voucher_type'],
                            df['period'],
                            df['redeemed_date']
                        ]
                    )
                )
            )
            fig.update_layout(title='Redeemed Code Details')
            chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        return chart


class TransactionIllustration:
    """
    This class fetches and processes illustration transaction data from a database.
    """
    def __init__(
            self, 
            session: AsyncSession, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Initializes the object with date filters and the type of data to fetch.

        Args:
            session (AsyncSession): The asynchronous database session used for queries.
            from_date: Start date for filtering transactions.
            to_date: End date for filtering transactions.
        Raises:
            ValueError: If `types` is not a valid option.
        """
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_illustration = pd.DataFrame()
        self.df_transaction = pd.DataFrame()

    @classmethod
    async def laod_data(cls, session: AsyncSession, from_date: datetime.date, to_date: datetime.date):
        """
        Asynchronously loads and processes Redeem Code data for a given date range.

        Creates an `TransactionIllustration` instance and fetches the TransactionIllustration data from multiple sources asynchronously 
        based on the specified date range.

        Args:
            session (AsyncSession): The asynchronous SQLAlchemy session used for database operations.
            from_date (datetime.date): The start date for filtering the TransactionIllustration data.
            to_date (datetime.date): The end date for filtering the TransactionIllustration data.

        Returns:
            TransactionIllustration: An instance of `TransactionIllustration` with the fetched data.
        """
        instance = cls(session, from_date, to_date)
        await asyncio.gather( 
            instance._read_db(types="illustration"),
            instance._read_db(types="illustration_details")
        )
        return instance

    async def _read_db(self, types: str):
        """
        Fetches data from the database based on the specified type.

        Args:
            types: Type of data to fetch.
                - 'illustration'
                - 'illustration_details'

        Returns:
            A Pandas DataFrame containing the fetched data.
        """
        valid_types = ['illustration', 'illustration_details']
        if types not in valid_types:
            raise ValueError(f"Invalid types: {types}. Valid options are: {valid_types}")

        if types == 'illustration':
            query = select(
                it.illustration_id.label("illustration_id"),
                i.novel_id.label("novel_id"),
                gn.novel_title.label("novel_title"),
                i.title.label("illustration_title"),
                i.price.label("price"), 
                s.name.label('source'),
                func.count(it.user_id).label("total_transaction")
            ).join(
                mhs, it.id == mhs.model_id
            ).join(
                s, mhs.source_id == s.id
            ).join(
                i, it.illustration_id == i.id
            ).join(
                gn, i.novel_id == gn.id
            ).filter(
                mhs.model_type == 'App\\Models\\IllustrationTransaction',
            ).group_by(
                "illustration_id",
                "novel_id",
                "novel_title",
                "illustration_title",
                "price",
                "source"
            ).order_by(desc("total_transaction"))
        elif types == 'illustration_details':
            query = select(
                it.id.label("transaction_id"),
                gud.id.label("user_id"),
                it.illustration_id.label("illustration_id"),
                i.title.label("illustration_title"),
                i.novel_id.label("novel_id"),
                gn.novel_title.label("novel_title"),
                s.name.label("source"),
                it.created_at.label("transaction_date")
            ).join(
                mhs, it.id == mhs.model_id
            ).join(
                s, mhs.source_id == s.id
            ).join(
                i, it.illustration_id == i.id
            ).join(
                gud, it.user_id == gud.id
            ).join(
                gn, i.novel_id == gn.id
            ).filter(
                mhs.model_type == 'App\\Models\\IllustrationTransaction',
                func.date(it.created_at).between(self.from_date, self.to_date)
            ).order_by(desc("transaction_date"))
        
        result = await self.session.execute(query)
        data = result.fetchall()
        df = pd.DataFrame(data)

        if types == "illustration":
            self.df_illustration  = df.copy()
        elif types == "illustration_details":
            self.df_transaction = df.copy()
    
    def _add_default_value(self):
        """
        Add a dedault values if the dataframe is empty
        """
        if self.df_illustration.empty:
            default_values = {
                'illustration_id': 0,
                'novel_id': 0,
                'novel_title': '-',
                'illustration_title': '-',
                'price': 0,
                'source': '-',
                'total_transaction': 0
            }
            self.df_illustration = pd.DataFrame(default_values, index=[0])
        elif self.df_transaction.empty:
            default_values = {
                'transaction_id': 0,
                'user_id': 0,
                'illustration_id': 0,
                'illustration_title': '-',
                'novel_id': 0,
                'novel_title': '-',
                'source': '-',
                'transaction_date' : pd.date_range(self.to_date, self.to_date).date
            }

            self.df_transaction = pd.DataFrame(default_values, index=[0])

    async def _process_date_column(self, types: str):
        """
        Adds a default row to the DataFrame if it's empty, depending on the data type.

        Args:
            types: The type of data ('illustration' or 'illustration_details').
        """

        date_columns = ['transaction_date']  # Columns to process
        if types == 'illustration_details':
            self.df_transaction['novel_title'] = self.df_transaction['novel_title'].str.lower()
            # Convert to date objects for daily analysis
            for col in date_columns:
                self.df_transaction[col] = await asyncio.to_thread(pd.to_datetime, self.df_transaction[col])
                self.df_transaction[col] = await asyncio.to_thread(lambda: self.df_transaction[col].dt.date)
        else:
            pass

    async def transaction_details(
            self, 
            types: str, 
            data: str, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            source: str = '', 
            novel_title: str = '', 
            illustration_id: int = 0):
        """
        Filters and aggregates transaction data based on specified criteria.

        Args:
            types: The type of data ('illustration' or 'illustration_details').
            data: The type of aggregation or data to return.
                - 'df': Returns the entire filtered DataFrame.
                - 'metrics': return a dict of multiple metrics
            from_date: Start date for filtering transactions.
            to_date: End date for filtering transactions.
            source: (Optional) Filter by source name.
            novel_title: (Optional) Filter by novel title.
            illustration_id: (Optional) Filter by illustration ID.

        Returns:
            Either a Pandas DataFrame (if data='df') or an integer representing the calculated value.

        Raises:
            ValueError: If an invalid `data` value is provided.
        """
        self._add_default_value()
        await self._process_date_column(types)
        
        df_read = self.df_transaction[(self.df_transaction['transaction_date'] >= from_date) & (self.df_transaction['transaction_date'] <= to_date)]
        if source != '':
            df_read = df_read[df_read['source'] == source]
        if novel_title != '':
            df_read = df_read[df_read['novel_title'] == novel_title]
        if illustration_id != 0:
            df_read = df_read[df_read['illustration_id'] == illustration_id]
        
        if data == 'df':
            df = df_read.copy()
        if data == "metrics":
            df = {
                "transaction_unique": int(df_read['user_id'].nunique()),
                "transaction_count": int(df_read['user_id'].count()),
                "transaction_by_novel": int(df_read['novel_id'].nunique()),
                "total_transaction": int(df_read['transaction_id'].count()),
            }

        return df
    
    async def daily_growth(
            self, 
            types: str,
            from_date: datetime.date, 
            to_date: datetime.date, 
            source: str = '', 
            novel_title: str = '', 
            illustration_id: int = 0):
        """
        Calculates the daily growth rate of transactions for the given period and filters.

        Args:
            types: The type of data ('illustration' or 'illustration_details').
            from_date: Start date for the current period.
            to_date: End date for the current period.
            source: (Optional) Filter by source name.
            illustration_id: (Optional) Filter by illustration ID.

        Returns:
            A string representing the daily growth rate as a percentage (e.g., "25%").

        Raises:
            ValueError: If `data` is not one of the allowed values.
        """
        # Calculate the dates for the previous period (last week)
        delta = (to_date - from_date) + timedelta(1)
        fromdate_lastweek = from_date - delta
        todate_lastweek = to_date - delta
        
        # get value for week 1 and week 2
        current_data = await self.transaction_details(types=types, data="metrics", from_date=from_date, to_date=to_date, source=source, novel_title=novel_title, illustration_id=illustration_id)
        last_week_data = await self.transaction_details(types=types, data="metrics", from_date=fromdate_lastweek, to_date=todate_lastweek, source=source, novel_title=novel_title, illustration_id=illustration_id)

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
    
    async def table_chart(
            self, 
            types: str, 
            from_date: datetime.date = None, 
            to_date: datetime.date = None, 
            source: str = '', 
            novel_title: str = '', 
            illustration_id: int = 0):
        """
        Generates a Plotly table chart for illustration transactions, either overall or detailed.

        Args:
            types: The type of data ('illustration' or 'illustration_details').
            from_date: (Optional, for 'illustration_details' only) Start date for filtering.
            to_date: (Optional, for 'illustration_details' only) End date for filtering.
            source: (Optional) Filter by source name.
            novel_title: (Optional) Filter by novel title.
            illustration_id: (Optional) Filter by illustration ID.

        Returns:
            A JSON string representation of the Plotly Figure object.
        """
        if types == 'illustration':
            df = self.df_illustration.copy()
            fig = go.Figure(
                go.Table(
                    header=dict(
                        fill_color="grey",
                        line_color="black",
                        font=dict(color="black"),
                        values=[
                            "Illustration Id",
                            "Novel Id",
                            "Novel Title",
                            "Illustration Title",
                            "Price",
                            "Source",
                            "Total Transaction"
                        ]),
                    cells=dict(
                        fill_color="white",
                        line_color="black",
                        font=dict(color="black"),
                        values=[
                            df["illustration_id"],
                            df['novel_id'],
                            df['novel_title'],
                            df['illustration_title'],
                            df['price'],
                            df['source'],
                            df['total_transaction']
                        ])
                )
            )
            fig.update_layout(title="Overall Illustration Transaction")
            chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        elif types == 'illustration_details':
            df = await self.transaction_details(types=types, data="df", from_date=from_date, to_date=to_date, source=source, novel_title=novel_title, illustration_id=illustration_id)
            fig = go.Figure(
                go.Table(
                    header=dict(
                        fill_color="grey",
                        line_color="black",
                        font=dict(color="black"),
                        values=[
                            "Transaction Id",
                            "User Id",
                            "Illustration Id",
                            "Illustration Title",
                            "Novel Id",
                            "Novel Title",
                            "Source",
                            "Transaction Date"
                        ]),
                    cells=dict(
                        fill_color="white",
                        line_color="black",
                        font=dict(color="black"),
                        values=[
                            df['transaction_id'],
                            df['user_id'],
                            df['illustration_id'],
                            df['illustration_title'],
                            df['novel_id'],
                            df['novel_title'],
                            df['source'],
                            df['transaction_date']
                        ])
                )
            )
            fig.update_layout(title="Illustration Transaction Details")
            chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        return chart


class GoogleEventData:
    """
    A class to process and analyze Google Analytics event data.
    """

    def __init__(
            self, 
            session: AsyncSession, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Initialize the class and read the CSV data.

        Args:
            session (AsyncSession): The asynchronous SQLite session used for queries.
            from_date(datetime.date): The start date of the data you want to fetch.
            to_date_date(datetime.date): The end date of the data you want to fetch.
        """
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_read = pd.DataFrame()

    @classmethod
    async def laod_data(cls, session: AsyncSession, from_date: datetime.date, to_date: datetime.date):
        """
        Asynchronously loads and processes Redeem Code data for a given date range.

        Creates an `GoogleEventData` instance and fetches the GoogleEventData data from multiple sources asynchronously 
        based on the specified date range.

        Args:
            session (AsyncSession): The asynchronous SQLAlchemy session used for database operations.
            from_date (datetime.date): The start date for filtering the GoogleEventData data.
            to_date (datetime.date): The end date for filtering the GoogleEventData data.

        Returns:
            GoogleEventData: An instance of `GoogleEventData` with the fetched data.
        """
        instance = cls(session, from_date, to_date)
        await instance._read_data()
        return instance

    async def _read_data(self):
        """
        Reads the CSV data, parses dates, and sorts by date.

        Returns:
            pd.DataFrame: The processed DataFrame.
        """
        try:
            query = select(
                Ga4EventData.date.label("date"),
                Ga4EventData.platform.label("platform"),
                Ga4EventData.event_name.label("eventName"),
                Ga4EventData.event_count.label("eventCount"),
                Ga4EventData.total_user.label("totalUsers")
            ).filter(
                func.date(Ga4EventData.date).between(self.from_date, self.to_date)
            )
            result = await self.session.execute(query)
            data = result.fetchall()
            
            df = pd.DataFrame(data)
            if df.empty:
                default_values = {
                    'date': pd.date_range(self.to_date, self.to_date).date,
                    'platform': '-',
                    'eventName': '-',
                    'eventCount': 0,
                    'totalUsers': 0
                }
                df = pd.DataFrame(default_values, index=[0])

            df['date'] = await asyncio.to_thread(pd.to_datetime, df['date'])
            df['date'] = await asyncio.to_thread(lambda: df['date'].dt.date)
            await asyncio.to_thread(lambda: df.sort_values(by='date', ascending=True, inplace=True))
            self.df_read = df.copy()
        except Exception as e:
            raise Exception(f"Error reading data: {e}")

    async def get_data(
            self, 
            data: str, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Retrieves the specified data within the given date range.

        Args:
            data (str): Type of data to retrieve ('df', 'metrics').
            from_date (datetime.date): Start date of the data.
            to_date (datetime.date): End date of the data.

        Returns:
            pd.DataFrame or int: The filtered DataFrame, total unique users, or total event count.
        """
        dataframe = self.df_read.copy()
        dataframe = await asyncio.to_thread(lambda: dataframe[(dataframe["date"] >= from_date) & (dataframe["date"] <= to_date)])
        unique_user = await asyncio.to_thread(lambda: dataframe['totalUsers'].sum())
        event_count = await asyncio.to_thread(lambda: dataframe['eventCount'].sum())
        
        if data == 'df':
            df = dataframe.copy()
        elif data == 'metrics':
            df = {
                "unique_user": int(unique_user),
                "event_count": int(event_count)
            }

        return df
    
    async def daily_growth(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date):
        """
        Calculates the daily growth rate of the specified data type between two date ranges.

        Args:
            from_date (datetime.date): The start date of the first period.
            to_date (datetime.date): The end date of the first period.

        Returns:
            str: The daily growth rate as a percentage string (e.g., "15%").
        """
        delta = (to_date - from_date) + timedelta(1)
        fromdate_lastweek = from_date - delta
        todate_lastweek = to_date - delta
        
        # get value for week 1 and week 2
        current_data = await self.get_data(data="metrics", from_date=from_date, to_date=to_date)
        last_week_data = await self.get_data(data="metrics", from_date=fromdate_lastweek, to_date=todate_lastweek)

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
    
    async def bar_chart(
            self, 
            from_date: datetime.date, 
            to_date: datetime.date, 
            types: str = "Unique"):
        """
        Generates an interactive bar chart showing event counts and unique users for each event type 
        within a specified date range.

        Args:
            from_date (datetime.date): The start date of the period.
            to_date (datetime.date): The end date of the period.
            types (str): The types of the bar chart to fetch ('Unique' or 'Count').

        Returns:
            str: A JSON string representation of the Plotly figure.
        """
        df = await self.get_data(data='df', from_date=from_date, to_date=to_date)
        df = await asyncio.to_thread(lambda: df.groupby(['date', 'eventName']).agg(
            eventCount=('eventCount','sum'), 
            totalUsers=('totalUsers','sum')).reset_index())

        fig = go.Figure(data=[
            go.Bar(
                x=df['date'],
                y=df['totalUsers'].apply(lambda x: "{:,.0f}".format(x)) if types == "Unique" else df["eventCount"].apply(lambda x: "{:,.0f}".format(x)),
                name=f'Users ({types})',
                text=df['totalUsers'].apply(lambda x: "{:,.0f}".format(x)) if types == "Unique" else df["eventCount"].apply(lambda x: "{:,.0f}".format(x)),
                textposition='inside'
            )
        ])
        fig.update_layout(title=f'Users ({types}) Download Chapter')
        fig.update_xaxes(title='Period', dtick='D1')
        fig.update_yaxes(title='Value')

        chart = await asyncio.to_thread(json.dumps, fig, cls=plotly.utils.PlotlyJSONEncoder)

        return chart
