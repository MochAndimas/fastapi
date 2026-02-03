import datetime, csv, requests, json, os
import pandas as pd
import yfinance as yf
from decouple import config
from sqlalchemy import func, select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from apscheduler.triggers.cron import CronTrigger
from google.ads.googleads.client import GoogleAdsClient
from authlib.jose import jwt
from Crypto.PublicKey import ECC
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Filter, FilterExpression
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun

from app.db.models.acquisition import Currency, Ga4EventData, GoogleAdsData, AsaData, AdmobReportData
from app.db.models.acquisition import AdsenseReportData, FacebookAdsData, Ga4AnalyticsData, Ga4SessionsData
from app.db.models.acquisition import Ga4LandingPageData, TiktokAdsData, Ga4ActiveUserData
from app.db.session import get_sqlite


async def usd_idr_to_csv(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    Fetches and stores historical exchange rate data for USD to IDR into a database.

    This asynchronous function retrieves historical data for USD to IDR using `yfinance`, processes it, and stores it in a database. 
    The data is fetched for a specified date range and missing values are forward-filled. The function supports two modes, 'auto' and 'manual', 
    for managing the data in the database.

    Args:
        start_date (datetime.date): The starting date for fetching the historical data.
        end_date (datetime.date): The ending date for fetching the historical data.
        session (AsyncSession, optional): An existing SQLAlchemy AsyncSession for interacting with the database. If not provided, a new session will be created.
        types (str, optional): The mode of operation. Defaults to 'manual'. Possible values are:
            - 'manual': Deletes and replaces existing records within the specified date range.
            - 'auto': Checks if the data already exists for the end date. If it does, no updates are made. If not, it deletes existing records within the date range and inserts new data.

    Returns:
        str: A message indicating the result of the operation:
            - 'Data is up-to-date!': If in 'auto' mode and the data already exists.
            - 'Data successfully retrieved!': If new data was successfully fetched and stored.

    Example:
        >>> await usd_idr_to_csv(
                start_date=datetime.date(2024, 1, 1), 
                end_date=datetime.date(2024, 1, 31), 
                session=async_session, 
                types="manual"
            )
        # Fetches USD to IDR data for January 2024 and updates the database.

    Notes:
        - This function uses `yfinance` to fetch the historical data.
        - Missing values in the exchange rate data are forward-filled.
        - Data is stored in the `Currency` table, with columns for the date and exchange rate (IDR).
        - If a session is not provided, a new async session is created from the `get_sqlite` generator.
    """
    # Fetch historical data for USD to IDR
    data = yf.download("USDIDR=X", start=start_date, end=end_date)
    
    # Generate a full date range between start_date and end_date
    full_date_range = pd.date_range(start=start_date, end=end_date).date  # 'B' means business days
    
    # Reindex the data to include all dates in the full date range
    data = data.reindex(full_date_range).reset_index()
    
    # Forward fill missing data
    data.ffill(inplace=True)

    # Save the data to a CSV file
    data = data.loc[:, ['Date', 'Adj Close']]
    data.rename(columns={'Adj Close': 'IDR'}, inplace=True)
    data.fillna(data['IDR'].mean(), inplace=True)
    data['IDR'] = data['IDR'].astype(int)

    data['Date'] = pd.to_datetime(data['Date'])
    data['Date']= data['Date'].dt.date

    if session is None:
        # Extracting the session from the async generator
        async_gen = get_sqlite()
        session = await anext(async_gen)
    
    # Begin async session lifecycle
    if session is not None:
        query = select(Currency).filter_by(date=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                # Delete existing records in the date range
                await session.execute(
                    delete(Currency).where(Currency.date.between(start_date, end_date))
                )
                
                # Insert new data in bulk
                currency_data = [
                    Currency(date=row[0], idr=row[1]) for row in data.values
                ]
                session.add_all(currency_data)
                await session.commit()
                await session.close()
                return "Data successfully retrieved!"

        elif types == "manual":
            # Delete existing records in the date range
            await session.execute(
                delete(Currency).where(Currency.date.between(start_date, end_date))
            )
            
            # Insert new data in bulk
            currency_data = [
                Currency(date=row[0], idr=row[1]) for row in data.values
            ]
            session.add_all(currency_data)
            await session.commit()
            await session.close()
            return "Data successfully retrieved!"


async def ga4_event_data(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types="manual"):
    """
    Pull active user data from Google Analytics 4 and store it in a database.

    This function retrieves data for active users from Google Analytics 4 for the specified
    view ID and date range. The data includes dimensions such as date and platform, along with
    the metrics active 1-day users and active 28-day users. The fetched data is then appended
    to a database.

    Parameters:
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: A message indicating whether the data was updated or is already up-to-date.
    """
    # Calculate the last 1 day date
    last1days = datetime.datetime.today() - datetime.timedelta(1)
    last1days_date = last1days.date()

    # Create the credentials object from the refresh token
    credentials = Credentials.from_authorized_user_info(info={
        'refresh_token': config("GA4_REFRESH_TOKEN", cast=str),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': config("GA4_CLIENT_ID", cast=str),
        'client_secret': config("GA4_CLIENT_SECRET", cast=str)
    })

    # Create the client object
    client = BetaAnalyticsDataClient(credentials=credentials)
    view_id = config("GA4_VIEW_ID", cast=str)

    # Define the report request
    request = RunReportRequest(
        property=f"properties/{view_id}",
        dimensions=[
            Dimension(name="date"),          
            Dimension(name="platform"),
            Dimension(name="eventName") #  Dimension(name="customEvent:download_chapter_name") -> change to this if params firebase available
        ],
        metrics=[
            Metric(name="eventCount"),
            Metric(name="totalUsers")
        ],
        date_ranges=[DateRange(start_date=f"{start_date}", end_date=f"{end_date}")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(match_type='EXACT', value="user_download_chapter"), 
            )
        ),
    )
    
    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)
    
    # Use SQLite to log daily Google Ads data
    if session is not None:
        query = select(Ga4EventData).filter_by(date=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                response = client.run_report(request)
                # delete row data
                await session.execute(
                    delete(Ga4EventData).filter(Ga4EventData.date.between(start_date, end_date))
                )
                
                # Insert new data in bulk
                data = [
                    Ga4EventData(
                        date = datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d"),
                        platform = row.dimension_values[1].value,
                        event_name = row.dimension_values[2].value,
                        event_count = row.metric_values[0].value,
                        total_user = row.metric_values[1].value
                    ) for row in response.rows
                ]
                session.add_all(data)
                await session.commit()
                await session.close()
                
                return 'Data is being updated!'
        
        elif types == "manual":
            response = client.run_report(request)
            # delete row data
            await session.execute(
                delete(Ga4EventData).filter(Ga4EventData.date.between(start_date, end_date))
            )
            
            # Insert new data in bulk
            data = [
                Ga4EventData(
                    date = datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d"),
                    platform = row.dimension_values[1].value,
                    event_name = row.dimension_values[2].value,
                    event_count = row.metric_values[0].value,
                    total_user = row.metric_values[1].value
                ) for row in response.rows
            ]
            session.add_all(data)
            await session.commit()
            await session.close()
                
            return 'Data is being updated!'


async def google_reporting(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    Retrieve Google Ads performance data via Google Ads API and store it in a CSV file.

    This function queries Google Ads API to retrieve performance data for campaigns over the last 90 days.
    It then checks if the data for the last 1 day is already present in the CSV file. If not, it appends the
    newly fetched data to the CSV file.

    Args:
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: A message indicating whether the data was updated or is already up-to-date.

    Example:
        google_reporting(client)
    """
    
    last1days = datetime.datetime.today() - datetime.timedelta(1)
    last1days_date = last1days.date()

    client = GoogleAdsClient.load_from_storage('googleads.yaml', version='v17')
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
        segments.date,
        campaign.id,
        campaign.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'"""
    
    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.query = query
    search_request.customer_id = '9211907209'

    # Use SQLite to log daily Google Ads data
    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)

    if session is not None:
        query =  select(GoogleAdsData).filter_by(date=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            
            else:
                response = ga_service.search_stream(search_request)
                # delete row data
                await session.execute(
                    delete(GoogleAdsData).filter(GoogleAdsData.date.between(start_date, end_date))
                )
                
                # Save data to MySQL and SQLite
                for batch in response:
                    data = [
                        GoogleAdsData(
                            date=datetime.datetime.strptime(row.segments.date, "%Y-%m-%d").date(),
                            campaign_id=row.campaign.id,
                            campaign_name=row.campaign.name,
                            impressions=row.metrics.impressions,
                            clicks=row.metrics.clicks,
                            spend=row.metrics.cost_micros / 1000000,  # Convert from micros to dollars
                            conversions=row.metrics.conversions
                        ) for row in batch.results
                    ]

                    session.add_all(data)
                await session.commit()
                await session.close()
                return 'Data has been updated!'
        
        elif types == "manual":
            response = ga_service.search_stream(search_request)
            # delete row data
            await session.execute(
                delete(GoogleAdsData).filter(GoogleAdsData.date.between(start_date, end_date))
            )
            
            # Save data to MySQL and SQLite
            for batch in response:
                data = [
                    GoogleAdsData(
                        date=datetime.datetime.strptime(row.segments.date, "%Y-%m-%d").date(),
                        campaign_id=row.campaign.id,
                        campaign_name=row.campaign.name,
                        impressions=row.metrics.impressions,
                        clicks=row.metrics.clicks,
                        spend=row.metrics.cost_micros / 1000000,  # Convert from micros to dollars
                        conversions=row.metrics.conversions
                    ) for row in batch.results
                ]
                    
                session.add_all(data)
            await session.commit()
            await session.close()
            return 'Data has been updated!'


def get_access_token():
    """
    Gets an access token from the Apple Search Ads API.

    Returns:
    str: Access token for accessing the API.

    Raises:
    Exception: If failed to get the access token.
    """
    # URL for accessing the Apple Search Ads API token endpoint
    url = "https://appleid.apple.com/auth/oauth2/token"
    
    # Header specifying the content type
    headers = {
        "Host": "appleid.apple.com",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    # Parameters required for obtaining the access token
    data = {
        "client_id": config("ASA_CLIENT_ID", cast=str),
        "client_secret": config("ASA_CLIENT_SECRET", cast=str),
        "grant_type": "client_credentials",
        "scope": "searchadsorg"
    }
    
    # Send POST request to obtain the access token
    response = requests.post(url=url, headers=headers, data=data)
    
    # If the request is successful, return the access token
    if response.status_code == 200:
        access_token = response.json().get("access_token")
        return access_token
    # Otherwise, raise an exception
    else:
        raise Exception(f"Failed to get access token: {response.status_code} - {response.text}")
    

def get_refresh_token():
    """Get refresh toke ASA"""
    private_key_file = "private-key.pem"
    public_key_file = "public-key.pem"
    client_id = config("ASA_CLIENT_ID", cast=str)
    team_id = config("ASA_TEAM_ID", cast=str)
    key_id = config("ASA_KEY_ID", cast=str)
    audience = "https://appleid.apple.com"
    alg = "ES256"


    # Create the private key if it doesn't already exist.
    if os.path.isfile(private_key_file):
        with open(private_key_file, "rt") as file:
            private_key = ECC.import_key(file.read())
    else:
        private_key = ECC.generate(curve='P-256')
        with open(private_key_file, 'wt') as file:
            file.write(private_key.export_key(format='PEM'))


    # Extract and save the public key.
    public_key = private_key.public_key()
    if not os.path.isfile(public_key_file):
        with open(public_key_file, 'wt') as file:
            file.write(public_key.export_key(format='PEM'))


    # Define the issue timestamp.
    issued_at_timestamp = int(datetime.datetime.now().timestamp())
    # Define the expiration timestamp, which may not exceed 180 days from the issue timestamp.
    expiration_timestamp = issued_at_timestamp + 86400*180


    # Define the JWT headers.
    headers = dict()
    headers['alg'] = alg
    headers['kid'] = key_id


    # Define the JWT payload.
    payload = dict()
    payload['sub'] = client_id
    payload['aud'] = audience
    payload['iat'] = issued_at_timestamp
    payload['exp'] = expiration_timestamp
    payload['iss'] = team_id


    # Open the private key.
    with open(private_key_file, 'rt') as file:
        private_key = ECC.import_key(file.read())


    # Encode the JWT and sign it with the private key.
    client_secret = jwt.encode(
        header=headers,
        payload=payload,
        key=private_key.export_key(format='PEM')
    ).decode('UTF-8')


    # Save the client secret to a file.
    with open('client_secret.txt', 'w') as output:
         output.write(client_secret)


async def get_asa_campaign_report(
        access_token, 
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    pulling data apple search ads API and store it to database
    
    Args:
        access_token (str): The accsess token retrieve from get_access_token() function.
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').
    """
    # Define the endpoint URL
    BASE_URL = "https://api.searchads.apple.com/api/v5/reports/campaigns"
    org_id = config("ASA_ORG_ID", cast=int)
    # Define the payload for generating JWT token
    header = {
        'Authorization': f'Bearer {access_token}',
        'X-AP-Context': f'orgId={org_id}',
        'Content-Type': 'application/json'
    }

    # Define your query parameters
    params = {
        "startTime": f"{start_date}",
        "endTime": f"{end_date}",
        "granularity":"DAILY",
        "selector": {
            "orderBy": [
            {
                "field": "startTime",
                "sortOrder": "DESCENDING"
            }
            ],
            "pagination": {
                "offset": 1,
                "limit": 1000
                }
        },
        "timeZone": "UTC",
        "returnRecordsWithNoMetrics": True,
        "returnRowTotals": False,
        "returnGrandTotals": False
        }
    
    request_payload_json = json.dumps(params)

    # format datetime into date only
    last1days = datetime.datetime.today() - datetime.timedelta(1)
    last1days_date = last1days.date()

    # Use SQLite to log daily Google Ads data
    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)

    if session is not None:
        query = select(AsaData).filter_by(date=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                # delete row data
                await session.execute(
                    delete(AsaData).filter(AsaData.date.between(start_date, end_date))
                )
                
                response = requests.post(BASE_URL, headers=header, data=request_payload_json)
                campaign_data = response.json()
                # Process campaign_data as needed
                # Extracting data from the response dictionary
                data = campaign_data['data']['reportingDataResponse']['row']

                # Iterate through campaign data and flatten it
                for campaign in data:
                    campaign_name = campaign['metadata']['campaignName']
                    daily_budget = campaign['metadata']['dailyBudget']['amount']
                    
                    granularity_data = campaign['granularity']
                    data = [
                        AsaData(
                            date = datetime.datetime.strptime(entry['date'], "%Y-%m-%d").date(),
                            campaign_name=campaign_name,
                            daily_budget=daily_budget,
                            local_spend = entry['localSpend']['amount'],
                            impressions = entry['impressions'],
                            taps = entry['taps'],
                            installs = entry['totalInstalls'],
                            new_downloads = entry['totalNewDownloads'],
                            redownloads = entry['totalRedownloads'],
                        ) for entry in granularity_data
                    ]
                    session.add_all(data)
                await session.commit()
                await session.close()

                return "Data is being updated!"
        
        elif types == "manual":
            # delete row data
            await session.execute(
                delete(AsaData).filter(AsaData.date.between(start_date, end_date))
            )
            
            response = requests.post(BASE_URL, headers=header, data=request_payload_json)
            campaign_data = response.json()
            # Process campaign_data as needed
            # Extracting data from the response dictionary
            data = campaign_data['data']['reportingDataResponse']['row']

            # Iterate through campaign data and flatten it
            for campaign in data:
                campaign_name = campaign['metadata']['campaignName']
                daily_budget = campaign['metadata']['dailyBudget']['amount']
                    
                granularity_data = campaign['granularity']
                
                asa_data = [
                    AsaData(
                        date = datetime.datetime.strptime(entry['date'], "%Y-%m-%d").date(),
                        campaign_name=campaign_name,
                        daily_budget=daily_budget,
                        local_spend = entry['localSpend']['amount'],
                        impressions = entry['impressions'],
                        taps = entry['taps'],
                        installs = entry['totalInstalls'],
                        new_downloads = entry['totalNewDownloads'],
                        redownloads = entry['totalRedownloads'],
                    ) for entry in granularity_data
                ]
                session.add_all(asa_data)
            await session.commit()
            await session.close()
                    
            return "Data is being updated!"


async def admob_report_api(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    Pulls AdMob mediation reports and store it to database.

    This function fetches AdMob mediation reports for a specified date range and store
    to database file with the fetched data. The default date range is from 60 days ago
    to 1 day ago. The function checks if the data for the latest day is already present
    in databasee and updates the file only if new data is available.

    Parameters:
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
    str: A message indicating whether the data is up-to-date or has been updated.
    """
    PUBLISHER_ID = config("ADMOB_PUBLISHER_ID", cast=str)

    # Create the credentials object from the refresh token
    credentials = Credentials.from_authorized_user_info(info={
        'refresh_token': config("ADMOB_REFRESH_TOKEN", cast=str),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': config("ADMOB_CLIENT_ID", cast=str),
        'client_secret': config("ADMOB_CLIENT_SECRET", cast=str)
    })

    admob = build('admob', 'v1', credentials=credentials)
    
    date_range = {
        'start_date': {'year': start_date.year, 'month': start_date.month, 'day': start_date.day},
        'end_date': {'year': end_date.year, 'month': end_date.month, 'day': end_date.day}
    }
    dimensions = ['DATE', 'PLATFORM']
    metrics = ['ESTIMATED_EARNINGS', 'IMPRESSIONS', 'OBSERVED_ECPM', 'IMPRESSION_CTR', 'CLICKS', 'AD_REQUESTS', 'MATCH_RATE', 'MATCHED_REQUESTS']
    sort_conditions = {'dimension': 'DATE', 'order': 'DESCENDING'}

    report_spec = {
        'date_range': date_range,
        'dimensions': dimensions,
        'metrics': metrics,
        'sort_conditions': [sort_conditions]
    }

    # Create network report request.
    request = {'report_spec': report_spec}
    
    # Parsing column date
    try:
        # Use SQLite to log daily Google Ads data
        if session is None:
            async_gen = get_sqlite()
            session = await anext(async_gen)

        if session is not None:
            query = select(AdmobReportData).filter_by(date=end_date)
            existing_data = (await session.execute(query)).first()

            if types == "auto":
                if existing_data:
                    await session.close()
                    return 'Data is up-to-date!'
                else:
                    try:
                        # Execute network report request.
                        response = admob.accounts().mediationReport().generate(
                            parent='accounts/{}'.format(PUBLISHER_ID), body=request).execute()
                    except Exception as e:
                        return 'Failed to fetch AdMob report.'
                    # delete row data
                    await session.execute(
                        delete(AdmobReportData).filter(AdmobReportData.date.between(start_date, end_date))
                    )
                    
                    # Insert bulk to sqlite
                    data = [
                        AdmobReportData(
                            date = datetime.datetime.strptime(entry["row"]['dimensionValues']['DATE']['value'], "%Y%m%d"),
                            platform = entry["row"]['dimensionValues']['PLATFORM']['value'],
                            estimated_earnings = entry["row"]['metricValues']['ESTIMATED_EARNINGS']['microsValue'] if \
                                "ESTIMATED_EARNINGS" in entry["row"]['metricValues'] else 0.0,
                            impressions = entry["row"]['metricValues']['IMPRESSIONS']['integerValue'] if \
                                "IMPRESSIONS" in entry["row"]['metricValues'] else 0,
                            observed_ecpm = entry["row"]['metricValues']['OBSERVED_ECPM']['microsValue'] if \
                                "OBSERVED_ECPM" in entry["row"]['metricValues'] else 0.0,
                            impression_ctr = entry["row"]['metricValues']['IMPRESSION_CTR']['doubleValue'] if \
                                "IMPRESSION_CTR" in entry["row"]['metricValues'] else 0.0,
                            clicks = entry["row"]['metricValues']['CLICKS']['integerValue'] if \
                                "CLICKS" in entry["row"]['metricValues'] else 0,
                            ad_requests = entry["row"]['metricValues']['AD_REQUESTS']['integerValue'] if \
                                "AD_REQUESTS" in entry["row"]['metricValues'] else 0,
                            match_rate = entry["row"]['metricValues']['MATCH_RATE']['doubleValue'] if \
                                "MATCH_RATE" in entry["row"]['metricValues'] else 0.0,
                            match_requests = entry["row"]['metricValues']['MATCHED_REQUESTS']['integerValue'] if \
                                "MATCHED_REQUESTS" in entry["row"]['metricValues'] else 0
                        ) for entry in response[1:-1]
                    ]
                    session.add_all(data)
                    await session.commit()
                    await session.close()

                    return 'Data has been updated!'
                
            elif types == "manual":
                try:
                    # Execute network report request.
                    response = admob.accounts().mediationReport().generate(
                        parent='accounts/{}'.format(PUBLISHER_ID), body=request).execute()
                except Exception as e:
                    return 'Failed to fetch AdMob report.'
                # delete row data
                await session.execute(
                    delete(AdmobReportData).filter(AdmobReportData.date.between(start_date, end_date))
                )
                    
                # Insert bulk to sqlite
                data = [
                    AdmobReportData(
                        date = datetime.datetime.strptime(entry["row"]['dimensionValues']['DATE']['value'], "%Y%m%d"),
                        platform = entry["row"]['dimensionValues']['PLATFORM']['value'],
                        estimated_earnings = entry["row"]['metricValues']['ESTIMATED_EARNINGS']['microsValue'] if \
                            "ESTIMATED_EARNINGS" in entry["row"]['metricValues'] else 0.0,
                        impressions = entry["row"]['metricValues']['IMPRESSIONS']['integerValue'] if \
                            "IMPRESSIONS" in entry["row"]['metricValues'] else 0,
                        observed_ecpm = entry["row"]['metricValues']['OBSERVED_ECPM']['microsValue'] if \
                            "OBSERVED_ECPM" in entry["row"]['metricValues'] else 0.0,
                        impression_ctr = entry["row"]['metricValues']['IMPRESSION_CTR']['doubleValue'] if \
                            "IMPRESSION_CTR" in entry["row"]['metricValues'] else 0.0,
                        clicks = entry["row"]['metricValues']['CLICKS']['integerValue'] if \
                            "CLICKS" in entry["row"]['metricValues'] else 0,
                        ad_requests = entry["row"]['metricValues']['AD_REQUESTS']['integerValue'] if \
                            "AD_REQUESTS" in entry["row"]['metricValues'] else 0,
                        match_rate = entry["row"]['metricValues']['MATCH_RATE']['doubleValue'] if \
                            "MATCH_RATE" in entry["row"]['metricValues'] else 0.0,
                        match_requests = entry["row"]['metricValues']['MATCHED_REQUESTS']['integerValue'] if \
                            "MATCHED_REQUESTS" in entry["row"]['metricValues'] else 0
                    ) for entry in response[1:-1]
                ]
                session.add_all(data)
                await session.commit()
                await session.close()

            return 'Data has been updated!'
    except Exception as e:
        return 'Error updating data!'


async def adsense_reprot_api(
        from_date: datetime.date, 
        to_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    Fetches Google AdSense report data using the AdSense Management API.

    This function retrieves AdSense report data either for a custom date range or the last 60 days
    and store it into database. It checks if the data is already up-to-date and returns a message
    accordingly.

    Args:
        session (AsyncSession): The Asynchornous SQLite session. 
        from_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        to_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: A message indicating the status of the data update.

    Raises:
        ValueError: If an unsupported data retrieval method is provided.

    Note:
        Ensure the necessary packages are installed:
            - pandas
            - google-auth
            - google-auth-oauthlib
            - google-api-python-client
    """
    
    last1days = datetime.datetime.today() - datetime.timedelta(1)
    last1days_date = last1days.date()
    ACCOUNT_ID = config("ADSENSE_PUBLISHER_ID", cast=str)

    # create the credentials object from the refresh token
    credentials = Credentials.from_authorized_user_info(info={
        'refresh_token': config("ADSENSE_REFRESH_TOKEN", cast=str),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': config("ADSENSE_CLIENT_ID", cast=str),
        'client_secret': config("ADSENSE_CLIENT_SECRET", cast=str)
    })

    adsense = build('adsense', 'v2', credentials=credentials)

    start_date = {'year':from_date.year,'month':from_date.month,'day':from_date.day}
    end_date = {'year':to_date.year,'month':to_date.month,'day':to_date.day}
    dimension = ['DATE', 'PLATFORM_TYPE_NAME', 'AD_PLACEMENT_NAME', 'AD_FORMAT_CODE']
    metrics = ['ESTIMATED_EARNINGS', 'IMPRESSIONS', 'CLICKS', 'AD_REQUESTS', 'MATCHED_AD_REQUESTS', 'IMPRESSIONS_RPM', 'IMPRESSIONS_CTR', 'AD_REQUESTS_CTR', 'MATCHED_AD_REQUESTS_CTR']
    order_by = ['+DATE']

    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)

    if session is not None:
        query = select(AdsenseReportData).filter_by(date=to_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                # delete row data
                await session.execute(
                    delete(AdsenseReportData).filter(AdsenseReportData.date.between(from_date, to_date))
                )

                result = adsense.accounts().reports().generate(
                    account='accounts/{}'.format(ACCOUNT_ID),
                    dateRange='CUSTOM',
                    startDate_year=start_date['year'],
                    startDate_month=start_date['month'],
                    startDate_day=start_date['day'],
                    endDate_year=end_date['year'],
                    endDate_month=end_date['month'],
                    endDate_day=end_date['day'],
                    metrics=metrics,
                    dimensions=dimension,
                    orderBy =order_by
                ).execute()

                data = [
                    AdsenseReportData(
                        date = datetime.datetime.strptime(row["cells"][0]["value"], "%Y-%m-%d").date(),
                        platform_type_name = row["cells"][1]["value"],
                        ad_placement_name = row["cells"][2]["value"],
                        ad_format_code = row["cells"][3]["value"],
                        estimated_earnings = row["cells"][4]["value"],
                        impressions = row["cells"][5]["value"],
                        clicks = row["cells"][6]["value"],
                        ad_requests = row["cells"][7]["value"],
                        matched_ad_requests = row["cells"][8]["value"],
                        impressions_rpm = row["cells"][9]["value"],
                        impressions_ctr = row["cells"][10]["value"],
                        ad_requests_ctr = row["cells"][11]["value"],
                        matched_ad_requests_ctr = row["cells"][12]["value"]
                    ) for row in result["rows"]
                ]
                session.add_all(data)
                await session.commit()
                await session.close()
                return 'data is being updated!'
        
        elif types == "manual":
            # delete row data
            await session.execute(
                delete(AdsenseReportData).filter(AdsenseReportData.date.between(from_date, to_date))
            )

            result = adsense.accounts().reports().generate(
                account='accounts/{}'.format(ACCOUNT_ID),
                dateRange='CUSTOM',
                startDate_year=start_date['year'],
                startDate_month=start_date['month'],
                startDate_day=start_date['day'],
                endDate_year=end_date['year'],
                endDate_month=end_date['month'],
                endDate_day=end_date['day'],
                metrics=metrics,
                dimensions=dimension,
                orderBy =order_by
            ).execute()

            data = [
                AdsenseReportData(
                    date = datetime.datetime.strptime(row["cells"][0]["value"], "%Y-%m-%d").date(),
                    platform_type_name = row["cells"][1]["value"],
                    ad_placement_name = row["cells"][2]["value"],
                    ad_format_code = row["cells"][3]["value"],
                    estimated_earnings = row["cells"][4]["value"],
                    impressions = row["cells"][5]["value"],
                    clicks = row["cells"][6]["value"],
                    ad_requests = row["cells"][7]["value"],
                    matched_ad_requests = row["cells"][8]["value"],
                    impressions_rpm = row["cells"][9]["value"],
                    impressions_ctr = row["cells"][10]["value"],
                    ad_requests_ctr = row["cells"][11]["value"],
                    matched_ad_requests_ctr = row["cells"][12]["value"]
                ) for row in result["rows"]
            ]
            session.add_all(data)
            await session.commit()
            await session.close()
            return 'data is being updated!'


async def fb_api(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        campaign_name: str = "AAA",
        attribution_window: list = ["1d_click"],
        types: str = "manual"):
    """
    Fetch Facebook Ads insights data via the Facebook Marketing API and store it in a database.

    This function initializes the Facebook Ads API with the required credentials and retrieves insights data
    such as campaign performance metrics (impressions, clicks, spend, etc.) for the last 90 days. It then writes
    this data to a database. If the data is already up to date, it returns a message indicating
    that. If not, it updates the data and returns a confirmation message.

    Args:
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: A message indicating whether the data is up to date or being updated.

    Example:
        fb_api()
    """

    my_app_id = config("FACEBOOK_APP_ID", cast=str)
    my_app_secret = config("FACEBOOK_APP_SECRET", cast=str)
    my_access_token = config("FACEBOOK_ACCESS_TOKEN", cast=str)

    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token, api_version="v20.0")
    my_account = AdAccount(fbid=config("FACEBOOK_ID", cast=str))

    fields = [
        'date_start',
        'date_stop',
        'campaign_name',
        'impressions',
        'clicks',
        'spend',
        'actions'
    ]
    params = {
        'action_attribution_windows': attribution_window,
        'level': 'campaign',
        'time_range': {'since': f'{start_date}', 'until': f'{end_date}'},
        'time_increment': 1,
        'fields': fields,
        'filtering': [{'field': 'campaign.name', 'operator': 'CONTAIN', 'value': f"{campaign_name}"}],
        'use_account_attribution_setting': True
    }

    # format datetime into date only
    last1days = datetime.datetime.today() - datetime.timedelta(1)
    last1days_date = last1days.date()
    
    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)

    if session is not None:
        query = select(FacebookAdsData).filter_by(date_start=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                # delete row data
                await session.execute(
                    delete(FacebookAdsData).filter(FacebookAdsData.date_start.between(start_date, end_date), FacebookAdsData.campaign_name == campaign_name)
                )
                
                campaigns = my_account.get_insights(params=params)
                # Extract the data you want
                data = [
                    FacebookAdsData(
                        date_start=datetime.datetime.strptime(insight['date_start'], "%Y-%m-%d").date(),
                        date_stop=datetime.datetime.strptime(insight['date_stop'], "%Y-%m-%d").date(),
                        campaign_name=insight['campaign_name'],
                        impressions=insight['impressions'],
                        clicks=insight['clicks'],
                        spend=insight['spend'],
                        unique_actions_mobile_app_install=insight['actions'][0]['7d_click'] if \
                            "actions" in insight and \
                                "7d_click" in insight['actions'][0]  else 0
                    ) for insight in campaigns
                ]
                session.add_all(data)
                await session.commit()
                await session.close()
                return 'data is being updated!'
            
        elif types == "manual":
            # delete row data
            await session.execute(
                delete(FacebookAdsData).filter(FacebookAdsData.date_start.between(start_date, end_date), FacebookAdsData.campaign_name == campaign_name)
            )
            
            campaigns = my_account.get_insights(params=params)
            
            # Extract the data you want
            data = [
                FacebookAdsData(
                    date_start=datetime.datetime.strptime(insight['date_start'], "%Y-%m-%d").date(),
                    date_stop=datetime.datetime.strptime(insight['date_stop'], "%Y-%m-%d").date(),
                    campaign_name=insight['campaign_name'],
                    impressions=insight['impressions'],
                    clicks=insight['clicks'],
                    spend=insight['spend'],
                    unique_actions_mobile_app_install=insight['actions'][0]['1d_click'] if \
                        "actions" in insight and \
                            "1d_click" in insight['actions'][0]  else 0
                ) for insight in campaigns
            ]
            session.add_all(data)
            await session.commit()
            await session.close()
            return 'data is being updated!'


async def get_google_analytics_data(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    Fetch Google Analytics data and store it in a database.

    This function fetches data from Google Analytics for the specified view ID
    and date range. The data includes metrics such as sessions, new users, active users,
    total users, and bounce rate, segmented by dimensions such as date, device category,
    platform, and source. The fetched data is then appended to a database.

    Parameters:
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: A message indicating whether the data was updated or is already up-to-date.
    """
    # Create the credentials object from the refresh token
    credentials = Credentials.from_authorized_user_info(info={
        'refresh_token': config("GA4_REFRESH_TOKEN", cast=str),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': config("GA4_CLIENT_ID", cast=str),
        'client_secret': config("GA4_CLIENT_SECRET", cast=str)
    })

    # Create the client object
    client = BetaAnalyticsDataClient(credentials=credentials)
    view_id = config("GA4_VIEW_ID", cast=str)

    # Define the report request
    request = RunReportRequest(
        property=f"properties/{view_id}",
        dimensions=[
            Dimension(name="date"), 
            Dimension(name="deviceCategory"),
            Dimension(name="platform"),
            Dimension(name="sessionDefaultChannelGroup")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="newUsers"),
            Metric(name="activeUsers"),
            Metric(name="totalUsers"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="engagedSessions"), 
            Metric(name="userEngagementDuration")],
        date_ranges=[DateRange(start_date=f"{start_date}", end_date=f"{end_date}")],
    )

    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)
    
    if session is not None:
        query = select(Ga4AnalyticsData).filter_by(date=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                response = client.run_report(request)
                # delete row data
                await session.execute(
                    delete(Ga4AnalyticsData).filter(Ga4AnalyticsData.date.between(start_date, end_date))
                )

                # Insert bulk data to sqlite
                data = [
                    Ga4AnalyticsData(
                        date=datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d"),
                        device_category=row.dimension_values[1].value,
                        platform=row.dimension_values[2].value,
                        source=row.dimension_values[3].value,
                        sessions=row.metric_values[0].value,
                        new_user=row.metric_values[1].value,
                        active_user=row.metric_values[2].value,
                        total_user=row.metric_values[3].value,
                        bounce_rate=row.metric_values[4].value,
                        avg_sesseion_duration=row.metric_values[5].value,
                        engaged_session=row.metric_values[6].value,
                        user_enagged_duration=row.metric_values[4].value
                    ) for row in response.rows
                ]
                session.add_all(data)
                await session.commit()
                await session.close()
                return 'Data is being updated!'
        
        elif types == "manual":
            response = client.run_report(request)
            # delete row data
            await session.execute(
                delete(Ga4AnalyticsData).filter(Ga4AnalyticsData.date.between(start_date, end_date))
            )

            # Insert bulk data to sqlite
            data = [
                Ga4AnalyticsData(
                    date=datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d"),
                    device_category=row.dimension_values[1].value,
                    platform=row.dimension_values[2].value,
                    source=row.dimension_values[3].value,
                    sessions=row.metric_values[0].value,
                    new_user=row.metric_values[1].value,
                    active_user=row.metric_values[2].value,
                    total_user=row.metric_values[3].value,
                    bounce_rate=row.metric_values[4].value,
                    avg_sesseion_duration=row.metric_values[5].value,
                    engaged_session=row.metric_values[6].value,
                    user_enagged_duration=row.metric_values[4].value
                ) for row in response.rows
            ]
            session.add_all(data)
            await session.commit()
            await session.close()
            return 'Data is being updated!'


async def get_ga4_session(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    Fetch Google Analytics data and store it in a database.

    This function fetches data from Google Analytics for the specified view ID
    and date range. The data includes metrics such as sessions, new users, active users,
    total users, and bounce rate, segmented by dimensions such as date, device category,
    platform, and source. The fetched data is then appended to a database.

    Parameters:
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: A message indicating whether the data was updated or is already up-to-date.
    """
    # Create the credentials object from the refresh token
    credentials = Credentials.from_authorized_user_info(info={
        'refresh_token': config("GA4_REFRESH_TOKEN", cast=str),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': config("GA4_CLIENT_ID", cast=str),
        'client_secret': config("GA4_CLIENT_SECRET", cast=str)
    })

    # Create the client object
    client = BetaAnalyticsDataClient(credentials=credentials)
    view_id = config("GA4_VIEW_ID", cast=str)

    # Define the report request
    request = RunReportRequest(
        property=f"properties/{view_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="deviceCategory"),
            Dimension(name="platform")],
        metrics=[
            Metric(name="userEngagementDuration")],
        date_ranges=[DateRange(start_date=f"{start_date}", end_date=f"{end_date}")],
    )

    if session is None:
        async_gen = get_sqlite()
        session  = await anext(async_gen)
    
    if session is not None:
        query = select(Ga4SessionsData).filter_by(date=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                response = client.run_report(request)
                # delete row data
                await session.execute(
                    delete(Ga4SessionsData).filter(Ga4SessionsData.date.between(start_date, end_date))
                )
                
                # Insert bulk data to sqlite
                data = [
                    Ga4SessionsData(
                        date=datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d").date(),
                        device_category=row.dimension_values[1].value,
                        platform=row.dimension_values[2].value,
                        user_engaged_duration=row.metric_values[0].value
                    ) for row in response.rows
                ]
                session.add_all(data)
                await session.commit()
                await session.close()
                return 'Data is being updated!'
        
        elif types == "manual":
            response = client.run_report(request)
            # delete row data
            await session.execute(
                delete(Ga4SessionsData).filter(Ga4SessionsData.date.between(start_date, end_date))
            )
                
            # Insert bulk data to sqlite
            data = [
                Ga4SessionsData(
                    date=datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d").date(),
                    device_category=row.dimension_values[1].value,
                    platform=row.dimension_values[2].value,
                    user_engaged_duration=row.metric_values[0].value
                ) for row in response.rows
            ]
            session.add_all(data)
            await session.commit()
            await session.close()
            return 'Data is being updated!'


async def landing_page_ga4(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    Pull landing page data from Google Analytics 4 by sessions and store it in a database.

    This function retrieves data for landing pages from Google Analytics 4 for the specified
    view ID and date range. The data includes dimensions such as date, landing page URL,
    session default channel group, platform, and source/medium, along with the metric sessions.
    The fetched data is then appended to a database.

    Parameters:
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: A message indicating whether the data was updated or is already up-to-date.
    """
    # Create the credentials object from the refresh token
    credentials = Credentials.from_authorized_user_info(info={
        'refresh_token': config("GA4_REFRESH_TOKEN", cast=str),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': config("GA4_CLIENT_ID", cast=str),
        'client_secret': config("GA4_CLIENT_SECRET", cast=str)
    })

    # Create the client object
    client = BetaAnalyticsDataClient(credentials=credentials)
    view_id = config("GA4_VIEW_ID", cast=str)
    
    # Define the report request
    request = RunReportRequest(
        property=f"properties/{view_id}",
        dimensions=[
            Dimension(name="date"), 
            Dimension(name="landingPagePlusQueryString"), 
            Dimension(name="sessionDefaultChannelGroup"),
            Dimension(name="platform"),
            Dimension(name="sourceMedium")],
        metrics=[
            Metric(name="sessions")],
        date_ranges=[DateRange(start_date=f"{start_date}", end_date=f"{end_date}")],
    )
    
    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)
    
    if session is not None:
        query = select(Ga4LandingPageData).filter_by(date=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                response = client.run_report(request)
                # delete row data
                await session.execute(
                    delete(Ga4LandingPageData).filter(Ga4LandingPageData.date.between(start_date, end_date))
                )
                data = [
                    Ga4LandingPageData(
                        date=datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d"),
                        landing_page=row.dimension_values[1].value,
                        source=row.dimension_values[2].value,
                        platform=row.dimension_values[3].value,
                        medium=row.dimension_values[4].value,
                        sessions=row.metric_values[0].value
                    ) for row in response.rows
                ]
                session.add_all(data)
                await session.commit()
                await session.close()
                return 'Data is being updated!'
            
        elif types == "manual":
            response = client.run_report(request)
            # delete row data
            await session.execute(
                delete(Ga4LandingPageData).filter(Ga4LandingPageData.date.between(start_date, end_date))
            )
            data = [
                Ga4LandingPageData(
                    date=datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d"),
                    landing_page=row.dimension_values[1].value,
                    source=row.dimension_values[2].value,
                    platform=row.dimension_values[3].value,
                    medium=row.dimension_values[4].value,
                    sessions=row.metric_values[0].value
                ) for row in response.rows
            ]
            session.add_all(data)
            await session.commit()
            await session.close()
            return 'Data is being updated!'


async def tiktok_report_API(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    Get TikTok campaign report via API and stroe it to database.

    Parameters:
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: Message indicating the status of data retrieval.
            - 'Data is uptodate!': If the data is already up-to-date.
            - 'Data is being updated!': If the data is being updated.

    """
    from_date = start_date + datetime.timedelta(30)
    # API endpoint URL
    URL = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/'
    
    # TikTok account details
    ACCESS_TOKEN = config("TIKTOK_ACCESS_TOKEN", cast=str)
    ADVERTISER_ID = config("TIKTOK_ADVERTISER_ID", cast=str)
    BC_ID = config("TIKTOK_BC_ID", cast=str)

    # Request headers
    headers = {
        'Access-Token': ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }

    # Request parameters
    params = {
        'report_type': 'BASIC',
        'service_type': 'AUCTION',
        'advertiser_id': ADVERTISER_ID,
        'data_level': 'AUCTION_CAMPAIGN',
        'dimensions': ['stat_time_day', 'campaign_id'],
        'metrics': ['campaign_name', 'spend', 'impressions', 'clicks', 'conversion'],
        'start_date': f'{from_date}',
        'end_date': f'{end_date}',
        'page_size': 31
    }

    # format datetime into date only
    last1days = datetime.datetime.today() - datetime.timedelta(1)
    last1days_date = last1days.date()
    
    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)

    if session is not None:
        query = select(TiktokAdsData).filter_by(date=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                # delete row data
                await session.execute(
                    delete(TiktokAdsData).filter(TiktokAdsData.date.between(start_date, end_date))
                )

                # Fetch new data from the API
                response = requests.get(url=URL, headers=headers, json=params).json()

                data = [
                    TiktokAdsData(
                        date=datetime.datetime.strptime(items['dimensions']['stat_time_day'], "%Y-%m-%d %H:%M:%S").date(),
                        campaign_name=items['metrics']['campaign_name'],
                        spend=items['metrics']['spend'],
                        impressions=items['metrics']['impressions'],
                        clicks=items['metrics']['clicks'],
                        conversion=items['metrics']['conversion']
                    ) for items in response['data']['list']
                ]
                session.add_all(data)
                await session.commit()
                await session.close()
                return 'Data is being updated!'
        
        elif types == "manual":
            # delete row data
            await session.execute(
                delete(TiktokAdsData).filter(TiktokAdsData.date.between(start_date, end_date))
            )

            # Fetch new data from the API
            response = requests.get(url=URL, headers=headers, json=params).json()

            data = [
                TiktokAdsData(
                    date=datetime.datetime.strptime(items['dimensions']['stat_time_day'], "%Y-%m-%d %H:%M:%S").date(),
                    campaign_name=items['metrics']['campaign_name'],
                    spend=items['metrics']['spend'],
                    impressions=items['metrics']['impressions'],
                    clicks=items['metrics']['clicks'],
                    conversion=items['metrics']['conversion']
                ) for items in response['data']['list']
            ]
            session.add_all(data)
            await session.commit()
            await session.close()
            return 'Data is being updated!'


async def google_sheet_api(
        sheet_range: str='', 
        file: str = '', 
        types: str = "manual"):
    """
    Pull data from a Google Sheet and save it to a CSV file.

    Parameters:
        sheet_range (str): The range of cells to retrieve data from in the Google Sheet.
        file (str): The name of the CSV file to save the data to.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: A message indicating whether the data is up to date or being updated.
    """
    today = datetime.datetime.today()

    # Create the credentials object from the refresh token
    creds = Credentials.from_authorized_user_info(info={
        'refresh_token': config("GOOGLE_SHEET_REFRESH_TOKEN", cast=str),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': config("GOOGLE_SHEET_CLIENT_ID", cast=str),
        'client_secret': config("GOOGLE_SHEET_CLIENT_SECRET", cast=str)
    })
    sheet_id = config("GOOGLE_SHEET_RANKING_INDEXING_ID", cast=str) if file in ["indexing", "ranking"] else config("GOOGLE_SHEET_DAILY_UPDATE_ID", cast=str)

    # Read the CSV file to check if the data is up to date
    read_df =  pd.read_csv(f'./csv/{file}.csv', index_col=False)

    # Check if today's date is already in the CSV file
    if types == "auto":
        if today.date().strftime('%Y-%m-%d') in read_df["pull_date"].values:
            return 'Data is up to date!'
        else:
            # sheet_id = '1C0kq0WFLPxfjltTg_OXn0q8xlpHrgs168jos99RxqZU'
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()

            # Retrieve data from the Google Sheet
            result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_range).execute()
            values = result.get('values', [])

            # Write the retrieved data to the CSV file
            with open(f'./csv/{file}.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                for row in values:
                    writer.writerow([i for i in row])
            
            # Read the CSV file again and update the pull_date column
            df = pd.read_csv(f'./csv/{file}.csv', index_col=False)
            if file == 'indexing':
                df = pd.read_csv(f'./csv/{file}.csv', index_col=False).set_index('Ahrefs Metrics').T
                df.index.name = 'week'
            else:
                df = pd.read_csv(f'./csv/{file}.csv', index_col=False)
            df['pull_date'] = datetime.datetime.today().date().strftime('%Y-%m-%d')
            df.to_csv(f'./csv/{file}.csv')

            return 'Data is being updated!'
    
    elif types == "manual":
        # sheet_id = '1C0kq0WFLPxfjltTg_OXn0q8xlpHrgs168jos99RxqZU'
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Retrieve data from the Google Sheet
        result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_range).execute()
        values = result.get('values', [])

        # Write the retrieved data to the CSV file
        with open(f'./csv/{file}.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in values:
                writer.writerow([i for i in row])
            
        # Read the CSV file again and update the pull_date column
        df = pd.read_csv(f'./csv/{file}.csv', index_col=False)
        if file == 'indexing':
            df = pd.read_csv(f'./csv/{file}.csv', index_col=False).set_index('Ahrefs Metrics').T
            df.index.name = 'week'
        else:
            df = pd.read_csv(f'./csv/{file}.csv', index_col=False)
        df['pull_date'] = datetime.datetime.today().date().strftime('%Y-%m-%d')
        df.to_csv(f'./csv/{file}.csv')

        return 'Data is being updated!'


async def ga4_active_user(
        start_date: datetime.date, 
        end_date: datetime.date, 
        session: AsyncSession = None, 
        types: str = "manual"):
    """
    Pull active user data from Google Analytics 4 and store it in a database.

    This function retrieves data for active users from Google Analytics 4 for the specified
    view ID and date range. The data includes dimensions such as date and platform, along with
    the metrics active 1-day users and active 28-day users. The fetched data is then appended
    to a database.

    Parameters:
        session (AsyncSession): The Asynchornous SQLite session. 
        start_date (datetime.date): The start date for the data retrieval in the format 'YYYY-MM-DD'.
        end_date (datetime.date): The end date for the data retrieval in the format 'YYYY-MM-DD'.
        types (str): The types trigering the function ('manual' or 'auto').

    Returns:
        str: A message indicating whether the data was updated or is already up-to-date.
    """
    # Create the credentials object from the refresh token
    credentials = Credentials.from_authorized_user_info(info={
        'refresh_token': config("GA4_REFRESH_TOKEN", cast=str),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': config("GA4_CLIENT_ID", cast=str),
        'client_secret': config("GA4_CLIENT_SECRET", cast=str)
    })

    # Create the client object
    client = BetaAnalyticsDataClient(credentials=credentials)
    view_id = config("GA4_VIEW_ID", cast=str)

    # Define the report request
    request = RunReportRequest(
        property=f"properties/{view_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="platform")],
        metrics=[
            Metric(name="active1DayUsers"),
            Metric(name="active28DayUsers")],
        date_ranges=[DateRange(start_date=f"{start_date}", end_date=f"{end_date}")],
    )

    if session is None:
        async_gen = get_sqlite()
        session = await anext(async_gen)
    
    if session is not None:
        query = select(Ga4ActiveUserData).filter_by(date=end_date)
        existing_data = (await session.execute(query)).first()

        if types == "auto":
            if existing_data:
                await session.close()
                return 'Data is up-to-date!'
            else:
                response = client.run_report(request)
                # delete row data
                await session.execute(
                    delete(Ga4ActiveUserData).filter(Ga4ActiveUserData.date.between(start_date, end_date))
                )
                data = [
                    Ga4ActiveUserData(
                        date=datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d"),
                        platform=row.dimension_values[1].value,
                        active_1day_users=row.metric_values[0].value,
                        active_28day_users=row.metric_values[1].value
                    ) for row in response.rows
                ]
                session.add_all(data)
                await session.commit()
                await session.close()
                return 'Data is being updated!'
        elif types == "manual":
            response = client.run_report(request)
            # delete row data
            await session.execute(
                delete(Ga4ActiveUserData).filter(Ga4ActiveUserData.date.between(start_date, end_date))
            )
            data = [
                Ga4ActiveUserData(
                    date=datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d"),
                    platform=row.dimension_values[1].value,
                    active_1day_users=row.metric_values[0].value,
                    active_28day_users=row.metric_values[1].value
                ) for row in response.rows
            ]
            session.add_all(data)
            await session.commit()
            await session.close()
            return 'Data is being updated!'


def start_scheduler(scheduler):
    # Schedule first task if not already scheduled
    if not any(job.id == 'my_scheduled_task' for job in scheduler.get_jobs()):
        print("my_scheduled_task Added to Jobs!")
        @scheduler.scheduled_job(CronTrigger(hour=10, minute=0), id='my_scheduled_task')
        async def scheduled_task():
            # Your async task here
            start_time = datetime.datetime.today() - datetime.timedelta(60)
            end_time = datetime.datetime.today() - datetime.timedelta(1)
            start_date = start_time.date()
            end_date = end_time.date()

            #  Apple search ads API CREDS
            ACCESS_TOKEN = get_access_token()

            # Running async tasks concurrently using asyncio.gather()
            await usd_idr_to_csv(start_date, end_date, types="auto")
            await fb_api(start_date=start_date, end_date=end_date, campaign_name="FB-BA_UA-Traffic_Web-ID-AON", attribution_window=["7d_click", "1d_view"], types="auto")
            await ga4_event_data(start_date=start_date, end_date=end_date, types="auto")
            await google_reporting(start_date=start_date, end_date=end_date, types="auto")
            await get_asa_campaign_report(start_date=start_date, end_date=end_date, access_token=ACCESS_TOKEN, types="auto")
            await admob_report_api(start_date=start_date, end_date=end_date, types="auto")
            await get_ga4_session(start_date=start_date, end_date=end_date, types="auto")
            await adsense_reprot_api(from_date=start_date, to_date=end_date, types="auto")
            await fb_api(start_date=start_date, end_date=end_date, types="auto")
            await get_google_analytics_data(start_date=start_date, end_date=end_date, types="auto")
            await landing_page_ga4(start_date=start_date, end_date=end_date, types="auto")
            await tiktok_report_API(start_date=start_date, end_date=end_date, types="auto")
            await ga4_active_user(start_date=start_date, end_date=end_date, types="auto")
            print("Update successfully!")

    # Schedule second task if not already scheduled
    if not any(job.id == 'my_scheduled_task_1' for job in scheduler.get_jobs()):
        print("my_scheduled_task_1 Added to Jobs!")
        @scheduler.scheduled_job(CronTrigger(hour=12, minute=0), id='my_scheduled_task_1')
        async def scheduled_task_1():
            # Async tasks for Google Sheets API
            await google_sheet_api(sheet_range='Indexing!A1:CZ5', file='indexing', types="auto")
            await google_sheet_api(sheet_range='Ranking!A1:CZ200', file='ranking', types="auto")
            await google_sheet_api(sheet_range='dau_mau_web!A1:D1000', file='dau_mau_web', types="auto")
            await google_sheet_api(sheet_range='play_console_install!A1:D1000', file='play_console_install', types="auto")
            await google_sheet_api(sheet_range='organic_play_console!A1:R1000', file='organic_play_console', types="auto")
            await google_sheet_api(sheet_range='apple_total_download!A1:C1000', file='apple_total_download', types="auto")
            await google_sheet_api(sheet_range='cost_revenue!A1:D1000', file='cost_revenue', types="auto")
            print("Update Successfully!")

