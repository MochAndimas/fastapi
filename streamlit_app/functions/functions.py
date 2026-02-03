import logging
import httpx
import streamlit as st
from datetime import datetime, timedelta
from sqlalchemy import select
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from jose import jwt
from app.db.models.user import UserToken
from jose.exceptions import ExpiredSignatureError, JWTError
from sqlalchemy.orm import Session, sessionmaker
from streamlit_cookies_controller import CookieController
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
from dateutil.relativedelta import relativedelta
import plotly.io as pio
cookie_controller = CookieController()


# streamlit engine and sessionmaker
streamlit_engine = create_engine(
    "sqlite:///./app/db/external_api.db",
    echo=False,
    poolclass=StaticPool,
    pool_pre_ping=True
)
streamlit_session = sessionmaker(
    bind=streamlit_engine,
    expire_on_commit=False,
    class_=Session
)
def get_streamlit():
    """ """
    with streamlit_session() as session:
        try:
            yield session
        finally:
            session.close()


def get_user(user_id):
    """ """
    session_gen = get_streamlit()
    session = next(session_gen)
    with session.begin():
        query = select(UserToken).filter_by(user_id=user_id)
        data = session.execute(query).scalars().first()
    session.close()
    return data


def get_session(session_id):
    """Retrieve session details from the SQLite database."""
    session_generator = get_streamlit()
    session = next(session_generator)
    with session.begin():
        query = select(UserToken).filter_by(session_id=session_id)
        existing_data = session.execute(query)
        user = existing_data.scalars().first()
        if user != None:
            if datetime.now() <= user.expiry and not user.is_revoked:
                st.session_state.role = user.role
                st.session_state.logged_in = user.logged_in
                st.session_state._user_id = user.user_id
                st.session_state.page = user.page
            else:
                user.is_revoked = True
                user.logged_in = False
                session.commit()
                
                cookie_controller.set("session_id", "", max_age=0)
                del st.session_state.logged_in
                del st.session_state.page
                del st.session_state._user_id
                del st.session_state.role
                st.toast("Session is expired! Please Re Log In.")
            session.close()
        return user


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
    if types == "txt":
        return "{:,.0f}".format(text)
    elif types == "rp":
        return "Rp. {:,.0f}".format(text)
    elif types == "persentase":
        return "{:.2%}".format(text)
    elif types == "plain":
        return text


def is_token_expired(st, token, verify=True):
    """
    Checks if a JWT token is expired.

    Args:
        token (str): The JWT token to verify.
        secret_key (str): The secret key used to sign the token.
        verify (bool, optional): Whether to verify the token's signature. Defaults to True.

    Returns:
        bool: True if the token is expired or invalid, False otherwise.
    """
    key = st.secrets['key']['JWT_SECRET_KEY']
    try:
        jwt.decode(
            token,
            f"b'{key}'",
            algorithms=["HS256"],  # Adjust if you use different algorithms
            options={"verify_signature": verify}  # Control signature verification
        )
        print(1)
        return False  # Token is valid
    except ExpiredSignatureError:
        print(2)
        return True  # Token is expired
    except JWTError as e:
        st.error(f'Invalid Token : {e}')
        return f'Invalid Token: {e}'  # Token is invalid for other reasons (e.g., wrong key, bad format)


async def fetch_data(st, host, uri, params):
    """
    Fetches data from a protected API endpoint, handling token refresh and errors gracefully.
    
    Args:
        st: The session state or application state object.
        host (str): The base URL of the API host.
        uri (str): The specific endpoint URI.
        data (dict): Data to be sent in the request.
    
    Returns:
        dict: The JSON response from the API or an error message.
    """
    try:
        user = get_user(st.session_state._user_id)
        url = f"{host}/api/{uri}"
        headers = {
            "Authorization": f"Bearer {user.access_token}",
        }
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)
            return response.json()
        
    except HTTPError as http_error:
        logging.error(f"HTTP error occurred: {http_error}")
        return {"message": f"HTTP error: {http_error}"}
    except (ConnectionError, Timeout) as conn_error:
        logging.error(f"Connection error or timeout: {conn_error}")
        return {"message": f"Connection error or timeout: {conn_error}"}
    except RequestException as req_error:
        logging.error(f"Request failed: {req_error}")
        return {"message": f"Request failed: {req_error}"}
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return {"message": f"An unexpected error occurred: {e}"}


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

            
async def logout(st, host, session_id):
    """
    Handles the logout process, clearing session state and redirecting the user.

    Args:
        st: Streamlit object for interacting with the app.
        host (str): Base URL of the API.
    """
    
    if st.button("Log Out", use_container_width=True):
        with st.spinner("Logging out..."):
            try:
                user = get_user(st.session_state._user_id)
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f"Bearer {user.access_token}"
                }
                async with httpx.AsyncClient(timeout=120) as client:
                    response = await client.post(f"{host}/api/logout", headers=headers)
                    response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)
                    data =  response.json()
                
                if data.get('success'):
                    # Clear session state
                    cookie_controller.set("session_id", "", max_age=0)
                    del st.session_state.logged_in
                    del st.session_state.page
                    del st.session_state._user_id
                    del st.session_state.role
                        
                    st.success("Logged out successfully!")
                    st.rerun()  # Redirect to login page (or home page)
                else:
                    error_message = data.get('message', "Logout failed")
                    st.error(error_message)

            except RequestException as e:
                st.error(f"An error occurred during logout: {e}. Please try again later.")


def footer(st):
    """Renders a styled footer at the bottom of the Streamlit app."""

    # Using a template string for better readability
    footer_html = f"""
    <style>
        .footer {{
            position: fixed;
            left: 0;
            bottom: 0;
            width: 100%;
            padding: 10px;
            text-align: right;
            font-size: 14px;
            color: #666; /* Slightly darker text */
        }}
    </style>
    <div class="footer">
        <p>© {datetime.now().year}, made with 💰</p> 
    </div>
    """

    st.markdown(footer_html, unsafe_allow_html=True)


def card_style(st):
    """Defines a reusable CSS style for cards within a Streamlit app."""

    # Using a template string for better readability and maintainability
    card_style_html = """
    <style>
        .card {
            background-color: #f5f5f5; 
            padding: 15px; /* Slightly increased padding for better visual spacing */
            border-radius: 10px; 
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1); /* Subtle shadow for depth */
            margin-bottom: 20px; /* Add spacing between cards */
        }

        .card-title {
            font-size: 0.8rem; /* Use relative units for better responsiveness */
            font-weight: bold;
            text-align: center; 
            margin-bottom: 10px; /* Add space between title and value */
        }

        .card-value {
            font-size: 1.2rem; /* Use relative units for better responsiveness */
            font-weight: bold;
            text-align: center; 
        }
    </style>
    """

    st.markdown(card_style_html, unsafe_allow_html=True)


def style_daily_growth(st, value, class_name):
    """
    Applies conditional styling to a daily growth value in a Streamlit app.
    """

    if value.startswith("-"):
        color = "red"
    else:
        color = "#00FF00"  # Green

    styling = f"""
    <style>
        .{class_name} {{
            font-size: 12px;
            color: {color};
            text-align: center;
        }}
    </style>
    """

    st.markdown(styling, unsafe_allow_html=True)
        

def card(st, card_title, card_value, card_daily_growth, class_name):
    """Renders a card with title, value, and daily growth."""
    st.markdown(f'<p class="card-title">{card_title}</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="card-value">{card_value}</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="{class_name}">{card_daily_growth} From Last Period</p>', unsafe_allow_html=True)


def create_card(st, card_title, card_value, types="txt", card_daily_growth="0", class_name="-", style="with_daily_growth", caption=False, caption_title=""):
    """
    Creates a styled card with optional daily growth and caption.
    """
    if style == "with_daily_growth":
        with st.container(border=True):
            style_daily_growth(
                st,
                value=str_converter(card_daily_growth, types="persentase"),
                class_name=class_name
            )
            card(
                st,
                card_title=card_title,
                card_value=str_converter(card_value, types=types),
                card_daily_growth=str_converter(card_daily_growth, types="persentase"),
                class_name=class_name
            )
            if caption:
                st.caption(caption_title)
    elif style == "plain":
        with st.container(border=True):
            st.markdown(f'<p class="card-title">{card_title}</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="card-value">{str_converter(card_value, types=types)}</p>', unsafe_allow_html=True)
            if caption:
                st.caption(caption_title)


def create_chart(st, data, caption=False, caption_title=''):
    """
    Creates a Plotly chart within a Streamlit container, with optional caption and title.
    """
    with st.container(border=True):
        st.plotly_chart(pio.from_json(data))
        if caption:
            st.caption(caption_title)
