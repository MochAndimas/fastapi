import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_logger_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">Update Data</h1>""", unsafe_allow_html=True)

    with st.form("logger_form"):
        update_data = {
            None : "",
            "all": "all",
            "currency" : "currency",
            "ga4_event": "ga4_event",
            "googleads": "googleads",
            "asa": "asa",
            "admob": "admob",
            "adsense": "adsense",
            "facebook": "facebook",
            "facebook_gdn": "facebook_gdn",
            "tiktok": "tiktok",
            "ga4_session": "ga4_session",
            "ga4_analytics": "ga4_analytics",
            "ga4_landing_page": "ga4_landing_page",
            "ga4_active_users": "ga4_active_users",
            "indexing": "indexing",
            "ranking": "ranking",
            "dau_mau_web": "dau_mau_web",
            "play_console_install": "play_console_install",
            "organic_play_console": "organic_play_console",
            "apple_total_download": "apple_total_download",
            "cost_revenue": "cost_revenue"
        }
        update_data_options = st.selectbox("Data to update", list(update_data.keys()), placeholder="Choose a data to update!", index=None, key="update-data-api")
        submit_button = st.form_submit_button(label="Apply Filters", disabled=False)
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                params = {
                        "data": update_data[update_data_options]
                    }
                
                data = await fetch_data(st, host=host, uri=f'feature-data/update-external-api', params=params)
                
                # card_style(st)
                if data:
                    st.info(data["message"])

            except Exception as e:
                st.error(f"Error fetching data: {e}") 