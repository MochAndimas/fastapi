import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_offline_mode_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">Offline Mode</h1>""", unsafe_allow_html=True)

    with st.container(border=True):
        # Calculate preset date ranges
        today = datetime.date.today()
        this_month_start = today.replace(day=1)
        last_month_start = (this_month_start - datetime.timedelta(days=1)).replace(day=1)
        last_month_end = this_month_start - datetime.timedelta(days=1)
        this_week_start = today - datetime.timedelta(days=today.weekday())
        last_week_start = this_week_start - datetime.timedelta(days=7)  # Monday of the previous week
        last_week_end = this_week_start - datetime.timedelta(days=1)    # Last Sunday
        last_7days_start = today - datetime.timedelta(days=7)
        last_7days_end  = today - datetime.timedelta(days=1)
        preset_date = {
            None: (last_week_start, last_week_end),
            "Custom Range" : "custom_range",
            "This Month" : (this_month_start, today),
            "Last Month" : (last_month_start, last_month_end),
            "This Week" : (this_week_start, today),
            "Last Week" : (last_week_start, last_week_end),
            "Last 7 Days": (last_7days_start, last_7days_end) 
        }
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"period_offline_mode")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key="offline_mode_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        submit_button = st.button(label="Apply Filters", disabled=False, key="submit_button_offline_mode")
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                params = {
                        "from_date": from_date,
                        "to_date": to_date
                    }
                
                data = await fetch_data(st, host=host, uri=f'feature-data/offline-mode', params=params)
                
                card_style(st)
                if data:
                    st.markdown(f"""<h1 align="center">Offline Mode By Day</h1>""", unsafe_allow_html=True)
                    
                    # -- offline mode by day section --
                    col1, col2 = st.columns(2)
                    with col1:
                        create_card(
                            st, 
                            card_title="Unique Users Offline Mode",
                            card_value=data["metrics_data"]["unique_user"],
                            card_daily_growth=data["metrics_daily_growth"]["unique_user"],
                            class_name="unique-user"
                        )
                    with col2:
                        create_card(
                            st,
                            card_title="Event Count Offline Mode",
                            card_value=data["metrics_data"]["event_count"],
                            card_daily_growth=data["metrics_daily_growth"]["event_count"],
                            class_name="event-count"
                        )
                    # -- offline mode chart section --
                    col3, col4 = st.columns(2)
                    with col3:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data["unique_bar_chart"]))
                    with col4:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data["count_bar_chart"]))

            except KeyError as ke:
                if data.get("message"):
                    st.error(f"Error while feting metrics data: {data.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 