import streamlit as st
import requests
import datetime
import asyncio
import httpx
from functools import partial
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_sem_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">SEM</h1>""", unsafe_allow_html=True)

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
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"period_sem")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key="sem_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        submit_button = st.button(label="Apply Filters", disabled=False, key="submit_button_sem")
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            params = {
                "from_date": from_date,
                "to_date": to_date
            }
                
            # Use partial application for cleaner task creation
            fetch_data_partial = partial(fetch_data, st, host=host)
            tasks = [
                fetch_data_partial(uri=f'sem', params=params),
                fetch_data_partial(uri=f'sem/daily-growth', params=params),
                fetch_data_partial(uri=f'sem/chart', params=params)
            ]
            try:
                data_text, data_persentase, data_chart = await asyncio.gather(*tasks)
            except httpx.RequestError as e:  # Handle potential exceptions
                st.error(f"Error fetching data: {e}")
                
            card_style(st)
            try:
                if data_text and data_persentase and data_chart:
                    st.markdown(f"""<h1 align="center">Google SEM Performance</h1>""", unsafe_allow_html=True)

                    # -- google sem performance section -- 
                    col1, col2, col3, col4, col5, col6 = st.columns(6)
                    with col1:
                        create_card(
                            st,
                            card_title="Cost Spend",
                            card_value=data_text["google_sem"]["spend"],
                            card_daily_growth=data_persentase["google_sem"]["spend"],
                            class_name="google-sem-spend"
                        )
                    with col2:
                        create_card(
                            st,
                            card_title="Impressions",
                            card_value=data_text["google_sem"]["impressions"],
                            card_daily_growth=data_persentase["google_sem"]["impressions"],
                            class_name="google-sem-impressions"
                        )
                    with col3:
                        create_card(
                            st,
                            card_title="Clicks",
                            card_value=data_text["google_sem"]["clicks"],
                            card_daily_growth=data_persentase["google_sem"]["clicks"],
                            class_name="google-sem-clicks"
                        )
                    with col4:
                        create_card(
                            st,
                            card_title="Click Through Rate (CTR)",
                            card_value=data_text["google_sem"]["ctr"],
                            card_daily_growth=data_persentase["google_sem"]["ctr"],
                            types="persentase",
                            class_name="google-sem-ctr"
                        )
                    with col5:
                        create_card(
                            st,
                            card_title="Cost / Impressions (CPM)",
                            card_value=data_text["google_sem"]["cpm"],
                            card_daily_growth=data_persentase["google_sem"]["cpm"],
                            class_name="google-sem-cpm"
                        )
                    with col6:
                        create_card(
                            st,
                            card_title="Cost / Click (CPC)",
                            card_value=data_text["google_sem"]["cpc"],
                            card_daily_growth=data_persentase["google_sem"]["cpc"],
                            class_name="google-sem-cpc"
                        )

                    # google sem performance chart section
                    col7, col8 = st.columns(2)
                    with col7:
                        create_chart(st, data_chart["google_sem_spend_chart"])
                    with col8:
                        create_chart(st, data_chart["google_sem_metrics_chart"])
                    
                    create_chart(st, data_chart["google_sem_details_table"])

                    # -- google GDN performance section -- 
                    st.markdown(f"""<h1 align="center">Google - GDN Performance</h1>""", unsafe_allow_html=True)
                    col9, col10, col11, col12, col13, col14 = st.columns(6)
                    with col9:
                        create_card(
                            st,
                            card_title="Cost Spend",
                            card_value=data_text["google_gdn"]["spend"],
                            card_daily_growth=data_persentase["google_gdn"]["spend"],
                            class_name="google-awareness-spend"
                        )
                    with col10:
                        create_card(
                            st,
                            card_title="Impressions",
                            card_value=data_text["google_gdn"]["impressions"],
                            card_daily_growth=data_persentase["google_gdn"]["impressions"],
                            class_name="google-awareness-impressions"
                        )
                    with col11:
                        create_card(
                            st,
                            card_title="Clicks",
                            card_value=data_text["google_gdn"]["clicks"],
                            card_daily_growth=data_persentase["google_gdn"]["clicks"],
                            class_name="google-awareness-clicks"
                        )
                    with col12:
                        create_card(
                            st,
                            card_title="Click Through Rate (CTR)",
                            card_value=data_text["google_gdn"]["ctr"],
                            card_daily_growth=data_persentase["google_gdn"]["ctr"],
                            types="persentase",
                            class_name="google-awareness-ctr"
                        )
                    with col13:
                        create_card(
                            st,
                            card_title="Cost / Impressions (CPM)",
                            card_value=data_text["google_gdn"]["cpm"],
                            card_daily_growth=data_persentase["google_gdn"]["cpm"],
                            class_name="google-awareness-cpm"
                        )
                    with col14:
                        create_card(
                            st,
                            card_title="Cost / Click (CPC)",
                            card_value=data_text["google_gdn"]["cpc"],
                            card_daily_growth=data_persentase["google_gdn"]["cpc"],
                            class_name="google-awareness-cpc"
                        )

                    # google GDN performance chart section
                    col15, col16 = st.columns(2)
                    with col15:
                        create_chart(st, data_chart["google_gdn_spend_chart"])
                    with col16:
                        create_chart(st, data_chart["google_gdn_metrics_chart"])
                    
                    create_chart(st, data_chart["google_gdn_details_table"])

                    # -- Facebook display ad performance section -- 
                    st.markdown(f"""<h1 align="center">Facebook Display Ad Performance</h1>""", unsafe_allow_html=True)
                    col17, col18, col19, col20, col21, col22 = st.columns(6)
                    with col17:
                        create_card(
                            st,
                            card_title="Cost Spend",
                            card_value=data_text["facebook_gdn"]["spend"],
                            card_daily_growth=data_persentase["facebook_gdn"]["spend"],
                            class_name="fb-awareness-spend"
                        )
                    with col18:
                        create_card(
                            st,
                            card_title="Impressions",
                            card_value=data_text["facebook_gdn"]["impressions"],
                            card_daily_growth=data_persentase["facebook_gdn"]["impressions"],
                            class_name="fb-awareness-impressions"
                        )
                    with col19:
                        create_card(
                            st,
                            card_title="Clicks",
                            card_value=data_text["facebook_gdn"]["clicks"],
                            card_daily_growth=data_persentase["facebook_gdn"]["clicks"],
                            class_name="fb-awareness-clicks"
                        )
                    with col20:
                        create_card(
                            st,
                            card_title="Click Through Rate (CTR)",
                            card_value=data_text["facebook_gdn"]["ctr"],
                            card_daily_growth=data_persentase["facebook_gdn"]["ctr"],
                            types="persentase",
                            class_name="fb-awareness-ctr"
                        )
                    with col21:
                        create_card(
                            st,
                            card_title="Cost / Impressions (CPM)",
                            card_value=data_text["facebook_gdn"]["cpm"],
                            card_daily_growth=data_persentase["facebook_gdn"]["cpm"],
                            class_name="fb-awareness-cpm"
                        )
                    with col22:
                        create_card(
                            st,
                            card_title="Cost / Click (CPC)",
                            card_value=data_text["facebook_gdn"]["cpc"],
                            card_daily_growth=data_persentase["facebook_gdn"]["cpc"],
                            class_name="fb-awareness-cpc"
                        )

                    # facebook display ad performance chart section
                    col15, col16 = st.columns(2)
                    with col15:
                        create_chart(st, data_chart["facebook_gdn_spend_chart"])
                    with col16:
                        create_chart(st, data_chart["facebook_gdn_metrics_chart"])
                    
                    create_chart(st, data_chart["facebook_gdn_details_table"])

            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")
                if data_persentase.get("message"):
                    st.error(f"Error while feting daily growth data: {data_persentase.get("message", None)}, KeyError: {ke}")
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 
