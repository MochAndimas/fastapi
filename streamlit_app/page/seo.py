import streamlit as st
import requests
import datetime
import asyncio
import httpx
from functools import partial
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_seo_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">SEO</h1>""", unsafe_allow_html=True)

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
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"period_seo")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key="seo_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        submit_button = st.button(label="Apply Filters", disabled=False, key="submit_button_seo")
    
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
                fetch_data_partial(uri=f'seo', params=params),
                fetch_data_partial(uri=f'seo/daily-growth', params=params),
                fetch_data_partial(uri=f'seo/chart', params=params)
            ]
            try:
                data_text, data_persentase, data_chart = await asyncio.gather(*tasks)
            except httpx.RequestError as e:  # Handle potential exceptions
                st.error(f"Error fetching data: {e}")
                
            card_style(st)
            try:
                if data_text and data_persentase and data_chart:
                    st.markdown(f"""<h1 align="center">Web Traffic Analytics</h1>""", unsafe_allow_html=True)

                    # -- web traffic analytics sections -- 
                    col1, col2, col3, col4, col5 = st.columns(5)
                    
                    with col1:
                        create_card(
                            st,
                            card_title="Sessions",
                            card_value=data_text["sessions"],
                            card_daily_growth=data_persentase['sessions'],
                            class_name="sessions"
                        )
                    with col2:
                        create_card(
                            st, 
                            card_title="Organic Search Sessions",
                            card_value=data_text["source"],
                            card_daily_growth=data_persentase["source"],
                            class_name="source"
                        )
                    with col3:
                        create_card(
                            st,
                            card_title="Users",
                            card_value=data_text["total_user"],
                            card_daily_growth=data_persentase["total_user"],
                            class_name="total-user"
                        )
                    with col4:
                        create_card(
                            st,
                            card_title="new Users",
                            card_value=data_text["new_user"],
                            card_daily_growth=data_persentase["new_user"],
                            class_name="new-user"
                        )
                    with col5:
                        create_card(
                            st,
                            card_title="Bounce Rate",
                            card_value=data_text["bounce_rate"],
                            card_daily_growth=data_persentase["bounce_rate"],
                            types="persentase",
                            class_name="bounce-rate"
                        )
                    
                    # -- web traffic analytics chart section -- 
                    create_chart(st, data_chart["metrics_chart"])
                    create_chart(st, data_chart["web_traffic_chart"])


                    # -- demographics chart section -- 
                    st.markdown(f"""<h1 align="center">Demographics</h1>""", unsafe_allow_html=True)
                    col6, col7 = st.columns(2)
                    with col6:
                        create_chart(st, data_chart["source_chart"])
                    with col7:
                        create_chart(st, data_chart["device_chart"])
                    
                    col8, col9 = st.columns(2)
                    with col8:
                        create_chart(st, data_chart["landing_page_organic"])
                    with col9:
                        create_chart(st, data_chart['landing_page_cpc'])

                    # -- seo analytics sections -- 
                    st.markdown(f"""<h1 align="center">SEO Analytics Periode: {data_text["periode"]}</h1>""", unsafe_allow_html=True)
                    col10, col11, col12, col13 = st.columns(4)
                    with col10:
                        create_card(
                            st,
                            card_title="Domain Rank",
                            card_value=data_text["dr"],
                            card_daily_growth=data_persentase["dr"],
                            class_name="domain-rank"
                        )
                    with col11:
                        create_card(
                            st,
                            card_title="Indexing",
                            card_value=data_text["indexing"],
                            card_daily_growth=data_persentase["indexing"],
                            class_name="indexing"
                        )
                    with col12:
                        create_card(
                            st,
                            card_title="Backlinks",
                            card_value=data_text["backlinks"],
                            card_daily_growth=data_persentase["backlinks"],
                            class_name="backlinks"
                        )
                    with col13:
                        create_card(
                            st,
                            card_title="Referring Domain",
                            card_value=data_text["ref_domain"],
                            card_daily_growth=data_persentase['ref_domain'],
                            class_name="ref-domainr"
                        )

                    col14, col15, col16, col17 = st.columns(4)
                    with col14:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Keywords",
                            card_value=data_text["total_keywords"]
                        )
                    with col15:
                        create_card(
                            st,
                            card_title="Rank 1-3 Keywords",
                            card_value=data_text["rank_1_3"],
                            card_daily_growth=data_persentase["rank_1_3"],
                            class_name="rank-1-3"
                        )
                    with col16:
                        create_card(
                            st, 
                            card_title="Rank 4-10 Keywords",
                            card_value=data_text["rank_4_10"],
                            card_daily_growth=data_persentase["rank_4_10"],
                            class_name="rank-4-10"
                        )
                    with col17:
                        create_card(
                            st,
                            card_title="Rank 11-30 Keywords",
                            card_value=data_text["rank_11_30"],
                            card_daily_growth=data_persentase["rank_11_30"],
                            class_name="rank-11-30"
                        )

                    # -- seo analytics chart section --
                    create_chart(st, data_chart["ranking_chart"])

            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")
                if data_persentase.get("message"):
                    st.error(f"Error while feting daily growth data: {data_persentase.get("message", None)}, KeyError: {ke}")
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 
