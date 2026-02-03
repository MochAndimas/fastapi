import streamlit as st
import requests
import datetime
import asyncio
import httpx
from functools import partial
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_new_install_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    last3day = datetime.datetime.today() - datetime.timedelta(3)
    las9days = datetime.datetime.today() - datetime.timedelta(9)
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">New Install</h1>""", unsafe_allow_html=True)

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
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"period_new_install")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key="new_install_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        submit_button = st.button(label="Apply Filters", disabled=False, key="submit_button_new_install")
    
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
                fetch_data_partial(uri=f'new-install', params=params),
                fetch_data_partial(uri=f'new-install/daily-growth', params=params),
                fetch_data_partial(uri=f'new-install/chart', params=params)
            ]
            try:
                data_text, data_persentase, data_chart = await asyncio.gather(*tasks)
            except httpx.RequestError as e:  # Handle potential exceptions
                st.error(f"Error fetching data: {e}")
                
            card_style(st)
            try:
                if data_text and data_persentase and data_chart:
                    st.markdown(f"""<h1 align="center">Install By Source</h1>""", unsafe_allow_html=True)

                    # -- install by source section --
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        create_card(
                            st, 
                            card_title="Total Install",
                            card_value=data_text["install_all"]["total_install"],
                            card_daily_growth=data_persentase["install_all"]["total_install"],
                            class_name="total-new-install"
                        )
                    with col2:
                        create_card(
                            st, 
                            card_title="Android Install",
                            card_value=data_text["install_all"]["android_install"],
                            card_daily_growth=data_persentase["install_all"]["android_install"],
                            class_name="android-install"
                        )
                    with col3:
                        create_card(
                            st,
                            card_title="IOS Install",
                            card_value=data_text["install_all"]["apple_install"],
                            card_daily_growth=data_persentase["install_all"]["apple_install"],
                            class_name="ios-install"
                        )

                    # -- install by source chart sections --
                    col4, col5 = st.columns(2)
                    with col4:
                        create_chart(st, data_chart['source_table'])
                    with col5:
                        create_chart(st, data_chart['source_chart'])
                    
                    # -- facebook ads performance section
                    st.markdown(f"""<h1 align="center">Facebook Ads Performance</h1>""", unsafe_allow_html=True)

                    col6, col7, col8, col9, col10 = st.columns(5)
                    with col6:
                        create_card(
                            st, 
                            card_title="Cost Spend",
                            card_value=data_text["facebook_performance"]["spend"],
                            card_daily_growth=data_persentase["facebook_performance"]["spend"],
                            types="rp",
                            class_name="fb-spend"
                        )
                    with col7:
                        create_card(
                            st,
                            card_title="Impressions",
                            card_value=data_text["facebook_performance"]["impressions"],
                            card_daily_growth=data_persentase["facebook_performance"]["impressions"],
                            class_name="fb-impressions"
                        )
                    with col8:
                        create_card(
                            st,
                            card_title="Clicks",
                            card_value=data_text["facebook_performance"]["clicks"],
                            card_daily_growth=data_persentase["facebook_performance"]["clicks"],
                            class_name="fb-clicks"
                        )
                    with col9:
                        create_card(
                            st,
                            card_title="Installs",
                            card_value=data_text["facebook_performance"]["install"],
                            card_daily_growth=data_persentase["facebook_performance"]["install"],
                            class_name="fb-installs"
                        )
                    with col10:
                        create_card(
                            st, 
                            card_title="Cost/Installs",
                            card_value=data_text["facebook_performance"]["cost_install"],
                            card_daily_growth=data_persentase["facebook_performance"]["cost_install"],
                            types="rp",
                            class_name="fb-cost-install"
                        )

                    # -- facebook ads performance ads chart --
                    col11, col12 = st.columns(2)
                    with col11:
                        create_chart(st, data_chart["fb_chart"])
                    with col12:
                        create_chart(st, data_chart["fb_install_chart"])

                    create_chart(st, data_chart["fb_table"])

                    # -- google ads performance ads chart -- 
                    st.markdown(f"""<h1 align="center">Google Ads Performance</h1>""", unsafe_allow_html=True)
                    
                    col13, col14, col15, col16, col17 = st.columns(5)
                    with col13:
                        create_card(
                            st, 
                            card_title="Cost Spend",
                            card_value=data_text["google_performance"]["spend"],
                            card_daily_growth=data_persentase["google_performance"]["spend"],
                            types="rp",
                            class_name="ggl-spend"
                        )
                    with col14:
                        create_card(
                            st,
                            card_title="Impressions",
                            card_value=data_text["google_performance"]["impressions"],
                            card_daily_growth=data_persentase["google_performance"]["impressions"],
                            class_name="ggl-impressions"
                        )
                    with col15:
                        create_card(
                            st,
                            card_title="Clicks",
                            card_value=data_text["google_performance"]["clicks"],
                            card_daily_growth=data_persentase["google_performance"]["clicks"],
                            class_name="ggl-clicks"
                        )
                    with col16:
                        create_card(
                            st,
                            card_title="Installs",
                            card_value=data_text["google_performance"]["install"],
                            card_daily_growth=data_persentase["google_performance"]["install"],
                            class_name="ggl-installs"
                        )
                    with col17:
                        create_card(
                            st, 
                            card_title="Cost/Installs",
                            card_value=data_text["google_performance"]["cost_install"],
                            card_daily_growth=data_persentase["google_performance"]["cost_install"],
                            types="rp",
                            class_name="ggl-cost-install"
                        )

                    # -- google ads performance ads chart --
                    col18, col19 = st.columns(2)
                    with col18:
                        create_chart(st, data_chart["ggl_chart"])
                    with col19:
                        create_chart(st, data_chart["ggl_install_chart"])

                    create_chart(st, data_chart["ggl_table"])

                    # -- tiktok ads performance ads chart -- 
                    st.markdown(f"""<h1 align="center">TikTok Ads Performance</h1>""", unsafe_allow_html=True)
                    
                    col20, col21, col22, col23, col24 = st.columns(5)
                    with col20:
                        create_card(
                            st, 
                            card_title="Cost Spend",
                            card_value=data_text["tiktok_performance"]["spend"],
                            card_daily_growth=data_persentase["tiktok_performance"]["spend"],
                            types="rp",
                            class_name="tiktok-spend"
                        )
                    with col21:
                        create_card(
                            st,
                            card_title="Impressions",
                            card_value=data_text["tiktok_performance"]["impressions"],
                            card_daily_growth=data_persentase["tiktok_performance"]["impressions"],
                            class_name="tiktok-impressions"
                        )
                    with col22:
                        create_card(
                            st,
                            card_title="Clicks",
                            card_value=data_text["tiktok_performance"]["clicks"],
                            card_daily_growth=data_persentase["tiktok_performance"]["clicks"],
                            class_name="tiktok-clicks"
                        )
                    with col23:
                        create_card(
                            st,
                            card_title="Installs",
                            card_value=data_text["tiktok_performance"]["install"],
                            card_daily_growth=data_persentase["tiktok_performance"]["install"],
                            class_name="tiktok-installs"
                        )
                    with col24:
                        create_card(
                            st, 
                            card_title="Cost/Installs",
                            card_value=data_text["tiktok_performance"]["cost_install"],
                            card_daily_growth=data_persentase["tiktok_performance"]["cost_install"],
                            types="rp",
                            class_name="tiktok-cost-install"
                        )

                    # -- tiktok ads performance ads chart --
                    col25, col26 = st.columns(2)
                    with col25:
                        create_chart(st, data_chart["chart_tiktok_cost_installs"])
                    with col26:
                        create_chart(st, data_chart["chart_tiktok_installs"])

                    create_chart(st, data_chart["table_tiktok_campaign"])

                    # -- asa ads performance ads chart -- 
                    st.markdown(f"""<h1 align="center">Apple Search Ads Performance</h1>""", unsafe_allow_html=True)
                    
                    col27, col28, col29, col30, col31 = st.columns(5)
                    with col27:
                        create_card(
                            st, 
                            card_title="Cost Spend",
                            card_value=data_text["asa_performance"]["spend"],
                            card_daily_growth=data_persentase["asa_performance"]["spend"],
                            types="rp",
                            class_name="asa-spend"
                        )
                    with col28:
                        create_card(
                            st,
                            card_title="Impressions",
                            card_value=data_text["asa_performance"]["impressions"],
                            card_daily_growth=data_persentase["asa_performance"]["impressions"],
                            class_name="asa-impressions"
                        )
                    with col29:
                        create_card(
                            st,
                            card_title="Taps",
                            card_value=data_text["asa_performance"]["clicks"],
                            card_daily_growth=data_persentase["asa_performance"]["clicks"],
                            class_name="asa-taps"
                        )
                    with col30:
                        create_card(
                            st,
                            card_title="Installs",
                            card_value=data_text["asa_performance"]["install"],
                            card_daily_growth=data_persentase["asa_performance"]["install"],
                            class_name="asa-installs"
                        )
                    with col31:
                        create_card(
                            st, 
                            card_title="Cost/Installs",
                            card_value=data_text["asa_performance"]["cost_install"],
                            card_daily_growth=data_persentase["asa_performance"]["cost_install"],
                            types="rp",
                            class_name="asa-cost-install"
                        )

                    # -- asa ads performance ads chart --
                    col32, col33 = st.columns(2)
                    with col32:
                        create_chart(st, data_chart["chart_asa_cost_install"])
                    with col33:
                        create_chart(st, data_chart["chart_asa_install"])

                    create_chart(st, data_chart["table_asa"])

                    # -- organic install section -- 
                    col34, col35, col36 = st.columns(3)
                    with col34:
                        create_card(
                            st,
                            card_title="Total Installs",
                            card_value=data_text["install_all"]["total_organic"],
                            card_daily_growth=data_persentase["install_all"]["total_organic"],
                            class_name="total-organic-install"
                        )
                    with col35:
                        create_card(
                            st, 
                            card_title="Android Install",
                            card_value=data_text["install_all"]["android_organic"],
                            card_daily_growth=data_persentase["install_all"]["android_organic"],
                            class_name="android-organic"
                        )
                    with col36:
                        create_card(
                            st,
                            card_title="IOS Install",
                            card_value=data_text["install_all"]["apple_organic"],
                            card_daily_growth=data_persentase["install_all"]["apple_organic"],
                            class_name="ios-organic"
                        )
                    
                    create_chart(st, data_chart["chart_aso"])

            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")
                if data_persentase.get("message"):
                    st.error(f"Error while feting daily growth data: {data_persentase.get("message", None)}, KeyError: {ke}")
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 