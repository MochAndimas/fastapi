import streamlit as st
import requests
import datetime
import asyncio
import httpx
from functools import partial
from streamlit_app.functions.functions import get_date_range, fetch_data
from streamlit_app.functions.functions import create_card, create_chart, card_style

import plotly.io as pio

async def show_overall_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">Overview Data</h1>""", unsafe_allow_html=True)

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
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"period_overview")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key="overview_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        submit_button = st.button(label="Apply Filters", disabled=False, key="submit_button_overview")
    
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
                fetch_data_partial(uri=f'overview', params=params),
                fetch_data_partial(uri=f'overview/daily-growth', params=params),
                fetch_data_partial(uri=f'overview/chart', params=params)
            ]
            try:
                data_text, data_dg, data_chart = await asyncio.gather(*tasks)
            except httpx.RequestError as e:  # Handle potential exceptions
                st.error(f"Error fetching data: {e}")
                
            card_style(st)
            try:
                if all([
                    data_text, data_dg, data_chart
                ]):
                    st.markdown(f"""<h1 align="center">Overall</h1>""", unsafe_allow_html=True)
                    
                    # -- install, dau, mau section -- 
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        create_card(
                            st,
                            card_title="Install",
                            card_value=data_text["install"]["total_install"],
                            card_daily_growth=data_dg["install"]["total_install"],
                            class_name="overall-install",
                            caption=True,
                            caption_title="*Data Delayed 2 Days from today date"
                        )
                    with col2:
                        create_card(
                            st,
                            card_title="Android Install",
                            card_value=data_text["install"]["android_install"],
                            card_daily_growth=data_dg["install"]["android_install"],
                            class_name="android-install",
                            caption=True,
                            caption_title="*Data Delayed 2 Days from today date"
                        )
                    with col3:
                        create_card(
                            st,
                            card_title="IOS Install",
                            card_value=data_text["install"]["apple_install"],
                            card_daily_growth=data_dg["install"]["apple_install"],
                            class_name="ios-install",
                            caption=True,
                            caption_title="*Data Delayed 2 Days from today date"
                        )
                    
                    # -- register section -- 
                    col4, col5, col6 = st.columns(3)
                    with col4:
                        create_card(
                            st, 
                            card_title="Total Register",
                            card_value=data_text["total_register"],
                            card_daily_growth=data_dg["total_register"],
                            class_name="total-register"
                        )
                    with col5:
                        create_card(
                            st, 
                            card_title="App register",
                            card_value=data_text["app_register"],
                            card_daily_growth=data_dg["app_register"],
                            class_name="app-register"
                        )
                    with col6:
                        create_card(
                            st,
                            card_title="Web Register",
                            card_value=data_text["web_register"],
                            card_daily_growth=data_dg["web_register"],
                            class_name="web-register"
                        )
                    
                    # -- moengaeg active user app
                    st.markdown(f"""<h1 align="center">FireBase Active User App</h1>""", unsafe_allow_html=True)
                    col7, col8 = st.columns(2)
                    with col7:
                        create_card(
                            st, 
                            card_title="Last Day Stickiness",
                            card_value=data_text["app_stickieness"]["last_day"],
                            card_daily_growth=data_dg["app_stickieness"]["last_day"],
                            types="persentase",
                            class_name="app-stickiness"
                        )
                    with col8:
                        create_card(
                            st,
                            card_title="Average Stickiness",
                            card_value=data_text["app_stickieness"]["average"],
                            card_daily_growth=data_dg["app_stickieness"]["average"],
                            types="persentase",
                            class_name="app-avg-stickiness"
                        )
                    
                    create_chart(st, data_chart["ga4_dau_mau"])

                    # -- moengaeg active user web
                    st.markdown(f"""<h1 align="center">FireBase Active User Web</h1>""", unsafe_allow_html=True)
                    col7, col8 = st.columns(2)
                    with col7:
                        create_card(
                            st, 
                            card_title="Last Day Stickiness",
                            card_value=data_text["web_stickieness"]["last_day"],
                            card_daily_growth=data_dg["web_stickieness"]["last_day"],
                            types="persentase",
                            class_name="web-stickiness"
                        )
                    with col8:
                        create_card(
                            st,
                            card_title="Average Stickiness",
                            card_value=data_text["web_stickieness"]["average"],
                            card_daily_growth=data_dg["web_stickieness"]["average"],
                            types="persentase",
                            class_name="web-avg-stickiness"
                        )
                    
                    create_chart(st, data_chart["web_ga4_dau_mau"])

                    create_chart(st, data_chart["chart_install"], caption=True, caption_title='*Data Delayed 2 Days from today date')

                    create_chart(st, data_chart["payment"])

                    # -- app user activity --
                    st.markdown(f"""<h1 align="center">App User Activity</h1>""", unsafe_allow_html=True)
                    col9, col10, col11 = st.columns(3)
                    with col9:
                        create_card(
                            st, 
                            card_title="App Chapter Reader Guest (Unique)",
                            card_value=data_text["app_chapter_read"]["unique_guest_reader"],
                            card_daily_growth=data_dg["app_chapter_data"]["chapter_read_data"]["unique_guest_reader"],
                            class_name="guest-reader"
                        )
                    with col10:
                        create_card(
                            st,
                            card_title="App Chapter Reader Register (Unique)",
                            card_value=data_text["app_chapter_read"]["unique_register_reader"],
                            card_daily_growth=data_dg["app_chapter_data"]["chapter_read_data"]["unique_register_reader"],
                            class_name="register-reader"
                        )
                    with col11:
                        create_card(
                            st,
                            card_title="App Total Chapter Readers (Unique)",
                            card_value=data_text["app_chapter_read"]["pembaca_chapter_unique"],
                            card_daily_growth=data_dg["app_chapter_data"]["chapter_read_data"]["pembaca_chapter_unique"],
                            class_name="total-reader"
                        )

                    col12, col13 = st.columns(2)
                    with col12:
                        create_card(
                            st,
                            card_title="App Unique Coin Purchase",
                            card_value=data_text["app_revenue"]["coin_unique"],
                            card_daily_growth=data_dg["app_revenue_data"]["coin_unique"],
                            class_name="unique-coin"
                        )
                    with col13:
                        create_card(
                            st,
                            card_title="App Coin Purchase",
                            card_value=data_text["app_revenue"]["coin_count"],
                            card_daily_growth=data_dg["app_revenue_data"]["coin_count"],
                            class_name="count-coin"
                        )

                    col14, col15, col16, col17, col18, col19 = st.columns(6)
                    with col14:
                        create_card(
                            st,
                            card_title="App Chapter Purchase With Koin Unique",
                            card_value=data_text["app_chapter_coin"]["chapter_unique"],
                            card_daily_growth=data_dg["app_chapter_data"]["chapter_coin_data"]["chapter_unique"],
                            class_name="app-pembeli-chapter-koin-unique"
                        )
                    with col15:
                        create_card(
                            st,
                            card_title="App Chapter Purchase With AdsKoin Unique",
                            card_value=data_text["app_chapter_adscoin"]["chapter_unique"],
                            card_daily_growth=data_dg["app_chapter_data"]["chapter_adscoin_data"]["chapter_unique"],
                            class_name="app-pembeli-chapter-adskoin-unique"
                        )
                    with col16:
                        create_card(
                            st,
                            card_title="App Chapter Purchase With Ads Unique",
                            card_value=data_text["app_chapter_ads"]["chapter_unique"],
                            card_daily_growth=data_dg["app_chapter_data"]["chapter_ads_data"]["chapter_unique"],
                            class_name="app-pembeli-chapter-admob-unique"
                        )
                    with col17:
                        create_card(
                            st,
                            card_title="App Chpater Purchase with Koin Count",
                            card_value=data_text["app_chapter_coin"]["chapter_count"],
                            card_daily_growth=data_dg["app_chapter_data"]["chapter_coin_data"]["chapter_count"],
                            class_name="app-pembeli-chapter-koin-count"
                        )
                    with col18:
                        create_card(
                            st,
                            card_title="App Chapter Purchase With AdsKoin Count",
                            card_value=data_text["app_chapter_adscoin"]["chapter_count"],
                            card_daily_growth=data_dg["app_chapter_data"]["chapter_adscoin_data"]["chapter_count"],
                            class_name="app-pembeli-chapter-adskoin-count"
                        )
                    with col19:
                        create_card(
                            st,
                            card_title="App Chapter Purchase With Ads Count",
                            card_value=data_text["app_chapter_ads"]["chapter_count"],
                            card_daily_growth=data_dg["app_chapter_data"]["chapter_ads_data"]["chapter_count"],
                            class_name="app-pembeli-chapter-admob-count"
                        )

                    col20, col21 = st.columns(2)
                    with col20:
                        create_card(
                            st,
                            card_title="App Total Pembeli Chapter Unique",
                            card_value=data_text["app_total_chapter_purchase"]["unique"],
                            card_daily_growth=data_dg["app_chapter_data"]["overall_chapter_purchase"]["unique"],
                            class_name="app-total-pembeli-chapter-unique"
                        )
                    with col21:
                        create_card(
                            st,
                            card_title="App Total Pembeli Chapter Count",
                            card_value=data_text["app_total_chapter_purchase"]["count"],
                            card_daily_growth=data_dg["app_chapter_data"]["overall_chapter_purchase"]["count"],
                            class_name="app-total-pembeli-chapter-count"
                        )
                    
                    create_chart(st, data_chart["user_journey_chart"])

                    # -- web user activity -- 
                    st.markdown(f"""<h1 align="center">Web User Activity</h1>""", unsafe_allow_html=True)
                    col22, col23, col24 = st.columns(3)
                    with col22:
                        create_card(
                            st, 
                            card_title="Web Chapter Reader Guest (Unique)",
                            card_value=data_text["web_chapter_read"]["unique_guest_reader"],
                            card_daily_growth=data_dg["web_chapter_data"]["chapter_read_data"]["unique_guest_reader"],
                            class_name="web-reader-guest"
                        )
                    with col23:
                        create_card(
                            st,
                            card_title="Web Chapter Reader Register (Unique)",
                            card_value=data_text["web_chapter_read"]["unique_register_reader"],
                            card_daily_growth=data_dg["web_chapter_data"]["chapter_read_data"]["unique_register_reader"],
                            class_name="web-reader-register"
                        )
                    with col24:
                        create_card(
                            st,
                            card_title="Web Total Chapter Reader (Unique)",
                            card_value=data_text["web_chapter_read"]["pembaca_chapter_unique"],
                            card_daily_growth=data_dg["web_chapter_data"]["chapter_read_data"]["pembaca_chapter_unique"],
                            class_name="web-total-register"
                        )

                    col25, col26 = st.columns(2)
                    with col25:
                        create_card(
                            st,
                            card_title="Web Unique Coin Purchase",
                            card_value=data_text["web_revenue"]["coin_unique"],
                            card_daily_growth=data_dg["web_revenue_data"]["coin_unique"],
                            class_name="web-unique-coin"
                        )
                    with col26:
                        create_card(
                            st,
                            card_title="Web Coin Purchase",
                            card_value=data_text["web_revenue"]["coin_count"],
                            card_daily_growth=data_dg["web_revenue_data"]["coin_count"],
                            class_name="web-count-coin"
                        )

                    col27, col28, col29, col30, col31, col32 = st.columns(6)
                    with col27:
                        create_card(
                            st,
                            card_title="Web Chapter Purchase With Koin Unique",
                            card_value=data_text["web_chapter_coin"]["chapter_unique"],
                            card_daily_growth=data_dg["web_chapter_data"]["chapter_coin_data"]["chapter_unique"],
                            class_name="pembeli-chapter-koin-unique"
                        )
                    with col28:
                        create_card(
                            st,
                            card_title="Web Chapter Purchase With AdsKoin Unique",
                            card_value=data_text["web_chapter_adscoin"]["chapter_unique"],
                            card_daily_growth=data_dg["web_chapter_data"]["chapter_adscoin_data"]["chapter_unique"],
                            class_name="pembeli-chapter-adskoin-unique"
                        )
                    with col29:
                        create_card(
                            st,
                            card_title="Web Chapter Purchase With Ads Unique",
                            card_value=data_text["web_chapter_ads"]["chapter_unique"],
                            card_daily_growth=data_dg["web_chapter_data"]["chapter_ads_data"]["chapter_unique"],
                            class_name="pembeli-chapter-admob-unique"
                        )
                    with col30:
                        create_card(
                            st,
                            card_title="Web Chpater Purchase with Koin Count",
                            card_value=data_text["web_chapter_coin"]["chapter_count"],
                            card_daily_growth=data_dg["web_chapter_data"]["chapter_coin_data"]["chapter_count"],
                            class_name="pembeli-chapter-koin-count"
                        )
                    with col31:
                        create_card(
                            st,
                            card_title="Web Chapter Purchase With AdsKoin Count",
                            card_value=data_text["web_chapter_adscoin"]["chapter_count"],
                            card_daily_growth=data_dg["web_chapter_data"]["chapter_adscoin_data"]["chapter_count"],
                            class_name="pembeli-chapter-adskoin-count"
                        )
                    with col32:
                        create_card(
                            st,
                            card_title="Web Chapter Purchase With Ads Count",
                            card_value=data_text["web_chapter_ads"]["chapter_count"],
                            card_daily_growth=data_dg["web_chapter_data"]["chapter_ads_data"]["chapter_count"],
                            class_name="pembeli-chapter-admob-count"
                        )

                    col33, col34 = st.columns(2)
                    with col33:
                        create_card(
                            st,
                            card_title="Web Total Pembeli Chapter Unique",
                            card_value=data_text["web_total_chapter_purchase"]["unique"],
                            card_daily_growth=data_dg["web_chapter_data"]["overall_chapter_purchase"]["unique"],
                            class_name="total-pembeli-chapter-unique"
                        )
                    with col34:
                        create_card(
                            st,
                            card_title="Web Total Pembeli Chapter Count",
                            card_value=data_text["web_total_chapter_purchase"]["count"],
                            card_daily_growth=data_dg["web_chapter_data"]["overall_chapter_purchase"]["count"],
                            class_name="total-pembeli-chapter-count"
                        )
                    
                    create_chart(st, data_chart["web_user_journey_chart"])

                    # -- Cost & revenue --
                    st.markdown(f"""<h1 align="center">Cost & Revenue</h1>""", unsafe_allow_html=True)
                    col35, col36, col37, col38 = st.columns(4)
                    with col35:
                        create_card(
                            st,
                            card_title="App Coin Purchase",
                            card_value=data_text["app_revenue"]["coin_count"],
                            card_daily_growth=data_dg["app_revenue_data"]["coin_count"],
                            class_name="app-coin-purchase"
                        )
                    with col36:
                        create_card(
                            st,
                            card_title="App Coin revenue",
                            card_value=data_text["app_revenue"]["gross_revenue"],
                            card_daily_growth=data_dg["app_revenue_data"]["gross_revenue"],
                            types="rp",
                            class_name="app-gross-revenue"
                        )
                    with col37:
                        create_card(
                            st,
                            card_title="App Average Revenue / Transaction",
                            card_value=data_text["app_revenue"]["arpt"],
                            card_daily_growth=data_dg["app_revenue_data"]["arpt"],
                            types="rp",
                            class_name="app-arpt"
                        )
                    with col38:
                        create_card(
                            st,
                            card_title="App Average Revenue / User",
                            card_value=data_text["app_revenue"]["arpu"],
                            card_daily_growth=data_dg["app_revenue_data"]["arpu"],
                            types="rp",
                            class_name="app-arpu"
                        )

                    col39, col40, col41, col42 = st.columns(4)
                    with col39:
                        create_card(
                            st,
                            card_title="Admob Impressions",
                            card_value=data_text["app_revenue"]["Impressions"],
                            card_daily_growth=data_dg["app_revenue_data"]["Impressions"],
                            class_name="admob-impressions"
                        )
                    with col40:
                        create_card(
                            st,
                            card_title="Admob Estimated Earnings",
                            card_value=data_text["app_revenue"]["Estimated earnings"],
                            card_daily_growth=data_dg["app_revenue_data"]["Estimated earnings"],
                            types="rp",
                            class_name="admob-estimated-earnings"
                        )
                    with col41:
                        create_card(
                            st,
                            card_title="Admob Estimated Earnings / User",
                            card_value=data_text["app_revenue"]["revenue_per_user"],
                            card_daily_growth=data_dg["app_revenue_data"]["revenue_per_user"],
                            types="rp",
                            class_name="admob-rev-user"
                        )
                    with col42:
                        create_card(
                            st,
                            card_title="Admob Estimated Earnings / Impressions",
                            card_value=data_text["app_revenue"]["Estimated_impressions"],
                            card_daily_growth=data_dg["app_revenue_data"]["Estimated_impressions"],
                            types="rp",
                            class_name="admob-rev-impressions"
                        )

                    col43, col44, col45, col46 = st.columns(4)
                    with col43:
                        create_card(
                            st,
                            card_title="Web Coin Purchase",
                            card_value=data_text["web_revenue"]["coin_count"],
                            card_daily_growth=data_dg["web_revenue_data"]["coin_count"],
                            class_name="web-coin-purchase"
                        )
                    with col44:
                        create_card(
                            st,
                            card_title="Web Coin revenue",
                            card_value=data_text["web_revenue"]["gross_revenue"],
                            card_daily_growth=data_dg["web_revenue_data"]["gross_revenue"],
                            types="rp",
                            class_name="web-gross-revenue"
                        )
                    with col45:
                        create_card(
                            st,
                            card_title="Web Average Revenue / Transaction",
                            card_value=data_text["web_revenue"]["arpt"],
                            card_daily_growth=data_dg["web_revenue_data"]["arpt"],
                            types="rp",
                            class_name="web-arpt"
                        )
                    with col46:
                        create_card(
                            st,
                            card_title="Web Average Revenue / User",
                            card_value=data_text["web_revenue"]["arpu"],
                            card_daily_growth=data_dg["web_revenue_data"]["arpu"],
                            types="rp",
                            class_name="web-arpu"
                        )

                    col47, col48, col49, col50 = st.columns(4)
                    with col47:
                        create_card(
                            st,
                            card_title="Adsense Impressions",
                            card_value=data_text["web_revenue"]["Impressions"],
                            card_daily_growth=data_dg["web_revenue_data"]["Impressions"],
                            class_name="adsense-impressions"
                        )
                    with col48:
                        create_card(
                            st,
                            card_title="Adsense Estimated Earnings",
                            card_value=data_text["web_revenue"]["Estimated earnings"],
                            card_daily_growth=data_dg["web_revenue_data"]["Estimated earnings"],
                            types="rp",
                            class_name="adsense-estimated-earnings"
                        )
                    with col49:
                        create_card(
                            st,
                            card_title="Adsense Estimated Earnings / User",
                            card_value=data_text["web_revenue"]["revenue_per_user"],
                            card_daily_growth=data_dg["web_revenue_data"]["revenue_per_user"],
                            types="rp",
                            class_name="adsense-rev-user"
                        )
                    with col50:
                        create_card(
                            st,
                            card_title="Adsense Estimated Earnings / Impressions",
                            card_value=data_text["web_revenue"]["Estimated_impressions"],
                            card_daily_growth=data_dg["web_revenue_data"]["Estimated_impressions"],
                            types="rp",
                            class_name="adsense-rev-impressions"
                        )

                    col51, col52 = st.columns(2)
                    with col51:
                        create_card(
                            st,
                            card_title="Total Cost",
                            card_value=data_text["cost"],
                            card_daily_growth=data_dg["cost"],
                            types="rp",
                            class_name="overall-cost"
                        )
                    with col52:
                        create_card(
                            st,
                            card_title="Total Revenue",
                            card_value=data_text["overall_revenue"]["overall_revenue"],
                            card_daily_growth=data_dg["overall_revenue"]["overall_revenue"],
                            types="rp",
                            class_name="all-source-rev"
                        )

                    create_chart(st, data_chart["revenue_cost_periods"])

                    create_chart(st, data_chart["revenue_cost_charts"])
            
            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")
                if data_dg.get("message"):
                    st.error(f"Error while feting daily growth data: {data_dg.get("message", None)}, KeyError: {ke}")
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 
