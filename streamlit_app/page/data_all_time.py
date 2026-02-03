import streamlit as st
import requests
import datetime
import asyncio
import httpx
from functools import partial
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio


async def show_data_all_time_page(host):
    """
    This function creates a Streamlit page to display chapter reading metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">Data All TIme</h1>""", unsafe_allow_html=True)

    with st.form("data_all_time"):
        year = {
            None : "2024",
            "2022": "2022",
            "2023": "2023",
            "2024": "2024"
        }
        year_options = st.selectbox("Year", list(year.keys()), placeholder="Year", index=None, key="all_time_date")
        submit_button = st.form_submit_button(label="Apply Filters", disabled=False)
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            params = {
                "year": year[year_options],
            }
                
            # Use partial application for cleaner task creation
            fetch_data_partial = partial(fetch_data, st, host=host)
            tasks = [
                fetch_data_partial(uri=f'data-all-time', params=params),
                fetch_data_partial(uri=f'data-all-time/chart', params=params)
            ]
            try:
                data_text, data_chart = await asyncio.gather(*tasks)
            except httpx.RequestError as e:  # Handle potential exceptions
                st.error(f"Error fetching data: {e}")
                
            card_style(st)
            try:
                if data_text and data_chart:  # Ensure data exists
                    # -- chapter read section --
                    st.markdown("""<h2 align="center">App Novel Reader & Purchase All Time</h2>""", unsafe_allow_html=True)

                    # -- app novel reader & purchase all time section -- 
                    col1, col2 = st.columns(2)
                    with col1:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Chapter Reader (Unique)",
                            card_value=data_text["app_chapter_read"]["pembaca_chapter_unique"]
                        )
                    with col2:
                        create_card(
                            st, 
                            style="plain",
                            card_title="App Chapter Reader (Count)",
                            card_value=data_text["app_chapter_read"]["pembaca_chapter_count"]
                        )
                    
                    col3, col4, col5, col6, col7, col8  = st.columns(6)
                    with col3:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Chapter Purchase With Coin (Unique)",
                            card_value=data_text["app_chapter_coin"]["chapter_unique"]
                        )
                    with col4:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Chapter Purchase With AdsCoin (Unique)",
                            card_value=data_text["app_chapter_adscoin"]["chapter_unique"]
                        )
                    with col5:
                        create_card(
                            st, 
                            style="plain",
                            card_title="App Chapter Purchase With Admob (Unique)",
                            card_value=data_text["app_chapter_ads"]["chapter_unique"]
                        )
                    with col6:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Chapter Purchase With Coin (Count)",
                            card_value=data_text["app_chapter_coin"]["chapter_count"]
                        )
                    with col7:
                        create_card(
                            st, 
                            style="plain",
                            card_title="App Chapter Purchase With AdsCoin (Count)",
                            card_value=data_text["app_chapter_adscoin"]["chapter_count"]
                        )
                    with col8:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Chapter Purchase With Admob (Count)",
                            card_value=data_text["app_chapter_ads"]["chapter_count"]
                        )
                    
                    col9, col10 = st.columns(2)
                    with col9:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Total Chapter Purchase (Unique)",
                            card_value=data_text["app_total_chapter_purchase"]["unique"]
                        )
                    with col10:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Total Chapter Purchase (Count)",
                            card_value=data_text["app_total_chapter_purchase"]["count"]
                        )

                    col11, col12 = st.columns(2)
                    with col11:
                        create_chart(st, data_chart["pembaca_pembeli_chapter_unique_chart_all"])
                    with col12:
                        create_chart(st, data_chart["pembaca_pembeli_chapter_count_chart_all"])
                    
                    create_chart(st, data_chart["pembaca_chapter_novel_table"])

                    # -- chapter read section --
                    st.markdown("""<h2 align="center">Web Novel Reader & Purchase All Time</h2>""", unsafe_allow_html=True)

                    # -- app novel reader & purchase all time section -- 
                    col13, col14 = st.columns(2)
                    with col13:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Chapter Reader (Unique)",
                            card_value=data_text["web_chapter_read"]["pembaca_chapter_unique"]
                        )
                    with col14:
                        create_card(
                            st, 
                            style="plain",
                            card_title="Web Chapter Reader (Count)",
                            card_value=data_text["web_chapter_read"]["pembaca_chapter_count"]
                        )
                    
                    col15, col16, col17, col18, col19, col20  = st.columns(6)
                    with col15:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Chapter Purchase With Coin (Unique)",
                            card_value=data_text["web_chapter_coin"]["chapter_unique"]
                        )
                    with col16:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Chapter Purchase With AdsCoin (Unique)",
                            card_value=data_text["web_chapter_adscoin"]["chapter_unique"]
                        )
                    with col17:
                        create_card(
                            st, 
                            style="plain",
                            card_title="Web Chapter Purchase With Admob (Unique)",
                            card_value=data_text["web_chapter_ads"]["chapter_unique"]
                        )
                    with col18:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Chapter Purchase With Coin (Count)",
                            card_value=data_text["web_chapter_coin"]["chapter_count"]
                        )
                    with col19:
                        create_card(
                            st, 
                            style="plain",
                            card_title="Web Chapter Purchase With AdsCoin (Count)",
                            card_value=data_text["web_chapter_adscoin"]["chapter_count"]
                        )
                    with col20:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Chapter Purchase With Admob (Count)",
                            card_value=data_text["web_chapter_ads"]["chapter_count"]
                        )
                    
                    col21, col22 = st.columns(2)
                    with col21:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Total Chapter Purchase (Unique)",
                            card_value=data_text["web_total_chapter_purchase"]["unique"]
                        )
                    with col22:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Total Chapter Purchase (Count)",
                            card_value=data_text["web_total_chapter_purchase"]["count"]
                        )

                    col23, col24 = st.columns(2)
                    with col23:
                        create_chart(st, data_chart["web_pembaca_pembeli_chapter_unique_chart_all"])
                    with col24:
                        create_chart(st, data_chart["web_pembaca_pembeli_chapter_count_chart_all"])
                        
                    create_chart(st, data_chart["web_pembaca_chapter_novel_table"])

                    # -- total revenue all time section --
                    st.markdown("""<h2 align="center">Total Revenue All Time</h2>""", unsafe_allow_html=True)
                    col25, col26, col27, col28, col29 = st.columns(5)
                    with col25:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Revenue Adsense",
                            card_value=data_text["web_pembelian_coin"]["Estimated earnings"], 
                            types="rp"
                        )
                    with col26:
                        create_card(
                            st, 
                            style="plain",
                            card_title="Total Revenue Admob",
                            card_value=data_text["app_pembelian_coin"]["Estimated earnings"],
                            types="rp"
                        )
                    with col27:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total App Revenue Coin",
                            card_value=data_text["app_pembelian_coin"]["gross_revenue"],
                            types="rp"
                        )
                    with col28:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Web Revenue Coin",
                            card_value=data_text["web_pembelian_coin"]["gross_revenue"],
                            types="rp"
                        )
                    with col29:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Revenue",
                            card_value=data_text["all_revenue_data"],
                            types="rp"
                        )
                    
                    create_chart(st, data_chart["all_revenue_chart"])

                    # -- App coin purchase revenue all time --
                    st.markdown("""<h2 align="center">App Coin Purchase Revenue All Time</h2>""", unsafe_allow_html=True)
                    col30, col31, col32, col33, col34 = st.columns(5)
                    with col30:
                        create_card(
                            st, 
                            style="plain",
                            card_title="App Expired Coin Transaction",
                            card_value=data_text["app_pembelian_coin"]["count_coin_expired"]
                        )
                    with col31:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Success Coin Transaction",
                            card_value=data_text["app_pembelian_coin"]["count_coin_success"]
                        )
                    with col32:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Total Coin Transaction",
                            card_value=data_text["app_pembelian_coin"]["count_coin_total"]
                        )
                    with col33:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Total Revenue",
                            card_value=data_text["app_pembelian_coin"]["revenue"], 
                            types="rp"
                        )
                    with col34:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Total Gross Revenue",
                            card_value=data_text["app_pembelian_coin"]["gross_revenue"],
                            types="rp"
                        )
                    
                    col35, col36 = st.columns(2)
                    with col35:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Frist Coin Purchase",
                            card_value=data_text["app_return_first_coin"]["first_purchase"]
                        )
                    with col36:
                        create_card(
                            st,
                            style="plain",
                            card_title="App Returning Coin Purchase",
                            card_value=data_text["app_return_first_coin"]["returning_purchase"]
                        )
                    
                    create_chart(st, data_chart["first_ret_purchase"])

                    col37, col38 = st.columns(2)
                    with col37:
                        create_chart(st, data_chart["coin_month"])
                    with col38:
                        create_chart(st, data_chart["persentase_koin_all_time"])
                    
                    create_chart(st, data_chart["rev_month"])

                    # -- Web coin purchase revenue all time --
                    st.markdown("""<h2 align="center">Web Coin Purchase Revenue All Time</h2>""", unsafe_allow_html=True)
                    col30, col31, col32, col33, col34 = st.columns(5)
                    with col30:
                        create_card(
                            st, 
                            style="plain",
                            card_title="Web Expired Coin Transaction",
                            card_value=data_text["web_pembelian_coin"]["count_coin_expired"]
                        )
                    with col31:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Success Coin Transaction",
                            card_value=data_text["web_pembelian_coin"]["count_coin_success"]
                        )
                    with col32:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Total Coin Transaction",
                            card_value=data_text["web_pembelian_coin"]["count_coin_total"]
                        )
                    with col33:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Total Revenue",
                            card_value=data_text["web_pembelian_coin"]["revenue"],
                            types="rp"
                        )
                    with col34:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Total Gross Revenue",
                            card_value=data_text["web_pembelian_coin"]["gross_revenue"],
                            types="rp"
                        )
                    
                    col35, col36 = st.columns(2)
                    with col35:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Frist Coin Purchase",
                            card_value=data_text["web_return_first_coin"]["first_purchase"]
                        )
                    with col36:
                        create_card(
                            st,
                            style="plain",
                            card_title="Web Returning Coin Purchase",
                            card_value=data_text["web_return_first_coin"]["returning_purchase"]
                        )
                    
                    create_chart(st, data_chart["web_first_ret_purchase"])

                    col37, col38 = st.columns(2)
                    with col37:
                        create_chart(st, data_chart["web_coin_month"])
                    with col38:
                        create_chart(st, data_chart["web_persentase_koin_all_time"])
                    
                    create_chart(st, data_chart["web_rev_month"])

                    # -- admob revenue all time
                    st.markdown("""<h2 align="center">Admob Revenue All Time</h2>""", unsafe_allow_html=True)
                    col39, col40 = st.columns(2)
                    with col39:
                        create_card(
                            st,
                            style="plain",
                            card_title="Admob Unique User",
                            card_value=data_text["app_chapter_ads"]["chapter_unique"]
                        )
                    with col40:
                        create_card(
                            st,
                            style="plain",
                            card_title="Admob revenue",
                            card_value=data_text["app_pembelian_coin"]["Estimated earnings"],
                            types="rp"
                        )
                    
                    col41, col42 = st.columns(2)
                    with col41:
                        create_chart(st, data_chart["users_unique_count_overall_chart"])
                    with col42:
                        create_chart(st, data_chart["revenue_chart"])

                    # -- adsense revenue all time -- 
                    st.markdown("""<h2 align="center">Adsense Revenue All Time</h2>""", unsafe_allow_html=True)
                    col39, col40 = st.columns(2)
                    with col39:
                        create_card(
                            st,
                            style="plain",
                            card_title="Adsense Unique User",
                            card_value=data_text["web_chapter_ads"]["chapter_unique"]
                        )
                    with col40:
                        create_card(
                            st,
                            style="plain",
                            card_title="Adsense revenue",
                            card_value=data_text["web_pembelian_coin"]["Estimated earnings"],
                            types="rp"
                        )
                    
                    col41, col42 = st.columns(2)
                    with col41:
                        create_chart(st, data_chart["web_users_unique_count_overall_chart"])
                    with col42:
                        create_chart(st, data_chart["adsense_revenue_all"])

            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 
