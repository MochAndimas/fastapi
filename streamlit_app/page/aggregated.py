import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio


async def show_aggregated_page(host):
    """
    This function creates a Streamlit page to display chapter reading metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">Aggregated</h1>""", unsafe_allow_html=True)

    with st.container(border=True):
        # Calculate preset date ranges
        today = datetime.date.today()
        all_time_start = datetime.datetime.strptime("2022-10-25", "%Y-%m-%d").date()
        this_year_start = today.replace(month=1, day=1)
        last_year_start = this_year_start.replace(year=this_year_start.year - 1)
        last_year_end = this_year_start - datetime.timedelta(days=1)
        this_month_start = today.replace(day=1)
        last_month_start = (this_month_start - datetime.timedelta(days=1)).replace(day=1)
        last_month_end = this_month_start - datetime.timedelta(days=1)
        this_week_start = today - datetime.timedelta(days=today.weekday())
        last_week_start = this_week_start - datetime.timedelta(days=7)  # Monday of the previous week
        last_week_end = this_week_start - datetime.timedelta(days=1)    # Last Sunday
        last_7days_start = today - datetime.timedelta(days=7)
        last_7days_end  = today - datetime.timedelta(days=1)
        preset_date = {
            None : (all_time_start, last_7days_end),
            "Custom Range" : "custom_range",
            "All Time": (all_time_start, last_7days_end),
            "This Year": (this_year_start, today),
            "Last Year": (last_year_start, last_year_end),
            "This Month" : (this_month_start, today),
            "Last Month" : (last_month_start, last_month_end),
            "This Week" : (this_week_start, today),
            "Last Week" : (last_week_start, last_week_end) ,
            "Last 7 Days": (last_7days_start, last_7days_end)
        }
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"period_aggregated")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key="aggregated_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        submit_button = st.button(label="Apply Filters", disabled=False, key="submit_button_aggregated")
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                params = {
                    "from_date": from_date,
                    "to_date": to_date,
                }
                
                data_text = await fetch_data(st, host=host, uri=f'aggregated', params=params)

                # Data Presentation
                card_style(st)
                if data_text:  # Ensure data exists
                    # -- chapter read section --
                    with st.container(border=True):
                        st.markdown("""<h2 align="center">Aggregated Data All Time</h2>""", unsafe_allow_html=True)

                    # -- aggregated data all time section -- 
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Download",
                            card_value=data_text["total_install"]
                        )
                    with col2:
                        create_card(
                            st,
                            style="plain",
                            card_title="Android Download",
                            card_value=data_text["install_android"]
                        )
                    with col3:
                        create_card(
                            st,
                            style="plain",
                            card_title="IOS Download",
                            card_value=data_text["install_ios"]
                        )
                    
                    col4, col5, col6 = st.columns(3)
                    with col4:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Register",
                            card_value=data_text["overall_register"]
                        )
                    with col5:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Reader",
                            card_value=data_text["overall_pembaca"]
                        )
                    with col6:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Purchase",
                            card_value=data_text["overall_pembeli"]
                        )

                    # -- Aggregated data app activity section --
                    with st.container(border=True):
                        st.markdown("""<h2 align="center">App User Activity</h2>""", unsafe_allow_html=True)
                        st.caption("*Data Register start from 06-jan-2024")

                    col7, col8, col9, col10 = st.columns(4)
                    with col7:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Register",
                            card_value=data_text["register_week"]
                        )
                    with col8:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Reader (Unique)",
                            card_value=data_text["app_chapter_read"]["pembaca_chapter_unique"]
                        )
                    with col9:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Reader Guest (Unique)",
                            card_value=data_text["app_chapter_read"]["unique_guest_reader"]
                        )
                    with col10:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Reader Register (Unique)",
                            card_value=data_text["app_chapter_read"]["unique_register_reader"]
                        )

                    col11, col12, col13 = st.columns(3)
                    with col11:
                        create_card(
                            st,
                            style="plain",
                            card_title="Coin Purchase",
                            card_value=data_text["app_pembelian_coin"]["coin_unique"]
                        )
                    with col12:
                        create_card(
                            st,
                            style="plain",
                            card_title="Frist Coin Purchase",
                            card_value=data_text["app_pembelian_coin"]["first_purchase"]
                        )
                    with col13:
                        create_card(
                            st,
                            style="plain",
                            card_title="Returning Coin Purchase",
                            card_value=data_text["app_pembelian_coin"]["return_purchase"]
                        )
                    
                    col14, col15, col16, col17, col18, col19 = st.columns(6)
                    with col14:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Coin (Unique)",
                            card_value=data_text["app_chapter_coin"]["chapter_unique"]
                        )
                    with col15:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With AdsCoin (Unique)",
                            card_value=data_text["app_chapter_adscoin"]["chapter_unique"]
                        )
                    with col16:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Admob (Unique)",
                            card_value=data_text["app_chapter_ads"]["chapter_unique"]
                        )
                    with col17:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Coin (Count)",
                            card_value=data_text["app_chapter_coin"]["chapter_count"]
                        )
                    with col18:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With AdsCoin (Count)",
                            card_value=data_text["app_chapter_adscoin"]["chapter_count"]
                        )
                    with col19:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Admob (Count)",
                            card_value=data_text["app_chapter_ads"]["chapter_count"]
                        )

                    col20, col21 = st.columns(2)
                    with col20:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Purchase (Unique)",
                            card_value=data_text["app_total_chapter_purchase"]["unique"]
                        )
                    with col21:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Purchase (Count)",
                            card_value=data_text["app_total_chapter_purchase"]["count"]
                        )
                    
                    # -- Aggregated data app activity section --
                    with st.container(border=True):
                        st.markdown("""<h2 align="center">Web User Activity</h2>""", unsafe_allow_html=True)
                        st.caption("*Data Register start from 06-jan-2024")

                    col7, col8, col9, col10 = st.columns(4)
                    with col7:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Register",
                            card_value=data_text["web_register_week"]
                        )
                    with col8:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Reader (Unique)",
                            card_value=data_text["web_chapter_read"]["pembaca_chapter_unique"]
                        )
                    with col9:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Reader Guest (Unique)",
                            card_value=data_text["web_chapter_read"]["unique_guest_reader"]
                        )
                    with col10:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Reader Register (Unique)",
                            card_value=data_text["web_chapter_read"]["unique_register_reader"]
                        )

                    col11, col12, col13 = st.columns(3)
                    with col11:
                        create_card(
                            st,
                            style="plain",
                            card_title="Coin Purchase",
                            card_value=data_text["web_pembelian_coin"]["coin_unique"]
                        )
                    with col12:
                        create_card(
                            st,
                            style="plain",
                            card_title="Frist Coin Purchase",
                            card_value=data_text["web_pembelian_coin"]["first_purchase"]
                        )
                    with col13:
                        create_card(
                            st,
                            style="plain",
                            card_title="Returning Coin Purchase",
                            card_value=data_text["web_pembelian_coin"]["return_purchase"]
                        )
                    
                    col14, col15, col16, col17, col18, col19 = st.columns(6)
                    with col14:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Coin (Unique)",
                            card_value=data_text["web_chapter_coin"]["chapter_unique"]
                        )
                    with col15:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With AdsCoin (Unique)",
                            card_value=data_text["web_chapter_adscoin"]["chapter_unique"]
                        )
                    with col16:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Admob (Unique)",
                            card_value=data_text["web_chapter_ads"]["chapter_unique"]
                        )
                    with col17:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Coin (Count)",
                            card_value=data_text["web_chapter_coin"]["chapter_count"]
                        )
                    with col18:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With AdsCoin (Count)",
                            card_value=data_text["web_chapter_adscoin"]["chapter_count"]
                        )
                    with col19:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Admob (Count)",
                            card_value=data_text["web_chapter_ads"]["chapter_count"]
                        )

                    col20, col21 = st.columns(2)
                    with col20:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Purchase (Unique)",
                            card_value=data_text["web_total_chapter_purchase"]["unique"]
                        )
                    with col21:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Purchase (Count)",
                            card_value=data_text["web_total_chapter_purchase"]["count"]
                        )

            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")
            
            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 
