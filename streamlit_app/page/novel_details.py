import streamlit as st
import requests
import datetime
import asyncio
import httpx
from functools import partial
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_novel_details_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">Novel Details</h1>""", unsafe_allow_html=True)

    with st.container(border=True):
        # Calculate preset date ranges
        today = datetime.date.today()
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
            None : (datetime.datetime.strptime("2022-10-25", "%Y-%m-%d").date(), today),
            "Custom Range" : "custom_range",
            "All Time": (datetime.datetime.strptime("2022-10-25", "%Y-%m-%d").date(), today),
            "This Year": (this_year_start, today),
            "Last Year": (last_year_start, last_year_end),
            "This Month" : (this_month_start, today),
            "Last Month" : (last_month_start, last_month_end),
            "This Week" : (this_week_start, today),
            "Last Week" : (last_week_start, last_week_end) ,
            "Last 7 Days": (last_7days_start, last_7days_end)
        }
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"period_novel_details")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key="novel_details_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        search = st.text_input("Novel Title", placeholder="Novel Title (Required)", key="novel_title")
        submit_button = st.button(label="Apply Filters", disabled=False, key="submit_button_novel_details")
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            
            params = {
                "from_date": from_date,
                "to_date": to_date,
                "novel_title": search
            }
                
            # Use partial application for cleaner task creation
            fetch_data_partial = partial(fetch_data, st, host=host)
            tasks = [
                fetch_data_partial(uri=f'novel/novel-details', params=params),
                fetch_data_partial(uri=f'novel/novel-details/chart', params=params)
            ]
            try:
                data, data_chart = await asyncio.gather(*tasks)
            except httpx.RequestError as e:  # Handle potential exceptions
                st.error(f"Error fetching data: {e}")
                
            card_style(st)
            try:
                if data:
                    st.markdown(f"""<h1 align="center">Novel Information : {search.capitalize()}</h1>""", unsafe_allow_html=True)
                    
                    # -- novel information --
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        create_card(
                            st, 
                            style="plain",
                            card_title="Novel Id",
                            card_value=data["id_novel"]
                        )
                    with col2:
                        create_card(
                            st,
                            style="plain",
                            card_title="Novel Title",
                            card_value=data["judul_novel"],
                            types="plain"
                        )
                    with col3:
                        create_card(
                            st,
                            style="plain",
                            card_title="Novel Genre",
                            card_value=data["category"],
                            types="plain"
                        )
                    with col4:
                        create_card(
                            st,
                            style="plain",
                            card_title="User Favorite",
                            card_value=data["total_favorite"]
                        )
                    
                    col5, col6, col7 = st.columns(3)
                    with col5:
                        create_card(
                            st,
                            style="plain",
                            card_title="Published Date",
                            card_value=data["tanggal_terbit"],
                            types="plain"
                        )
                    with col6:
                        create_card(
                            st,
                            style="plain",
                            card_title="Novel Status",
                            card_value=data["status"],
                            types="plain"
                        )
                    with col7:
                        create_card(
                            st,
                            style="plain",
                            card_title="Last Update",
                            card_value=data["last_updated"],
                            types="plain"
                        )
                    
                    col8, col9, col10 = st.columns(3)
                    with col8:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter",
                            card_value=data["total_bab"]
                        )
                    with col9:
                        create_card(
                            st,
                            style="plain",
                            card_title="Unpublished Chapter",
                            card_value=data["belum_terbit"]
                        )
                    with col10:
                        create_card(
                            st,
                            style="plain",
                            card_title="Published Chapter",
                            card_value=data["bab_terbit"]
                        )
                    
                    # -- novel writer information --
                    st.markdown(f"""<h1 align="center">Novel Writer Information : {search.capitalize()}</h1>""", unsafe_allow_html=True)

                    col11, col12, col13 = st.columns(3)
                    with col11:
                        create_card(
                            st,
                            style="plain",
                            card_title="Pen name",
                            card_value=data["nama_pena"],
                            types="plain"
                        )
                    with col12:
                        create_card(
                            st,
                            style="plain",
                            card_title="Novel Writer Name",
                            card_value=data["nama_penulis"],
                            types="plain"
                        )
                    with col13:
                        create_card(
                            st,
                            style="plain",
                            card_title="Gender",
                            card_value=data["gender"],
                            types="plain"
                        )
                    
                    col14, col15 = st.columns(2)
                    with col14:
                        create_card(
                            st, 
                            style="plain",
                            card_title="Email",
                            card_value=data["email"],
                            types="plain"
                        )
                    with col15:
                        create_card(
                            st,
                            style="plain",
                            card_title="No Tlp / WA",
                            card_value=data["no_tlp"],
                            types="plain"
                        )

                    create_card(
                        st,
                        style="plain", 
                        card_title="Writer Address",
                        card_value=data["alamat"],
                            types="plain"
                    )
                    
                    #  -- novel reader & purchase information -- 
                    st.markdown(f"""<h1 align="center">Novel Performance : {search.capitalize()}</h1>""", unsafe_allow_html=True)
                    col17, col18, col19, col20 = st.columns(4)
                    with col17:
                        create_card(
                            st,
                            style="plain",
                            card_title="Register Chapter Reader (Unique)",
                            card_value=data["regis_pembaca_unique"]
                        )
                    with col18:
                        create_card(
                            st, 
                            style="plain",
                            card_title="Guest Chapter reader (unique)",
                            card_value=data["guest_pembaca_unique"]
                        )
                    with col19:
                        create_card(
                            st,
                            style="plain",
                            card_title="Register Chapter Reader (Count)",
                            card_value=data["regis_pembaca_count"]
                        )
                    with col20:
                        create_card(
                            st,
                            style="plain",
                            card_title="Guest Chapter Reader (Count)",
                            card_value=data["guest_pembaca_count"]
                        )
                    
                    col21, col22 = st.columns(2)
                    with col21:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter reader (Unique)",
                            card_value=data["total_pembaca_unique"]
                        )
                    
                    with col22:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Reader (Count)",
                            card_value=data["total_pembaca_count"]
                        )

                    col23, col24, col25, col26, col27, col28 = st.columns(6)
                    with col23:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Coin (Unique)",
                            card_value=data["chapter_coin_unique"]
                        )
                    
                    with col24:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With AdsCoin (Unique)",
                            card_value=data["chapter_adscoin_unique"]
                        )
                    
                    with col25:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Ads (Unique)",
                            card_value=data["chapter_ads_unique"]
                        )

                    with col26:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Coin (Count)",
                            card_value=data["chapter_coin_count"]
                        )
                    
                    with col27:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With AdsCoin (Count)",
                            card_value=data["chapter_adscoin_count"]
                        )
                    
                    with col28:
                        create_card(
                            st,
                            style="plain",
                            card_title="Chapter Purchase With Ads (Count)",
                            card_value=data["chapter_ads_count"]
                        )
                    
                    col29, col30 = st.columns(2)
                    with col29:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Purchase (Unique)",
                            card_value=data["total_chapter_unique"]
                        )

                    with col30:
                        create_card(
                            st,
                            style="plain",
                            card_title="Total Chapter Purchase (count)",
                            card_value=data["total_chapter_count"]
                        )

                    col31, col32 = st.columns(2)
                    with col31:
                        create_chart(st, data_chart["frequency_table"])
                    
                    with col32:
                        create_chart(st, data_chart["frequency_chart"])

                    create_chart(st, data_chart["user_table_pembaca"])

                    create_chart(st, data_chart["user_table_chapter_coin"])

                    create_chart(st, data_chart["user_table_chapter_adscoin"])

                    create_chart(st, data_chart["user_table_chapter_ads"])

            except KeyError as ke:
                if data.get("message"):
                    st.error(f"Error while feting metrics data: {data.get("message", None)}, KeyError: {ke}")
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 