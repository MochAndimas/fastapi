import streamlit as st
import datetime
import asyncio
import httpx
from functools import partial
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio


async def show_chapter_page(host, source):
    """
    This function creates a Streamlit page to display chapter reading metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">{source.capitalize()} Novel Reader & Purchase All By Periods</h1>""", unsafe_allow_html=True)

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
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"{source}_chapter_periods")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key=f"{source}_chapter_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        columns = {
            None : "total_pembeli_chapter_count",
            "Pembaca Chapter unique" : "pembaca_chapter_unique",
            "Pembaca Chapter Count": "pembaca_chapter_count",
            "Pembeli Chapter Unique With Koin": "pembeli_chapter_koin_unique",
            "Pembeli Chapter Count With Koin": "pembeli_chapter_koin_count",
            "Pembeli Chapter Unique With AdsKoin": "pembeli_chapter_adskoin_unique",
            "Pembeli Chapter Count With AdsKoin": "pembeli_chapter_adskoin_count",
            "Pembeli Chapter Unique With Admob": "pembeli_chapter_admob_unique",
            "Pembeli Chapter Count With Admob": "pembeli_chapter_admob_count",
            "Total Pembeli Chapter Unique": "total_pembeli_chapter_unique",
            "Total Pembeli Chapter Count": "total_pembeli_chapter_count"
        }
        order_val = {
            None : False,
            'Ascending': True, 
            'Descending': False
        }
        sort_by = st.selectbox("Sort By", list(columns.keys()), placeholder="Sort By  (Optional)", index=None, key=f"{source}_sort_by_chapter")
        order = st.selectbox("Order By", list(order_val.keys()), placeholder="Order By (Optional)", index=None, key=f"{source}_order_by_chapter")
        submit_button = st.button(label="Apply Filters", disabled=False, key=f"{source}_submit_button_chapter")
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            params = {
                "source": source,
                "from_date": from_date,
                "to_date": to_date,
                "sort_by": columns[sort_by],
                "ascendings": order_val[order]
            }
                
            # Use partial application for cleaner task creation
            fetch_data_partial = partial(fetch_data, st, host=host)
            tasks = [
                fetch_data_partial(uri=f'chapter', params=params),
                fetch_data_partial(uri=f'chapter/daily-growth', params=params),
                fetch_data_partial(uri=f'chapter/chart', params=params)
            ]
            try:
                data_text, data_persentase, data_chart = await asyncio.gather(*tasks)
            except httpx.RequestError as e:  # Handle potential exceptions
                st.error(f"Error fetching data: {e}")

            # Data Presentation
            card_style(st)
            try:
                if data_text and data_persentase and data_chart:  # Ensure data exists
                    # -- chapter read section --
                    st.markdown("""<h2 align="center">All User Chapter Reader & Purchase</h2>""", unsafe_allow_html=True)

                    col27, col28, col29, col30 = st.columns(4)
                    with col27:
                        create_card(
                            st,
                            card_title="Unique Register Reader",
                            card_value=data_text['chapter_read_data']['unique_register_reader'],
                            card_daily_growth=data_persentase['chapter_read_data']['unique_register_reader'],
                            class_name="unique-register-reader"
                        )
                    
                    with col28:
                        create_card(
                            st,
                            card_title="Unique Guest Reader",
                            card_value=data_text['chapter_read_data']['unique_guest_reader'],
                            card_daily_growth=data_persentase['chapter_read_data']['unique_guest_reader'],
                            class_name="unique-guest-reader"
                        )

                    with col29:
                        create_card(
                            st,
                            card_title="Count Register Reader",
                            card_value=data_text['chapter_read_data']['count_register_reader'],
                            card_daily_growth=data_persentase['chapter_read_data']['count_register_reader'],
                            class_name="count-register-reader"
                        )
                    
                    with col30:
                        create_card(
                            st,
                            card_title="Count Guest Reader",
                            card_value=data_text['chapter_read_data']['count_guest_reader'],
                            card_daily_growth=data_persentase['chapter_read_data']['count_guest_reader'],
                            class_name="count-guest-reader"
                        )

                    col1, col2 = st.columns(2)
                    with col1:
                        create_card(
                            st, 
                            card_title="Chapter Reader Unique",
                            card_value=data_text['chapter_read_data']["pembaca_chapter_unique"],
                            card_daily_growth=data_persentase['chapter_read_data']["pembaca_chapter_unique"],
                            class_name="chapter-unique"
                        )

                    with col2:
                        create_card(
                            st,
                            card_title="Chapter Reader Count",
                            card_value=data_text['chapter_read_data']["pembaca_chapter_count"],
                            card_daily_growth=data_persentase['chapter_read_data']["pembaca_chapter_count"],
                            class_name="chapter-count"
                        )
                    
                    # -- chapter read & purchase chart section --
                    col3, col4 = st.columns(2)
                    with col3:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["pembaca_pembeli_chapter_unique_chart"]))
                    with col4:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["pembaca_pembeli_chapter_count_chart"]))

                    # -- chapter purchase section --
                    col5, col6, col7, col8, col9, col10 = st.columns(6)
                    with col5:
                        create_card(
                            st,
                            card_title="Chapter Purchase With Koin Unique",
                            card_value=data_text["chapter_coin_data"]["chapter_unique"],
                            card_daily_growth=data_persentase["chapter_coin_data"]["chapter_unique"],
                            class_name="pembeli-chapter-koin-unique"
                        )
                    with col6:
                        create_card(
                            st,
                            card_title="Chapter Purchase With AdsKoin Unique",
                            card_value=data_text["chapter_adscoin_data"]["chapter_unique"],
                            card_daily_growth=data_persentase["chapter_adscoin_data"]["chapter_unique"],
                            class_name="pembeli-chapter-adskoin-unique"
                        )
                    with col7:
                        create_card(
                            st,
                            card_title="Chapter Purchase With Ads Unique",
                            card_value=data_text["chapter_ads_data"]["chapter_unique"],
                            card_daily_growth=data_persentase["chapter_ads_data"]["chapter_unique"],
                            class_name="pembeli-chapter-admob-unique"
                        )
                    with col8:
                        create_card(
                            st,
                            card_title="Chpater Purchase with Koin Count",
                            card_value=data_text["chapter_coin_data"]["chapter_count"],
                            card_daily_growth=data_persentase["chapter_coin_data"]["chapter_count"],
                            class_name="pembeli-chapter-koin-count"
                        )
                    with col9:
                        create_card(
                            st,
                            card_title="Chapter Purchase With AdsKoin Count",
                            card_value=data_text["chapter_adscoin_data"]["chapter_count"],
                            card_daily_growth=data_persentase["chapter_adscoin_data"]["chapter_count"],
                            class_name="pembeli-chapter-adskoin-count"
                        )
                    with col10:
                        create_card(
                            st,
                            card_title="Chapter Purchase With Ads Count",
                            card_value=data_text["chapter_ads_data"]["chapter_count"],
                            card_daily_growth=data_persentase["chapter_ads_data"]["chapter_count"],
                            class_name="pembeli-chapter-admob-count"
                        )

                    # -- total chapter purchase section --
                    col11, col12 = st.columns(2)
                    with col11:
                        create_card(
                            st,
                            card_title="Total Pembeli Chapter Unique",
                            card_value=data_text["overall_chapter_purchase"]["unique"],
                            card_daily_growth=data_persentase["overall_chapter_purchase"]["unique"],
                            class_name="total-pembeli-chapter-unique"
                        )
                    with col12:
                        create_card(
                            st,
                            card_title="Total Pembeli Chapter Count",
                            card_value=data_text["overall_chapter_purchase"]["count"],
                            card_daily_growth=data_persentase["overall_chapter_purchase"]["count"],
                            class_name="total-pembeli-chapter-count"
                        )

                    # -- chapter read & purchase by day chart section --
                    col13, col14 = st.columns(2)
                    with col13:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["pembaca_chapter_day"]))
                    with col14:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["pembeli_chapter_day"]))

                    # -- chapter read & purchase by genre chart section --
                    col15, col16 = st.columns(2)
                    with col15:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["pembaca_chapter_genre"]))
                    with col16:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["pembeli_chapter_genre"]))

                    # -- chapter read & purchase table section --
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["pembaca_chapter_novel_table"]))

                    # -- Old & new user chapter reader section --
                    st.markdown("""<h2 align="center">Old & New User Chapter Reader & Purchase</h2>""", unsafe_allow_html=True)


                    col17, col18, col31, col32 = st.columns(4)
                    with col17:
                        create_card(
                            st,
                            card_title="New Register User Chapter Reader",
                            card_value=data_text["chapter_read_data"]["new_user_count"],
                            card_daily_growth=data_persentase["chapter_read_data"]["new_user_count"],
                            class_name="pembaca-chapter-new-user"
                        )
                    with col18:
                        create_card(
                            st,
                            card_title="Old Register User Chapter Reader",
                            card_value=data_text["chapter_read_data"]["old_user_count"],
                            card_daily_growth=data_persentase["chapter_read_data"]["old_user_count"],
                            class_name="pembaca-chapter-old-user"
                        )
                    
                    with col31:
                        create_card(
                            st,
                            card_title="New User Guest Chapter Reader",
                            card_value=data_text["chapter_read_data"]["guest_new_user_count"],
                            card_daily_growth=data_persentase["chapter_read_data"]["guest_new_user_count"],
                            class_name="pembaca-guest-chapter-new-user"
                        )
                    with col32:
                        create_card(
                            st,
                            card_title="Old User Guest Chapter Reader",
                            card_value=data_text["chapter_read_data"]["guest_old_user_count"],
                            card_daily_growth=data_persentase["chapter_read_data"]["guest_old_user_count"],
                            class_name="pembaca-guest-chapter-old-user"
                        )
                    
                    # -- pie chart chapter reader old & new section --
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["pie_chart_old_new_chapter_read"]))

                    # -- bar chart old & new user chapter reader section --
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["pembaca_old_new_chart"]))

                    # -- chapter purchase old & new user section --
                    col19, col20, col21, col22, col23, col24 = st.columns(6)
                    with col19:
                        create_card(
                            st,
                            card_title="New Chapter Purchase With Koin",
                            card_value=data_text["chapter_coin_data"]["new_user_count"],
                            card_daily_growth=data_persentase["chapter_coin_data"]["new_user_count"],
                            class_name="pembeli-chapter-koin-new"
                        )
                    with col20:
                        create_card(
                            st,
                            card_title="New Chapter Purchase With AdsKoin",
                            card_value=data_text["chapter_adscoin_data"]["new_user_count"],
                            card_daily_growth=data_persentase["chapter_adscoin_data"]["new_user_count"],
                            class_name="pembeli-chapter-adskoin-new"
                        )
                    with col21:
                        create_card(
                            st,
                            card_title="New Chapter Purchase With Ads",
                            card_value=data_text["chapter_ads_data"]["new_user_count"],
                            card_daily_growth=data_persentase["chapter_ads_data"]["new_user_count"],
                            class_name="pembeli-chapter-admob-new"
                        )
                    with col22:
                        create_card(
                            st,
                            card_title="Old Chpater Purchase with Koin",
                            card_value=data_text["chapter_coin_data"]["old_user_count"],
                            card_daily_growth=data_persentase["chapter_coin_data"]["old_user_count"],
                            class_name="pembeli-chapter-koin-old"
                        )
                    with col23:
                        create_card(
                            st,
                            card_title="Old Chapter Purchase With AdsKoin",
                            card_value=data_text["chapter_adscoin_data"]["old_user_count"],
                            card_daily_growth=data_persentase["chapter_adscoin_data"]["old_user_count"],
                            class_name="pembeli-chapter-adskoin-old"
                        )
                    with col24:
                        create_card(
                            st,
                            card_title="Old Chapter Purchase With Ads",
                            card_value=data_text["chapter_ads_data"]["old_user_count"],
                            card_daily_growth=data_persentase["chapter_ads_data"]["old_user_count"],
                            class_name="pembeli-chapter-admob-old"
                        )

                    # -- total chapter purchase old & new section --
                    col25, col26 = st.columns(2)
                    with col25:
                        create_card(
                            st,
                            card_title="New Total Pembeli Chapter",
                            card_value=data_text["overall_oldnew_chapter_purchase"]["count"],
                            card_daily_growth=data_persentase["overall_oldnew_chapter_purchase"]["count"],
                            class_name="total-pembeli-chapter-new"
                        )
                    with col26:
                        create_card(
                            st,
                            card_title="Old Total Pembeli Chapter",
                            card_value=data_text["overall_oldnew_chapter_purchase"]["unique"],
                            card_daily_growth=data_persentase["overall_oldnew_chapter_purchase"]["unique"],
                            class_name="total-pembeli-chapter-old"
                        )

                    # -- chapter purchase old & new user pie chart
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["pie_chart_old_new_chapter_purchase"]))
                    
                    # -- chapter purchase old & new bar chart
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["pembeli_old_new_chart"]))

            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")
                if data_persentase.get("message"):
                    st.error(f"Error while feting daily growth data: {data_persentase.get("message", None)}, KeyError: {ke}")
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except Exception as e:
                st.error(f"Error fetching data: {e}") 
