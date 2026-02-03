import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_chapter_purchase_page(host, source):
    """
    This function creates a Streamlit page to display chapter metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">{source.capitalize()} Chapter Purchase Types</h1>""", unsafe_allow_html=True)

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
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"{source}_period_chapter_types")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key=f"{source}_chapter_types_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        chapter_types = {
            None : "",
            "Chapter Purchase With Coin": "chapter_coin",
            "Chapter Purchase With AdsCoin": "chapter_adscoin",
            "Chapter Purchase With Ads": "chapter_ads"

        }
        sort_by = {
            None : "pembeli_chapter_unique",
            "Pembeli Chapter Unique": "pembeli_chapter_unique",
            "Pembeli Chapter Count": "pembeli_chapter_count"
        }
        sort_type = {
            None : "False",
            "Ascending" : "True",
            "Descending" : "False"
        } 
        chapter_types_options = st.selectbox("Chapter Type", list(chapter_types.keys()), placeholder="Chapter Type", index=None, key=f"{source}_chapter_types")
        sort_by_options = st.selectbox("Sort By", list(sort_by.keys()), placeholder="Sort By (Optional)", index=None, key=f"{source}_sort_by_chapter_coin")
        sort_type_options = st.selectbox("Sort Type", list(sort_type.keys()), placeholder="Sort Type (Optional)", index=None, key=f"{source}_sort_type_chapter_coin")
        submit_button = st.button(label="Apply Filters", disabled=False, key=f"{source}_submit_button_chapter_types")
        
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                params = {
                        "from_date": from_date,
                        "to_date": to_date,
                        "chapter_types": chapter_types[chapter_types_options],
                        "sort_by": sort_by[sort_by_options],
                        "sort": sort_type[sort_type_options],
                        "source": source
                    }
                
                data_text = await fetch_data(st, host=host, uri=f'chapter-purchase', params=params)

                # Data Presentation
                card_style(st)
                if data_text:  # Ensure data exists
                    # -- Total Revenue section --
                    st.markdown(f"""<h2 align="center">{chapter_types_options}</h2>""", unsafe_allow_html=True)

                    # -- chapter unique & count section --
                    col1, col2 = st.columns(2)
                    with col1:
                        create_card(
                            st,
                            card_title="Chapter Purchase With Coin (Unique)",
                            card_value=data_text["chapter_data"]["chapter_unique"],
                            card_daily_growth=data_text['data_daily_growth']["chapter_unique"],
                            class_name="pembeli-chapter-unique"
                        )
                    with col2:
                        create_card(
                            st,
                            card_title="Chapter Purchase With Coin (Count)",
                            card_value=data_text["chapter_data"]["chapter_count"],
                            card_daily_growth=data_text['data_daily_growth']["chapter_count"],
                            class_name="pembeli-chapter-count"
                        )
                    
                    # -- chapter unique & count chart section -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_text["unique_count_chart"]))

                    # -- Old & new chapter purchase section --
                    col3, col4 = st.columns(2)
                    with col3:
                        create_card(
                            st, 
                            card_title="New User Chapter Purchase With Coin",
                            card_value=data_text["chapter_data"]["new_user_count"],
                            card_daily_growth=data_text['data_daily_growth']["new_user_count"],
                            class_name="pembeli-chapter-new-user"
                        )
                    with col4:
                        create_card(
                            st,
                            card_title="Old User Chapter Purchase Wtih Coin",
                            card_value=data_text["chapter_data"]["old_user_count"],
                            card_daily_growth=data_text['data_daily_growth']["old_user_count"],
                            class_name="pembeli-chapter-old-user"
                        )

                    # -- old & new chapter purchase chart section
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_text["old_new_chart"]))

                    # -- chapter purchase by day & genre chart section -- 
                    col5, col6 = st.columns(2)
                    with col5:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_text["chapter_by_day_chart"]))
                    with col6:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_text["chapter_by_category"]))

                    # -- chapter purchase coin details table section -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_text["chapter_table"]))

            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 
            
            