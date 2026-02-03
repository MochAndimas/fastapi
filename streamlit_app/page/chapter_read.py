import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, card_style
from streamlit_app.functions.functions import create_card, create_chart

async def show_chapter_reader_page(host, source):
    """
    This function creates a Streamlit page to display chapter metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">{source.capitalize()} Chapter Reader Types</h1>""", unsafe_allow_html=True)

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
        sort_by = {
            None : "pembaca_chapter_unique",
            "Chapter Reader Unique": "pembaca_chapter_unique",
            "Chapter Reader Count": "pembaca_chapter_count"
        }
        sort_type = {
            None : "False",
            "Ascending" : "True",
            "Descending" : "False"
        }
        is_completed = {
            None: [True, False],
            "All Chapter Reader": [True, False],
            "Completed Chapter Reader": [True],
            "Uncompleted Chapter Reader": [False]
        }

        is_completed_optios = st.selectbox("Chapter Reader Types", list(is_completed.keys()), placeholder="Chapter Reader Types", index=None, key=f"{source}_chapter_reader_types")
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
                        "sort_by": sort_by[sort_by_options],
                        "sort": sort_type[sort_type_options],
                        "read_is_completed": is_completed[is_completed_optios],
                        "source": source
                    }
                data_text = await fetch_data(st, host=host, uri=f'chapter-read', params=params)
                
                # Data Presentation
                card_style(st)
                if data_text:  # Ensure data exists
                    # -- Total Revenue section --
                    st.markdown(f"""<h2 align="center">{is_completed_optios}</h2>""", unsafe_allow_html=True)

                    # -- Guest & register chapter reader section --
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        create_card(
                            st,
                            card_title="Register Chapter Reader (Unique)",
                            card_value=data_text["chapter_read_data"]["unique_register_reader"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["unique_register_reader"],
                            class_name="chapter_register_reader_unique"
                        )
                    with col2:
                        create_card(
                            st,
                            card_title="Guest Chapter Reader (Unique)",
                            card_value=data_text["chapter_read_data"]["unique_guest_reader"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["unique_guest_reader"],
                            class_name="chapter_guest_reader_unique"
                        )
                    with col3:
                        create_card(
                            st,
                            card_title="Register Chapter Reader (Count)",
                            card_value=data_text["chapter_read_data"]["count_register_reader"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["count_guest_reader"],
                            class_name="chapter_register_reader_count"
                        )
                    with col4:
                        create_card(
                            st,
                            card_title="Guest Chapter Reader",
                            card_value=data_text["chapter_read_data"]["count_guest_reader"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["count_guest_reader"],
                            class_name="chapter_guest_reader_count"
                        )
                    
                    col5, col6 = st.columns(2)
                    with col5:
                        create_card(
                            st,
                            card_title="Total Chapter Reader (Unique)",
                            card_value=data_text["chapter_read_data"]["pembaca_chapter_unique"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["pembaca_chapter_unique"],
                            class_name="total_chapter_reader_unqiue"
                        )
                    with col6:
                        create_card(
                            st,
                            card_title="Total Chapter Reader (Count)",
                            card_value=data_text["chapter_read_data"]["pembaca_chapter_count"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["pembaca_chapter_count"],
                            class_name="total_chapter_reader_count"
                        )

                    create_chart(st, data_text["chart_unique_count"])

                    # -- Guest & register old new chapter reader section --
                    col7, col8, col9, col10 = st.columns(4)
                    with col7:
                        create_card(
                            st,
                            card_title="New User Register Chapter reader (Unique)",
                            card_value=data_text["chapter_read_data"]["new_user_count"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["new_user_count"],
                            class_name="register_new_user_chapter_reader"
                        )
                    with col8:
                        create_card(
                            st,
                            card_title="Old User Guest Chapter Reader (Unique)",
                            card_value=data_text["chapter_read_data"]["old_user_count"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["old_user_count"],
                            class_name="old_user_guest_chapter_reader"
                        )
                    with col9:
                        create_card(
                            st,
                            card_title="New User Register Chapter Reader (Count)",
                            card_value=data_text["chapter_read_data"]["guest_new_user_count"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["guest_new_user_count"],
                            class_name="new_user_register_chapter_reader_count"
                        )
                    with col10:
                        create_card(
                            st,
                            card_title="Old User Guest Chapter Reader (Count)",
                            card_value=data_text["chapter_read_data"]["guest_old_user_count"],
                            card_daily_growth=data_text["daily_growth"]["chapter_read_data"]["guest_old_user_count"],
                            class_name="old_user_guest_chapter_reader_count"
                        )
                    
                    create_chart(st, data_text["chart_old_new"])

                    col11, col12 = st.columns(2)
                    with col11:
                        create_chart(st, data_text["chart_day"])
                    with col12:
                        create_chart(st, data_text["chart_category"])
                    
                    create_chart(st, data_text["chart_table"])

                    col13, col14 = st.columns(2)
                    with col13:
                        create_chart(st, data_text["frequency_table"])
                    with col14:
                        create_chart(st, data_text["frequency_chart"])

            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 
            
            