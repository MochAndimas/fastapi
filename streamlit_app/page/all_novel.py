import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_all_novel_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">Novel Analytics</h1>""", unsafe_allow_html=True)

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
            "Last Week" : (last_week_start, last_week_end),
            "Last 7 Days": (last_7days_start, last_7days_end) 
        }
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"period_novel_analytics")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key="novel_analytics_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        columns = {
            None : "reader_purchase_percentage",
            "Reader To Purchase %" : "reader_purchase_percentage",
            "Unpublished Chapter": "unpublised_chapter",
            "Register Reader": "registered_reader",
            "Guest Reader": "guest_reader",
            "Total Reader": "total_reader",
            "Chapter Purchase With Coin": "chapter_purchase_coin",
            "Chapter Purchase With AdsCoin": "chapter_purchase_adscoin",
            "Chapter Purchase With Ads": "chapter_purchase_ads",
        }
        category = {
            None: "",
            "Romansa": "romansa",
            "Fantasi": "fantasi",
            "Misteri": "misteri",
            "Sains Fiksi": "sains fiksi",
            'Horor': "horor",
            "Thriller": "thriller",
            "Action": "action",
            'Teen Fiction': "teen fiction",
            "Comedy": "comedy",
            "Religi": "religi",
            "Drama": "drama",
            "Family": "family",
            "Adult": "adult",

        }
        order_val = {
            None : False,
            'Ascending': True, 
            'Descending': False
        }
        search = st.text_input("Novel Title", placeholder="Novel Title (Optional)", key="title_novel_analytics")
        category_option =  st.selectbox("Novel Category", list(category.keys()), placeholder="Novel Category (Optional)", index=None, key="category_novel_analytics")
        sort_by = st.selectbox("Sort By", list(columns.keys()), placeholder="Sort By  (Optional)", index=None, key="sort_by_novel_analytics")
        order = st.selectbox("Order By", list(order_val.keys()), placeholder="Order By (Optional)", index=None, key="order_novel_analytics")
        submit_button = st.button(label="Apply Filters", disabled=False, key="submit_button_novel_analytics")
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                params = {
                        "from_date": from_date,
                        "to_date": to_date,
                        "novel_title": search,
                        "category_novel": category[category_option],
                        "sort_by": columns[sort_by],
                        "ascending": order_val[order]
                    }
                
                data = await fetch_data(st, host=host, uri=f'novel/novel-analytics', params=params)
                
                card_style(st)
                if data:
                    st.markdown(f"""<h1 align="center">Novel Analytics</h1>""", unsafe_allow_html=True)

                    create_chart(st, data["novel_table"])

            except KeyError as ke:
                if data.get("message"):
                    st.error(f"Error while feting metrics data: {data.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 