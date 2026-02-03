import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

def show_chapter_ads_page(host, source):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">{source.capitalize()} Chapter Purchase With Ads</h1>""", unsafe_allow_html=True)

    with st.form(f"{source}_chapter_ads_form"):
        from_date, to_date = st.date_input(
            "Select Date Range",
            value=(get_date_range(days=7, period='days')),
            min_value=datetime.date(2022, 1, 1),
            max_value=get_date_range(days=2, period='days')[1],
            key=f"{source}_chapter_ads_date"
        )
        sort_by = {
            None : "total_pembelian_chapter_unique",
            "Pembeli Chapter Unique": "total_pembelian_chapter_unique",
            "Pembeli Chapter Count": "total_pembelian_chapter_count"
        }
        sort_type = {
            None : "False",
            "Ascending" : "True",
            "Descending" : "False"
        } 
        sort_by_options = st.selectbox("Sort By", list(sort_by.keys()), placeholder="Sort By (Optional)", index=None, key=f"{source}_sort_by_chapter_ads")
        sort_type_options = st.selectbox("Sort Type", list(sort_type.keys()), placeholder="Sort Type (Optional)", index=None, key=f"{source}_sort_type_chapter_ads")
        submit_button = st.form_submit_button(label="Apply Filters", disabled=False)
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                form_data = {
                        "from_date": from_date,
                        "to_date": to_date,
                        "sort_by": sort_by[sort_by_options],
                        "ascendings": sort_type[sort_type_options]
                    }
                
                data_text = fetch_data(st, host=host, uri=f'{source}-chapter-type/ads', data=form_data)
                data_chart = fetch_data(st, host=host, uri=f'{source}-chapter-type/ads-chart', data=form_data)

                # Data Presentation
                card_style(st)
                if data_text and data_chart:  # Ensure data exists
                    # -- Total Revenue section --
                    st.markdown("""<h2 align="center">Chapter Purchase With Ads</h2>""", unsafe_allow_html=True)

                    # -- chapter unique & count section --
                    col1, col2 = st.columns(2)
                    with col1:
                        create_card(
                            st,
                            card_title="Chapter Purchase With Ads (Unique)",
                            card_value=data_text["pembelian_chapter_unique"],
                            card_daily_growth=data_text['dg_chapter_unique'],
                            class_name="pembeli-chapter-unique"
                        )
                    with col2:
                        create_card(
                            st,
                            card_title="Chapter Purchase With Ads (Count)",
                            card_value=data_text["pembelian_chapter_count"],
                            card_daily_growth=data_text["dg_chapter_count"],
                            class_name="pembeli-chapter-count"
                        )
                    
                    # -- chapter unique & count chart section -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["beli_chapter_unique_count_period"]))

                    # -- Old & new chapter purchase section --
                    col3, col4 = st.columns(2)
                    with col3:
                        create_card(
                            st, 
                            card_title="New User Chapter Purchase With Ads",
                            card_value=data_text["new_user_admob"],
                            card_daily_growth=data_text["dg_new_user_admob"],
                            class_name="pembeli-chapter-new-user"
                        )
                    with col4:
                        create_card(
                            st,
                            card_title="Old User Chapter Purchase Wtih Ads",
                            card_value=data_text["old_user_admob"],
                            card_daily_growth=data_text["dg_old_user_admob"],
                            class_name="pembeli-chapter-old-user"
                        )

                    # -- old & new chapter purchase chart section
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["old_new_admob"]))

                    # -- chapter purchase by day & genre chart section -- 
                    col5, col6 = st.columns(2)
                    with col5:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["admob_chapter_day"]))
                    with col6:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["admob_chapter_genre"]))

                    # -- chapter purchase coin details table section -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["beli_chapter_details"]))

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 