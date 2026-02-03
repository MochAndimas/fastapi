import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

def show_chapter_adscoin_page(host, source):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">{source.capitalize()} Chapter Purchase With AdsCoin</h1>""", unsafe_allow_html=True)

    with st.form(f"{source}_chapter_adscoin_form"):
        from_date, to_date = st.date_input(
            "Select Date Range",
            value=(get_date_range(days=7, period='days')),
            min_value=datetime.date(2022, 1, 1),
            max_value=get_date_range(days=2, period='days')[1],
            key=f"{source}_chapter_adscoin_date"
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
        sort_by_options = st.selectbox("Sort By", list(sort_by.keys()), placeholder="Sort By (Optional)", index=None, key=f"{source}_sort_by_chapter_adscoin")
        sort_type_options = st.selectbox("Sort Type", list(sort_type.keys()), placeholder="Sort Type (Optional)", index=None, key=f"{source}_sort_type_chapter_adscoin")
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
                
                data_text = fetch_data(st, host=host, uri=f'{source}-chapter-type/adscoin', data=form_data)
                data_chart = fetch_data(st, host=host, uri=f'{source}-chapter-type/adscoin-chart', data=form_data)

                # Data Presentation
                card_style(st)
                if data_text and data_chart:  # Ensure data exists
                    # -- Total Revenue section --
                    st.markdown("""<h2 align="center">Chapter Purchase With AdsCoin</h2>""", unsafe_allow_html=True)

                    # -- chapter unique & count section --
                    col1, col2 = st.columns(2)
                    with col1:
                        create_card(
                            st,
                            card_title="Chapter Purchase With AdsCoin (Unique)",
                            card_value=data_text["adskoin_unique_periode"],
                            card_daily_growth=data_text['dg_adskoin_unique'],
                            class_name="pembeli-chapter-unique"
                        )
                    with col2:
                        create_card(
                            st,
                            card_title="Chapter Purchase With AdsCoin (Count)",
                            card_value=data_text["adskoin_count_periode"],
                            card_daily_growth=data_text["dg_adskoin_count"],
                            class_name="pembeli-chapter-count"
                        )
                    
                    # -- chapter unique & count chart section -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["adskoin_unique_count_chart_periode"]))

                    # -- Old & new chapter purchase section --
                    col3, col4 = st.columns(2)
                    with col3:
                        create_card(
                            st, 
                            card_title="New User Chapter Purchase With AdsCoin",
                            card_value=data_text["adskoin_new_user"],
                            card_daily_growth=data_text["dg_new_user"],
                            class_name="pembeli-chapter-new-user"
                        )
                    with col4:
                        create_card(
                            st,
                            card_title="Old User Chapter Purchase Wtih AdsCoin",
                            card_value=data_text["adskoin_old_user"],
                            card_daily_growth=data_text["dg_old_user"],
                            class_name="pembeli-chapter-old-user"
                        )

                    # -- old & new chapter purchase chart section
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["adskoin_old_new_chart"]))

                    # -- chapter purchase by day & genre chart section -- 
                    col5, col6 = st.columns(2)
                    with col5:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["adskoin_chapter_day"]))
                    with col6:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["adskoin_chapter_genre"]))

                    # -- chapter purchase coin details table section -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["adskoin_novel_details_periode"]))

                    # -- user adskoin details section -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["user_wallet_adskoin"]))

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 