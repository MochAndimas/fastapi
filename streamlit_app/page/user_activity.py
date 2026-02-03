import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_user_activity_page(host, source):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">{source.capitalize()} User Activity Time</h1>""", unsafe_allow_html=True)

    with st.form(f"{source}_user_activity"):
        year = {
            None : "2024",
            "2024" : "2024",
            "2023": "2023"
        }
        types = {
            None : "hour",
            "Hour" : "hour",
            "Day" : "day"
        } 
        year_options = st.selectbox("Year", list(year.keys()), placeholder="Choose a Year", index=None, key=f"{source}_user_activity_year")
        types_options = st.selectbox("Types", list(types.keys()), placeholder="Choose a Types", index=None, key=f"{source}_user_activity_type")
        submit_button = st.form_submit_button(label="Apply Filters", disabled=False)
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                params = {
                        "year": year[year_options],
                        "types": types[types_options],
                        "source": source
                    }
                
                data_chart = await fetch_data(st, host=host, uri=f'user-activity', params=params)
                
                if data_chart:
                    st.markdown(f"""<h1 align="center">Engagement Time</h1>""", unsafe_allow_html=True)
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["session_chart"]))

                    st.markdown(f"""<h1 align="center">User Activity By {types[types_options]}</h1>""", unsafe_allow_html=True)
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["chart_activity_time"]))

            except KeyError as ke:
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 