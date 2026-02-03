import streamlit as st
import requests
import datetime
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_retention_page(host, source):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">{source.capitalize()} Retention Data</h1>""", unsafe_allow_html=True)
    
    with st.container(border=True):
        event_name = {
            None: "user_read_chapter",
            "User Read Chapter": "user_read_chapter",
            "User Buy Chapter With Coin": "user_buy_chapter_coin",
            "User Buy Chapter With AdsCoin": "user_buy_chapter_adscoin",
            "User Buy Chapter With Ads": "user_buy_chapter_ads",
            "User Buy Coin": "user_coin_purchase"
        }
        data = {
            None: "persentase",
            "Persentase": "persentase",
            "Total User": "total_user"
        }
        period = {
            None: "Daily",
            "Daily": "Daily",
            "Monthly": "Monthly"
            }
        event_name_options = st.selectbox("Event Name", list(event_name.keys()), placeholder="Choose a Event Name", index=None, key=f"{source}_event_name_retention")
        data_options = st.selectbox("Data Types", list(data.keys()), placeholder="Choose a Data Types", index=None, key=f"{source}_data_type_retention")
        period_options = st.selectbox("Periods", list(period.keys()), placeholder="Choose a Periods", index=None, key=f"{source}_period_retention")
        if period[period_options] == "Daily":
            preset_date = {
                None: "last_7_days",
                "Last 7 Days": "last_7_days",
                "Last 14 Days": "last_14_days",
                "Last 28 Days": "last_28_days"
            }
            preset_date_options = st.selectbox("Preset Date", list(preset_date.keys()), placeholder="Choose a Preset Date", index=None, key=f"{source}_preset_date_retention")
        else:
            preset_date = {
                None: "last_3_months",
                "Last 3 Months": "last_3_months",
                "Last 6 Months": "last_6_months",
                "Last 12 Months": "last_12_months"
            }
            preset_date_options = st.selectbox("Preset Date", list(preset_date.keys()), placeholder="Choose a Preset Date", index=None, key=f"{source}_preset_date_retention")
        submit_button = st.button(label="Apply Filters", disabled=False)

    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                params = {
                        "event_name": event_name[event_name_options],
                        "data": data[data_options],
                        "period": period[period_options],
                        "preset_date": preset_date[preset_date_options],
                        "source": source
                    }
                
                data_chart = await fetch_data(st, host=host, uri=f'retention', params=params)
                
                if data_chart:
                    st.markdown(f"""<h1 align="center"> {period[period_options]} Retention {event_name[event_name_options]}</h1>""", unsafe_allow_html=True)
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["retention_charts"]))

                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["table_cohort"]))

            except KeyError as ke:
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except requests.RequestException as e:
                st.error(f"Error fetching data: {e}") 