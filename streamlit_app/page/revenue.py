import streamlit as st
import datetime
import asyncio
import httpx
from functools import partial
from streamlit_app.functions.functions import get_date_range, fetch_data, create_card, create_chart, card_style
import plotly.io as pio

async def show_revenue_page(host, source):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">{source.capitalize()} Total Revenue - Periods</h1>""", unsafe_allow_html=True)

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
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"{source}_period_revenue")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2022, 1, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key=f"{source}_revenue_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        filters = {
            None : "",
            "Paid": "paid",
            "Expired": "expired",
            "Pending": "pending"
        }
        filter_box = st.selectbox("Payment Status", list(filters.keys()), placeholder="Payment Status (Optional)", index=None, key=f"{source}_filter_box_paid")
        submit_button = st.button(label="Apply Filters", disabled=False, key=f"{source}_submit_button_revenue")
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            params = {
                "source": source,
                "from_date": from_date,
                "to_date": to_date,
                "filters": filters[filter_box]
            }

            # Use partial application for cleaner task creation
            fetch_data_partial = partial(fetch_data, st, host=host)
            tasks = [
                fetch_data_partial(uri=f'revenue', params=params),
                fetch_data_partial(uri=f'revenue/daily-growth', params=params),
                fetch_data_partial(uri=f'revenue/chart', params=params)
            ]
            try:
                data_text, data_persentase, data_chart = await asyncio.gather(*tasks)
            except httpx.RequestError as e:  # Handle potential exceptions
                st.error(f"Error fetching data: {e}")
                
            # Data Presentation
            card_style(st)
            try:
                if data_text and data_persentase and data_chart:  # Ensure data exists
                    # -- Total Revenue section --
                    st.markdown("""<h2 align="center">Total revenue</h2>""", unsafe_allow_html=True)

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        create_card(
                            st,
                            card_title="Total Revenue Ads",
                            card_value=data_text["coin_data"]["Estimated earnings"],
                            card_daily_growth=data_persentase["coin_data"]["Estimated earnings"],
                            types="rp",
                            class_name="total-estimated-earnings"
                        )
                    with col2:
                        create_card(
                            st, 
                            card_title="Total Revenue Coin",
                            card_value=data_text["coin_data"]["gross_revenue"],
                            card_daily_growth=data_persentase["coin_data"]["gross_revenue"],
                            types="rp",
                            class_name="total-gross-revenue"
                        )
                    with col3:
                        create_card(
                            st, 
                            card_title="Total Revenue",
                            card_value=data_text["coin_data"]["overall_revenue"],
                            card_daily_growth=data_persentase["coin_data"]["overall_revenue"],
                            types="rp",
                            class_name="total-revenue"
                        )
                    
                    # -- all revenue chart -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["overall_revenue_chart"]))

                    # -- Coin Transaction section --
                    st.markdown("""<h2 align="center">Coin Transaction</h2>""", unsafe_allow_html=True)

                    col4, col5, col6, col7, col8 = st.columns(5)
                    with col4:
                        create_card(
                            st,
                            card_title="Expired Coin Transaction",
                            card_value=data_text["coin_data"]["count_coin_expired"],
                            card_daily_growth=data_persentase["coin_data"]["count_coin_expired"],
                            class_name="coin-expired"
                        )
                    with col5:
                        create_card(
                            st,
                            card_title="Success Coin Transaction",
                            card_value=data_text["coin_data"]["count_coin_success"],
                            card_daily_growth=data_persentase["coin_data"]["count_coin_success"],
                            class_name="coin-success"
                        )
                    with col6:
                        create_card(
                            st,
                            card_title="Total Coin Transaction",
                            card_value=data_text["coin_data"]["count_coin_total"],
                            card_daily_growth=data_persentase["coin_data"]["count_coin_total"],
                            class_name="total-transaction-coin"
                        )
                    with col7:
                        create_card(
                            st,
                            card_title="Total Revenue",
                            card_value=data_text["coin_data"]["revenue"],
                            card_daily_growth=data_persentase["coin_data"]["revenue"],
                            types="rp",
                            class_name="revenue-period"
                        )
                    with col8:
                        create_card(
                            st, 
                            card_title="Total Gross Revenue",
                            card_value=data_text["coin_data"]["gross_revenue"],
                            card_daily_growth=data_persentase["coin_data"]["gross_revenue"],
                            types="rp",
                            class_name="gross-rev-period"
                        )

                    # -- returning & first coin purchase section --
                    col9, col10 = st.columns(2)
                    with col9:
                        create_card(
                            st,
                            card_title="First Coin Purchase",
                            card_value=data_text["returning_first_purchase"]["first_purchase"],
                            card_daily_growth=data_persentase["returning_first_purchase"]["first_purchase"],
                            class_name="first-purchase-coin"
                        )
                    with col10:
                        create_card(
                            st,
                            card_title="Returning Coin Purchase",
                            card_value=data_text["returning_first_purchase"]["returning_purchase"],
                            card_daily_growth=data_persentase["returning_first_purchase"]["returning_purchase"],
                            class_name="returning-purchase-coin"
                        )

                    # -- first & returning purchase coin chart --
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["chart_returning_first_purchase"]))

                    # --coin transaction chart
                    col11, col12 = st.columns(2)
                    with col11:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["total_transaksi_coin"]))

                    with col12:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["persentase_coin"]))

                    # -- old & new coin transaction
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["old_new_user_pembeli_koin"]))

                    # --coin transaction per value & revenue per days chart
                    col13, col14 = st.columns(2)
                    with col13:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["category_coin"]))

                    with col14:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["revenue_days"]))
                    
                    # -- transaction coin per days
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["coin_days"]))
                    
                    # -- table coin details -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["coin_details"]))

                    # -- ads revenue details -- 
                    st.markdown("""<h2 align="center">Ads Revenue</h2>""", unsafe_allow_html=True)
                    col15, col16, col17 = st.columns(3)
                    with col15:
                        create_card(
                            st,
                            card_title="Impressions",
                            card_value=data_text["coin_data"]["Impressions"],
                            card_daily_growth=data_persentase["coin_data"]["Impressions"],
                            class_name="ads-impressions"
                        )
                    with col16:
                        create_card(
                            st,
                            card_title="Ads Unique Users",
                            card_value=data_text["coin_data"]["unique_user"],
                            card_daily_growth=data_persentase["coin_data"]["unique_user"],
                            class_name="ads-unique-user"
                        )
                    with col17:
                        create_card(
                            st,
                            card_title="Ads Impressions Per Unique Users",
                            card_value=data_text["coin_data"]["impression_per_user"],
                            card_daily_growth=data_persentase["coin_data"]["impression_per_user"],
                            class_name="ads-impression-unique-user"
                        )

                    if source == 'app':
                        col18, col19, col20, col21 = st.columns(4) 
                        with col18:
                            create_card(
                                st, 
                                card_title="Ads Revenue",
                                card_value=data_text["coin_data"]["Estimated earnings"],
                                card_daily_growth=data_persentase["coin_data"]["Estimated earnings"],
                                types="rp",
                                class_name="ads-estimated-earnings"
                            )
                        with col19:
                            create_card(
                                st, 
                                card_title="Ads Observed ECPM",
                                card_value=data_text["coin_data"]["Observed ECPM"],
                                card_daily_growth=data_persentase["coin_data"]["Observed ECPM"],
                                types="rp",
                                class_name="observed-ecpm"
                            )
                        with col20:
                            create_card(
                                st, 
                                card_title="Ads Revenue Per User",
                                card_value=data_text["coin_data"]["revenue_per_user"],
                                card_daily_growth=data_persentase["coin_data"]["revenue_per_user"],
                                types="rp",
                                class_name="ads-revenue-user"
                            )
                        with col21:
                            create_card(
                                st, 
                                card_title="Ads Revenue Per Impressions",
                                card_value=data_text["coin_data"]["Estimated_impressions"],
                                card_daily_growth=data_persentase["coin_data"]["Estimated_impressions"],
                                types="rp",
                                class_name="estimated-earnings-impressions"
                            )
                    else:
                        col18, col19, col20 = st.columns(3) 
                        with col18:
                            create_card(
                                st, 
                                card_title="Ads Revenue",
                                card_value=data_text["coin_data"]["Estimated earnings"],
                                card_daily_growth=data_persentase["coin_data"]["Estimated earnings"],
                                types="rp",
                                class_name="ads-estimated-earnings"
                            )
                        with col19:
                            create_card(
                                st, 
                                card_title="Ads Revenue Per User",
                                card_value=data_text["coin_data"]["revenue_per_user"],
                                card_daily_growth=data_persentase["coin_data"]["revenue_per_user"],
                                types="rp",
                                class_name="ads-revenue-user"
                            )
                        with col20:
                            create_card(
                                st, 
                                card_title="Ads Revenue Per Impressions",
                                card_value=data_text["coin_data"]["Estimated_impressions"],
                                card_daily_growth=data_persentase["coin_data"]["Estimated_impressions"],
                                types="rp",
                                class_name="ads-revenue-impressions"
                            )
                                
                    col22, col23 = st.columns(2)
                    # -- unique & count ads user chart --
                    with col22:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["unique_count_users_admob"]))

                    # -- revenue impression chart -- 
                    with col23:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["impression_revenue_chart"]))
                    
                    # -- ads table -- 
                    with st.container(border=True):
                        st.plotly_chart(pio.from_json(data_chart["ads_details"]))

                    col24, col25 = st.columns(2)
                    # -- freq group chart -- 
                    with col24:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["frequency_distribution_ads_chart"]))
                    
                    # frew group table
                    with col25:
                        with st.container(border=True):
                            st.plotly_chart(pio.from_json(data_chart["frequency_admob_table"]))
        
            except KeyError as ke:
                if data_text.get("message"):
                    st.error(f"Error while feting metrics data: {data_text.get("message", None)}, KeyError: {ke}")
                if data_persentase.get("message"):
                    st.error(f"Error while feting daily growth data: {data_persentase.get("message", None)}, KeyError: {ke}")
                if data_chart.get("message"):
                    st.error(f"Error while feting chart data: {data_chart.get("message", None)}, KeyError: {ke}")

            except Exception as e:
                st.error(f"Error fetching data: {e}")
