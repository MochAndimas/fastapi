import streamlit as st
# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Gooddreamer Analytics",
    page_icon="./streamlit_app/page/LOGO_150x150.png",
    layout="wide",  # Optional: Use "wide" for full-width layout
    initial_sidebar_state="collapsed", # Start with the sidebar opened 
)
import asyncio
from decouple import config
from streamlit_app.page import chapter_purchase, overall, login, chapter
from streamlit_app.page import revenue, user_activity, chapter_read
from streamlit_app.page import retention, redeem_code, illustration_transaction
from streamlit_app.page import offline_mode, new_install, sem, seo, aggregated
from streamlit_app.page import data_all_time, novel_details, all_novel, logger
from streamlit_app.functions.functions import footer, logout, cookie_controller
from streamlit_app.functions.functions import get_session

# --- App Settings ---
session_id = get_session(cookie_controller.get("session_id"))
HOST = st.secrets["api"]["HOST"] if config("ENV") == "production" else st.secrets["api"]["DEV_HOST"]
footer(st)

# --- Initialize session state ---
if 'page' not in st.session_state:
    st.session_state.page = 'login'
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'role' not in st.session_state:
    st.session_state.role = None

# --- PAGE DEFINITIONS ---
# Logger Page
logger_page = st.Page(page=logger, title="Update Data", url_path="/update-date")

# Login Page
login_page = st.Page(page=login, title="Login")

# Overview data page
overall_page = st.Page(page=overall, title="Overall Data", url_path="/overall-page", icon="🗃")
aggregated_page = st.Page(page=aggregated, title="Aggregated Data", url_path="aggregated", icon="📋")
data_all_time_page = st.Page(page=data_all_time, title="Data All Time", url_path="data-all-time", icon="📊")
all_user_activity = st.Page(page=user_activity, title="User Activity", url_path="all-user-activity", icon="👣")
all_retention = st.Page(page=retention, title="Retention", url_path="all-retention", icon="📉")
novel_details_page = st.Page(page=novel_details, title="Novel Details", url_path="novel-details", icon="📖")
all_novel_page = st.Page(page=all_novel, title="Novel Analytics", url_path="novel-analytics", icon="📑")

# Acquisition page
new_install_page = st.Page(page=new_install, title="New Install", url_path="new-install", icon="🆕️")
seo_page = st.Page(page=seo, title="SEO", url_path="seo", icon="⌨️")
sem_page = st.Page(page=sem, title="SEM", url_path="sem", icon="💻")

# App data page
app_revenue_page = st.Page(page=revenue, title="💵 Revenue Coin & Admob", url_path="app-revenue", icon="📱")
app_chapter_page = st.Page(page=chapter, title="📚 All Novel Reader & Purchase",url_path="app-chapter-data", icon="📱")
app_chapter_read_page = st.Page(page=chapter_read, title="📖 Chapter Reader", url_path="app-chapter-read", icon="📱")
app_chapter_purchase = st.Page(page=chapter_purchase, title="📕 Chapter Purchase Types", url_path="app-chapter-purchase", icon="📱")
app_user_activity = st.Page(page=user_activity, title="👣 User Activity", url_path="app-user-activity", icon="📱")
app_retention = st.Page(page=retention, title="📉 Retention", url_path="app-retention", icon="📱")

# Web data Page
web_revenue_page = st.Page(page=revenue, title="💵 Revenue Coin & Adsense", url_path="web-revenue", icon="🖥️")
web_chapter_page = st.Page(page=chapter, title="📚 All Novel Reader & Purchase",url_path="web-chapter-data", icon="🖥️")
web_chapter_read_page = st.Page(page=chapter_read, title="📖 Chapter Reader", url_path="web-chapter-read", icon="🖥️")
web_chapter_purchase = st.Page(page=chapter_purchase, title="📕 Chapter Purchase Types", url_path="web-chapter-purchase", icon="🖥️")
web_user_activity = st.Page(page=user_activity, title="👣 User Activity", url_path="web-user-activity", icon="🖥️")
web_retention = st.Page(page=retention, title="📉 Retention", url_path="web-retention", icon="🖥️")

# Feature Data
redeem_code_page = st.Page(page=redeem_code, title="Redeem Code", url_path="redeem-code", icon="🎟")
illustration_page = st.Page(page=illustration_transaction, title="Illustration Transaction", url_path="illustration-transaction", icon="🎨")
offline_mode_page = st.Page(page=offline_mode, title="Offline Mode", url_path="offline-mode", icon="📴")

# show menu only for spesific role
if st.session_state.role in ['developer', 'superadmin']:
    menu_options = {
        "🗂 Overview Data" : [
            overall_page, aggregated_page, data_all_time_page, 
            all_user_activity, all_retention, novel_details_page, 
            all_novel_page],
        "🔎 Acquisition Data" : [new_install_page, seo_page, sem_page],
        "📱 App Data" : [
            app_revenue_page, app_chapter_page, app_chapter_read_page,
            app_chapter_purchase, app_user_activity,
            app_retention],
        "🖥️ Web Data" : [
            web_revenue_page, web_chapter_page, web_chapter_read_page,
            web_chapter_purchase, web_user_activity, 
            web_retention],
        "📲 Feature Data" : [redeem_code_page, illustration_page, offline_mode_page]
    }
    if st.session_state.role == 'developer':
        menu_options["Update Data"] = [logger_page]
elif st.session_state.role == 'growth':
    menu_options = {
        "🗂 Overview Data" : [
            overall_page, aggregated_page, all_user_activity, 
            all_retention, novel_details_page, all_novel_page],
        "🔎 Acquisition Data" : [new_install_page, seo_page, sem_page],
        "📱 App Data" : [
            app_chapter_page, app_chapter_read_page, app_chapter_purchase,
            app_user_activity, app_retention],
        "🖥️ Web Data" : [
            web_chapter_page, web_chapter_read_page, web_chapter_purchase,
            web_user_activity, web_retention],
        "📲 Feature Data" : [redeem_code_page, illustration_page, offline_mode_page]
    }
elif st.session_state.role == 'operation':
    menu_options = {
    "🗂 Overview Data" : [aggregated_page, novel_details_page, all_novel_page],
    "📱 App Data" : [
        app_chapter_page, app_chapter_read_page, app_chapter_purchase],
    "🖥️ Web Data" : [
        web_chapter_page, web_chapter_read_page, web_chapter_purchase],
    "📲 Feature Data" : [redeem_code_page, illustration_page, offline_mode_page]
    }

# --- NAVIGATION ---
with st.sidebar:
    if st.session_state["logged_in"]:
        st.image("./streamlit_app/page/gd_wide.png", use_column_width=True)
        page = st.navigation(
            menu_options,
            position='sidebar'
        )
        asyncio.run(logout(st, HOST, session_id))
    else:
        page = st.navigation(
            [login_page],
            position='sidebar'
        )

# --- PAGE CONTENT ---
try:
    if not st.session_state['logged_in']:
        asyncio.run(login.show_login_page(HOST))
    else:
        page_handlers = {
            overall_page: lambda: overall.show_overall_page(HOST),
            aggregated_page: lambda: aggregated.show_aggregated_page(HOST),
            data_all_time_page: lambda: data_all_time.show_data_all_time_page(HOST),
            all_user_activity: lambda: user_activity.show_user_activity_page(HOST, source='all'),
            all_retention: lambda: retention.show_retention_page(HOST, source='all'),
            novel_details_page: lambda: novel_details.show_novel_details_page(HOST),
            all_novel_page: lambda: all_novel.show_all_novel_page(HOST),
            new_install_page: lambda: new_install.show_new_install_page(HOST),
            seo_page: lambda: seo.show_seo_page(HOST),
            sem_page: lambda: sem.show_sem_page(HOST),
            app_chapter_page: lambda: chapter.show_chapter_page(HOST, source='app'),
            web_chapter_page: lambda: chapter.show_chapter_page(HOST, source='web'),
            app_revenue_page: lambda: revenue.show_revenue_page(HOST, source='app'),
            web_revenue_page: lambda: revenue.show_revenue_page(HOST, source='web'),
            app_chapter_read_page: lambda: chapter_read.show_chapter_reader_page(HOST, source="app"),
            web_chapter_read_page: lambda: chapter_read.show_chapter_reader_page(HOST, source="web"),
            app_chapter_purchase: lambda: chapter_purchase.show_chapter_purchase_page(HOST, source='app'),
            web_chapter_purchase: lambda: chapter_purchase.show_chapter_purchase_page(HOST, source='web'),
            app_user_activity: lambda: user_activity.show_user_activity_page(HOST, source='app'),
            web_user_activity: lambda: user_activity.show_user_activity_page(HOST, source='web'),
            app_retention: lambda: retention.show_retention_page(HOST, source='app'),
            web_retention: lambda: retention.show_retention_page(HOST, source='web'),
            redeem_code_page: lambda: redeem_code.show_redeem_code_page(HOST),
            illustration_page: lambda: illustration_transaction.show_illustration_transaction_page(HOST),
            offline_mode_page: lambda: offline_mode.show_offline_mode_page(HOST),
            logger_page: lambda: logger.show_logger_page(HOST)
        }
        if page in page_handlers:
            asyncio.run(page_handlers[page]())  # Call the appropriate function based on the page
            st.session_state.page = page.url_path
except Exception as e:
    st.error(f"Error Fetching Data! {e}")

