import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import os
from pathlib import Path

# 1. Find where this script is (src folder)
CURRENT_DIR = Path(__file__).parent.absolute()

# 2. Go up one level to the project root, then into 'credentials'
# Adjust the filename 'gcp-key.json' to match your actual file name
KEY_PATH = CURRENT_DIR.parent / "credentials" / "gcp-key.json"

# 3. Initialize the client
client = bigquery.Client.from_service_account_json(os.fspath(KEY_PATH))

# Page config
st.set_page_config(page_title="Olist Executive Dashboard", layout="wide")

# Initialize BigQuery Client
# client = bigquery.Client.from_service_account_json("../gcp-key.json") # Adjust path if needed

st.title("🇧🇷 Olist E-commerce Insights")
st.markdown("Real-time business intelligence from BigQuery Star Schema")

# Fetch data from our NEW Reporting View
@st.cache_data
def get_data():
    query = "SELECT * FROM `olist-data-pipeline-489511.olist_analytics.view_executive_dashboard`"
    data = client.query(query).to_dataframe()
    
    # Standardize column names to lowercase to avoid KeyErrors
    data.columns = [c.lower() for c in data.columns]
    
    return data

df = get_data()
# st.write("Actual Columns in DataFrame:", df.columns.tolist())

# --- SIDEBAR FILTERS ---
st.sidebar.header("Filter Data")
selected_state = st.sidebar.multiselect("Select State", options=df['state'].unique(), default=df['state'].unique())
filtered_df = df[df['state'].isin(selected_state)]

# --- TOP LEVEL METRICS ---
col1, col2, col3 = st.columns(3)
col1.metric("Total Revenue", f"R$ {filtered_df['revenue'].sum():,.0f}")
col2.metric("Total Orders", f"{filtered_df['order_id'].nunique():,}")
col3.metric("Avg Order Value", f"R$ {filtered_df['revenue'].mean():,.2f}")

# --- VISUALIZATIONS ---
c1, c2 = st.columns(2)

with c1:
    st.subheader("Revenue by Category")
    fig_cat = px.bar(filtered_df.groupby('product_category')['revenue'].sum().nlargest(10).reset_index(), 
                     x='revenue', y='product_category', orientation='h', color='revenue')
    st.plotly_chart(fig_cat, use_container_width=True)

with c2:
    st.subheader("Sales Trend")
    filtered_df['order_date'] = pd.to_datetime(filtered_df['order_date'])
    trend_df = filtered_df.groupby('order_date')['revenue'].sum().reset_index()
    fig_trend = px.line(trend_df, x='order_date', y='revenue')
    st.plotly_chart(fig_trend, use_container_width=True)

st.dataframe(filtered_df.head(100), use_container_width=True)