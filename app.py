import streamlit as st
from data_loader import load_data_from_s3

st.set_page_config(layout="wide")

st.title("User Usage Data")

if st.sidebar.button("Load / Refresh Data", type="primary"):
    st.cache_data.clear()
    st.sidebar.success("Cache cleared. Data will be reloaded from S3.")

report_type = st.selectbox("Select Report Type", ["Weekly", "Monthly"])

if st.button("Load Data"):
    final_df = load_data_from_s3(report_type)
    if not final_df.empty:
        st.dataframe(final_df)
    else:
        st.warning("No data loaded. Please check the S3 bucket and file paths.")
