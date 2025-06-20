import streamlit as st
from data_loader import load_data_from_s3

st.set_page_config(layout="wide")

st.title("User Usage Data")

report_type = st.selectbox("Select Report Type", ["Weekly", "Monthly"])

if st.button("Load Data"):
    final_df = load_data_from_s3(report_type)
    if not final_df.empty:
        st.dataframe(final_df)
    else:
        st.warning("No data loaded. Please check the S3 bucket and file paths.")
