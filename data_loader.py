import streamlit as st
import duckdb
import pandas as pd
import ast

def _configure_duckdb_s3_access(con):
    """Helper function to configure DuckDB with S3 credentials from st.secrets."""
    s3_access_key = st.secrets["aws"]["aws_access_key_id"]
    s3_secret_key = st.secrets["aws"]["aws_secret_access_key"]
    s3_region = st.secrets["aws"]["aws_region"]
    
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='{s3_region}';")
    con.execute(f"SET s3_access_key_id='{s3_access_key}';")
    con.execute(f"SET s3_secret_access_key='{s3_secret_key}';")
    return con

def expand_model_usage(df):
    """
    Takes a merged dataframe and expands the 'model_to_messages' column
    into a long format, with one row per model usage.
    """
    # Safely parse the 'model_to_messages' string into a dictionary.
    def parse_model_string(s):
        try:
            return ast.literal_eval(s) if pd.notna(s) else {}
        except (ValueError, SyntaxError):
            return {}

    # Make a copy to avoid modifying the original DataFrame in place
    df_expanded = df.copy()
    df_expanded['model_map'] = df_expanded['model_to_messages'].apply(parse_model_string)

    # Separate users who have model usage data from those who don't
    # This also includes employees who have no usage data at all
    is_inactive = df_expanded['model_map'].apply(lambda d: not d)
    inactive_df = df_expanded[is_inactive].copy()
    inactive_df['model'] = 'none'
    inactive_df['message_count'] = 0

    active_df = df_expanded[~is_inactive].copy()

    if not active_df.empty:
        # Transform the dictionary into a list of items and "explode" the DataFrame
        active_df['model_list'] = active_df['model_map'].apply(lambda d: list(d.items()))
        active_df = active_df.explode('model_list')
        
        # Split the list into 'model' and 'message_count' columns
        active_df[['model', 'message_count']] = pd.DataFrame(
            active_df['model_list'].tolist(), index=active_df.index
        )
    
    # Combine the processed active users with the inactive ones
    final_df = pd.concat([active_df, inactive_df], ignore_index=True)

    # Clean up intermediate and now redundant columns
    final_df.drop(columns=['model_to_messages', 'model_map', 'model_list'], inplace=True, errors='ignore')
    final_df['message_count'] = pd.to_numeric(final_df['message_count'], errors='coerce').fillna(0).astype(int)

    return final_df

@st.cache_data
def load_data_from_s3(report_type):
    """
    Main data pipeline function. Connects to S3, loads raw data,
    processes it, and performs the final join.
    """
    con = duckdb.connect(database=':memory:')
    con = _configure_duckdb_s3_access(con)
    
    bucket_name = st.secrets["aws"]["s3_bucket_name"]
    s3_folder = "weekly" if report_type == "Weekly" else "monthly"
    s3_path = f"s3://{bucket_name}/{s3_folder}/*.csv"
    
    # --- SIMPLIFIED LOADING: Read the FULL CSV files from S3 ---
    try:
        raw_usage_query = f"""
            SELECT 
                email,
                cadence,
                period_start,
                user_status,
                is_active,
                first_day_active_in_period,
                last_day_active_in_period,
                model_to_messages,
                gpts_messaged,
                projects_created,
                created_or_invited_date,
                last_day_active, 
            FROM read_csv_auto('{s3_path}', header=true)
        """
        raw_usage_df = con.execute(raw_usage_query).fetchdf()
    except Exception as e:
        st.error(f"Could not read usage files from S3 folder '{s3_folder}'. Error: {e}")
        return pd.DataFrame()

    employee_path = f"s3://{bucket_name}/employee_info/*.csv"
    try:
        employee_query = f"""
            SELECT
                "First Name" as first_name,
                "Last Name" as last_name,
                "Email Address" as email_address,
                "Company" as company,
                "PBU" as pbu,
                "Department" as department,
                "Job Title" as job_title,
                "Location" as location,
                "Employee Type" as employee_type,
                "Original Hire Date" as original_hire_date
            FROM read_csv_auto('{employee_path}', header=true)
        """
        employee_df = con.execute(employee_query).fetchdf()
    except Exception as e:
        st.error(f"Could not read the employee info file from S3. Error: {e}")
        return pd.DataFrame()

    
    # --- Final Join ---
    con.register('usage_table', raw_usage_df)
    con.register('employee_table', employee_df)
    
    final_join_query = f"""
    SELECT
        COALESCE(e.first_name || ' ' || e.last_name, u.email) AS name,
        COALESCE(e.email_address, u.email) AS email,
        "e".* EXCLUDE (first_name, last_name, email_address),
        "u".* EXCLUDE (email)
    FROM employee_table AS e
    FULL OUTER JOIN usage_table AS u
        ON e.email_address = u.email;
    """
    merged_df = con.execute(final_join_query).fetchdf()


    # --- FINAL STEP: Expand the model usage data ---
    analysis_ready_df = expand_model_usage(merged_df)
    
    return analysis_ready_df