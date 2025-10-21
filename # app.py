# app.py

import streamlit as st
import pandas as pd
from log_parser import parse_log_file
from config import CEID_MAP

st.set_page_config(page_title="Hirata Log Analyzer", layout="wide")
st.title("Hirata Equipment Log Analyzer")

uploaded_file = st.file_uploader("Upload your Hirata Log File (.txt or .log)", type=['txt', 'log'])

if uploaded_file:
    with st.spinner("Analyzing log file..."):
        # The parser now only returns events that have details.
        parsed_events = parse_log_file(uploaded_file)
    
    st.header("Detailed Event Log")

    if parsed_events:
        df = pd.json_normalize(parsed_events)
        
        # Create a human-readable 'EventName' column
        if 'details.CEID' in df.columns:
            # Coerce errors to handle potential non-numeric data gracefully
            df['details.CEID'] = pd.to_numeric(df['details.CEID'], errors='coerce')
            df['EventName'] = df['details.CEID'].map(CEID_MAP).fillna("Unknown Event")
        elif 'details.RCMD' in df.columns:
             df['EventName'] = df['details.RCMD']
        else:
             df['EventName'] = "Unknown"

        cols_in_order = [
            "timestamp", "msg_name", "EventName", "details.LotID", "details.PanelCount",
            "details.MagazineID", "details.OperatorID", "details.PortID", "details.PortStatus",
            "details.AlarmID"
        ]
        
        display_cols = [col for col in cols_in_order if col in df.columns]
        
        st.metric(label="Meaningful Events Found", value=len(df))
        st.dataframe(df[display_cols])

        with st.expander("Show Raw JSON Data (First 20 Events for Debugging)"):
            st.json(parsed_events[:20])
    else:
        st.warning("No meaningful S6F11 or S2F49 messages were found in the log file.")

else:
    st.info("Please upload a log file to begin analysis.")
