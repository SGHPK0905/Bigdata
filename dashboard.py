import streamlit as st
import pandas as pd
import glob
import time
import os
import plotly.express as px
import plotly.graph_objects as go
import hashlib

st.set_page_config(page_title="SOC XDR", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #0b1121; color: #e2e8f0; }
    .kpi-box { background-color: #162032; border-left: 4px solid #0ea5e9; padding: 15px; border-radius: 5px; text-align: center; }
    .kpi-title { font-size: 14px; color: #94a3b8; }
    .kpi-value { font-size: 32px; font-weight: bold; color: #38bdf8; margin: 5px 0;}
    .alert-text { border-left-color: #ef4444; }
</style>
""", unsafe_allow_html=True)
