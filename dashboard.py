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

if 'last_valid_df' not in st.session_state:
    st.session_state.last_valid_df = pd.DataFrame()
if 'blacklisted_ips' not in st.session_state:
    st.session_state.blacklisted_ips = pd.DataFrame()

def get_lat_lon(ip):
    h = int(hashlib.md5(ip.encode()).hexdigest(), 16)
    return (h % 160) - 80, ((h // 160) % 360) - 180

list_of_files = glob.glob('ket_qua_output/*.csv')
if list_of_files:
    try:
        df_list = [pd.read_csv(f) for f in list_of_files if os.path.getsize(f) > 0]
        if df_list:
            temp_df = pd.concat(df_list, ignore_index=True)
            temp_df['time'] = pd.to_datetime(temp_df['time'], errors='coerce')
            st.session_state.last_valid_df = temp_df
    except:
        pass 

df = st.session_state.last_valid_df
attack_ips = len(st.session_state.blacklisted_ips)

col_title, col_toggle = st.columns([3, 1])
with col_title:
    st.write("")
with col_toggle:
    st.write("")
    is_paused = st.toggle("⏸️ TẠM DỪNG ĐỂ PHÂN TÍCH (PAUSE)", value=False)

if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    col1.markdown(f'<div class="kpi-box"><div class="kpi-title">Traffic</div><div class="kpi-value">{len(df)}</div></div>', unsafe_allow_html=True)
    col2.markdown(f'<div class="kpi-box alert-text"><div class="kpi-title">Errors 401</div><div class="kpi-value" style="color:#f87171;">{len(df[df["status"] == 401])}</div></div>', unsafe_allow_html=True)
    col3.markdown(f'<div class="kpi-box alert-text"><div class="kpi-title">Blocked</div><div class="kpi-value" style="color:#ef4444;">{attack_ips}</div></div>', unsafe_allow_html=True)
    col4.markdown(f'<div class="kpi-box"><div class="kpi-title">Time</div><div class="kpi-value" style="font-size:24px;">{time.strftime("%H:%M:%S")}</div></div>', unsafe_allow_html=True)
    
    st.write("")

    brute_df = df[(df['status'] == 401) & (df['url'] == '/login')]
    if not brute_df.empty:
        hacker_ips = brute_df.groupby('ip').size().reset_index(name='count')
        hackers = hacker_ips[hacker_ips['count'] > 10]
        if not hackers.empty:
            st.session_state.blacklisted_ips = pd.concat([st.session_state.blacklisted_ips, hackers]).drop_duplicates(subset=['ip'], keep='last')
            attack_ips = len(st.session_state.blacklisted_ips)

    map_col, table_col = st.columns([2, 1])
    with map_col:
        if not st.session_state.blacklisted_ips.empty:
            map_data = st.session_state.blacklisted_ips.copy()
            map_data['lat'], map_data['lon'] = zip(*map_data['ip'].apply(get_lat_lon))
            fig_map = px.scatter_geo(map_data, lat='lat', lon='lon', size='count', color='count', color_continuous_scale="Reds", projection="natural earth", size_max=25)
            fig_map.update_layout(uirevision='map', margin={"r":0,"t":0,"l":0,"b":0}, geo=dict(bgcolor='#0b1121', showland=True, landcolor='#1e293b', showocean=True, oceancolor='#0f172a', showcountries=True, countrycolor='#334155'), paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', coloraxis_showscale=False)
            st.plotly_chart(fig_map, width='stretch', key="map")
        else:
            st.info("No threats detected.")

    with table_col:
        if not st.session_state.blacklisted_ips.empty:
            display_df = st.session_state.blacklisted_ips.sort_values(by='count', ascending=False).head(10)
            display_df.columns = ['Attacker IP', 'Attempts']
            display_df.index = range(1, len(display_df) + 1)
            st.dataframe(display_df, width='stretch', height=350)

    chart_col1, chart_col2 = st.columns([2, 1])
    with chart_col1:
        if 'time' in df.columns:
            time_df = df.dropna(subset=['time']).copy()
            time_df['time_sec'] = time_df['time'].dt.floor('s') 
            
            latest_time = time_df['time_sec'].max()
            start_time = latest_time - pd.Timedelta(seconds=60)
            recent_df = time_df[time_df['time_sec'] >= start_time]
            
            trend = recent_df.groupby('time_sec').size().reset_index(name='Requests')
            
            fig_line = px.area(trend, x='time_sec', y='Requests', template="plotly_dark")
            
            fig_line.update_layout(
                xaxis=dict(range=[start_time, latest_time], fixedrange=False), 
                yaxis=dict(fixedrange=False),
                paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":10}
            )
            fig_line.update_traces(mode='lines+markers', marker=dict(size=4), fillcolor='rgba(14, 165, 233, 0.2)')
            
            st.plotly_chart(fig_line, width='stretch', key="line", config={'scrollZoom': True, 'displayModeBar': True})

    with chart_col2:
        status_dist = df.groupby('status').size().reset_index(name='count')
        status_dist['status'] = status_dist['status'].astype(str)
        fig_pie = px.pie(status_dist, values='count', names='status', hole=0.5, template="plotly_dark")
        fig_pie.update_layout(uirevision='pie_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":10, "b":10})
        st.plotly_chart(fig_pie, width='stretch', key="pie")

    c3, c4 = st.columns([2, 1])
    with c3:
        url_dist = df.groupby('url').size().reset_index(name='count').sort_values(by='count')
        fig_bar = px.bar(url_dist, x='count', y='url', orientation='h', color='count', color_continuous_scale="Blues", template="plotly_dark")
        fig_bar.update_layout(uirevision='bar_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', coloraxis_showscale=False, margin={"t":10})
        st.plotly_chart(fig_bar, width='stretch', key="bar")

    with c4:
        max_gauge_val = max(150, attack_ips + 50) 
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number", value=attack_ips, 
            gauge={
                'axis': {'range': [0, max_gauge_val]}, 'bar': {'color': "#ef4444"},
                'steps': [
                    {'range': [0, max_gauge_val * 0.3], 'color': "#064e3b"},
                    {'range': [max_gauge_val * 0.3, max_gauge_val * 0.7], 'color': "#b45309"},
                    {'range': [max_gauge_val * 0.7, max_gauge_val], 'color': "#7f1d1d"}
                ]
            }
        ))
        fig_gauge.update_layout(uirevision='gauge_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":20, "b":20, "l":20, "r":20}, font={'color': "white"})
        st.plotly_chart(fig_gauge, width='stretch', key="gauge")

if not is_paused:
    time.sleep(1)
    st.rerun()