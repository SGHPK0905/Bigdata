import streamlit as st
import pandas as pd
import glob
import time
import os
import plotly.express as px
import plotly.graph_objects as go
import hashlib

st.set_page_config(page_title="SOC XDR Dashboard", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #0b1121; color: #e2e8f0; }
    .kpi-box { background-color: #162032; border-left: 4px solid #0ea5e9; padding: 15px; border-radius: 5px; text-align: center; }
    .kpi-title { font-size: 14px; color: #94a3b8; }
    .kpi-value { font-size: 32px; font-weight: bold; color: #38bdf8; margin: 5px 0;}
    .alert-text { border-left-color: #ef4444; }
    .chart-desc { font-size: 12px; color: #94a3b8; font-style: italic; margin-bottom: 10px; display: block;}
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

if not df.empty:
    st.markdown("<h3 style='color: #38bdf8;'>🛡️ SIEM & BIG DATA ANALYTICS DASHBOARD</h3>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.markdown(f'<div class="kpi-box"><div class="kpi-title">Tổng Lưu lượng (Requests)</div><div class="kpi-value">{len(df)}</div></div>', unsafe_allow_html=True)
    col2.markdown(f'<div class="kpi-box alert-text"><div class="kpi-title">Truy cập Thất bại (Lỗi 401)</div><div class="kpi-value" style="color:#f87171;">{len(df[df["status"] == 401])}</div></div>', unsafe_allow_html=True)
    col3.markdown(f'<div class="kpi-box alert-text"><div class="kpi-title">Hacker bị Đưa vào Sổ đen</div><div class="kpi-value" style="color:#ef4444;">{attack_ips}</div></div>', unsafe_allow_html=True)
    col4.markdown(f'<div class="kpi-box"><div class="kpi-title">Cập nhật lúc</div><div class="kpi-value" style="font-size:24px;">{time.strftime("%H:%M:%S")}</div></div>', unsafe_allow_html=True)
    
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
        st.markdown("<h5 style='color: #e2e8f0;'>🗺️ BẢN ĐỒ MỤC TIÊU (Live Threat Map)</h5>", unsafe_allow_html=True)
        st.markdown("<span class='chart-desc'>Hiển thị vị trí quy đổi của các địa chỉ IP đang thực hiện tấn công Brute Force.</span>", unsafe_allow_html=True)
        if not st.session_state.blacklisted_ips.empty:
            map_data = st.session_state.blacklisted_ips.copy()
            map_data['lat'], map_data['lon'] = zip(*map_data['ip'].apply(get_lat_lon))
            fig_map = px.scatter_geo(map_data, lat='lat', lon='lon', size='count', color='count', color_continuous_scale="Reds", projection="natural earth", size_max=25)
            fig_map.update_layout(uirevision='map', margin={"r":0,"t":0,"l":0,"b":0}, geo=dict(bgcolor='#0b1121', showland=True, landcolor='#1e293b', showocean=True, oceancolor='#0f172a', showcountries=True, countrycolor='#334155'), paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', coloraxis_showscale=False)
            st.plotly_chart(fig_map, width='stretch', key="map")
        else:
            st.info("Hệ thống an toàn. Chưa phát hiện IP độc hại.")

    with table_col:
        st.markdown("<h5 style='color: #e2e8f0;'>🚨 DANH SÁCH ĐEN (Blacklist)</h5>", unsafe_allow_html=True)
        st.markdown("<span class='chart-desc'>Top 100 IP nguy hiểm nhất đã bị Spark phát hiện và chặn lại.</span>", unsafe_allow_html=True)
        if not st.session_state.blacklisted_ips.empty:
            display_df = st.session_state.blacklisted_ips.sort_values(by='count', ascending=False).head(100)
            display_df.columns = ['IP Kẻ tấn công', 'Số lần thử sai mật khẩu']
            display_df.index = range(1, len(display_df) + 1)
            st.dataframe(display_df, width='stretch', height=350)

    st.markdown("---")

    chart_col1, chart_col2 = st.columns([2, 1])
    with chart_col1:
        line_header, line_toggle = st.columns([2, 1])
        line_header.markdown("<h5 style='color: #e2e8f0;'>📈 DIỄN BIẾN LƯU LƯỢNG (Time Series)</h5>", unsafe_allow_html=True)
        freeze_chart = line_toggle.toggle("🔍 Khóa biểu đồ này để Zoom/Kéo", value=False)
        st.markdown("<span class='chart-desc'>Theo dõi các đợt tăng vọt (Spikes) để phát hiện tấn công từ chối dịch vụ (DDoS).</span>", unsafe_allow_html=True)
        
        if 'time' in df.columns:
            time_df = df.dropna(subset=['time']).copy()
            time_df['time_sec'] = time_df['time'].dt.floor('s') 
            
            latest_time = time_df['time_sec'].max()
            start_time = latest_time - pd.Timedelta(seconds=60)
            recent_df = time_df[time_df['time_sec'] >= start_time]
            trend = recent_df.groupby('time_sec').size().reset_index(name='Requests')
            
            if freeze_chart:
                if 'frozen_trend' not in st.session_state:
                    st.session_state.frozen_trend = trend.copy()
                plot_data = st.session_state.frozen_trend
            else:
                if 'frozen_trend' in st.session_state:
                    del st.session_state['frozen_trend']
                plot_data = trend

            fig_line = px.area(plot_data, x='time_sec', y='Requests', template="plotly_dark")
            
            x_range = None if freeze_chart else [start_time, latest_time]
            fig_line.update_layout(
                xaxis=dict(range=x_range, fixedrange=not freeze_chart), 
                yaxis=dict(fixedrange=not freeze_chart),
                paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":10}
            )
            fig_line.update_traces(mode='lines+markers', marker=dict(size=4), fillcolor='rgba(14, 165, 233, 0.2)')
            st.plotly_chart(fig_line, width='stretch', key="line", config={'scrollZoom': True, 'displayModeBar': True})

    with chart_col2:
        st.markdown("<h5 style='color: #e2e8f0;'>🍩 PHÂN BỔ MÃ TRẠNG THÁI HTTP</h5>", unsafe_allow_html=True)
        st.markdown("<span class='chart-desc'>Tỷ lệ màu Đỏ (401) phình to chứng tỏ hệ thống đang bị rò rỉ hoặc dò quét mật khẩu.</span>", unsafe_allow_html=True)
        
        status_dist = df.groupby('status').size().reset_index(name='count')
        status_dist['status'] = status_dist['status'].astype(str)
        
        status_map = {
            '200': '200 (Truy cập hợp lệ)', 
            '401': '401 (Báo động: Sai Pass)', 
            '404': '404 (Không tìm thấy trang)', 
            '500': '500 (Máy chủ quá tải)'
        }
        status_dist['Tên Trạng Thái'] = status_dist['status'].map(status_map).fillna(status_dist['status'])
        
        color_discrete_map = {
            '200 (Truy cập hợp lệ)': '#10b981', 
            '401 (Báo động: Sai Pass)': '#ef4444', 
            '404 (Không tìm thấy trang)': '#f59e0b', 
            '500 (Máy chủ quá tải)': '#f97316'
        }
        
        fig_pie = px.pie(status_dist, values='count', names='Tên Trạng Thái', color='Tên Trạng Thái', color_discrete_map=color_discrete_map, hole=0.5, template="plotly_dark")
        fig_pie.update_layout(uirevision='pie_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":10, "b":10}, legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5))
        st.plotly_chart(fig_pie, width='stretch', key="pie")

    c3, c4 = st.columns([2, 1])
    with c3:
        st.markdown("<h5 style='color: #e2e8f0;'>🎯 TOP ĐÍCH ĐẾN BỊ NHẮM MỤC TIÊU</h5>", unsafe_allow_html=True)
        st.markdown("<span class='chart-desc'>Thống kê các đường dẫn (URL) bị truy cập nhiều nhất. Đường dẫn /login cao bất thường là dấu hiệu bị Hacker nhắm tới.</span>", unsafe_allow_html=True)
        
        url_dist = df.groupby('url').size().reset_index(name='count').sort_values(by='count')
        fig_bar = px.bar(url_dist, x='count', y='url', orientation='h', color='count', color_continuous_scale="Blues", template="plotly_dark")
        fig_bar.update_layout(uirevision='bar_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', coloraxis_showscale=False, margin={"t":10}, xaxis_title="Số lượng truy cập", yaxis_title="")
        st.plotly_chart(fig_bar, width='stretch', key="bar")

    with c4:
        st.markdown("<h5 style='color: #e2e8f0;'>⏱️ MỨC ĐỘ ĐE DỌA TOÀN CỤC</h5>", unsafe_allow_html=True)
        st.markdown("<span class='chart-desc'>Kim chỉ màu Đỏ báo hiệu hệ thống đang chịu rủi ro cao dựa trên số lượng IP xấu.</span>", unsafe_allow_html=True)
        
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
        fig_gauge.update_layout(uirevision='gauge_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":10, "b":10, "l":20, "r":20}, font={'color': "white"})
        st.plotly_chart(fig_gauge, width='stretch', key="gauge")

time.sleep(1)
st.rerun()