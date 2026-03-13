import streamlit as st
import pandas as pd
import glob
import time
import os
import plotly.express as px
import plotly.graph_objects as go
import hashlib

st.set_page_config(page_title="Enterprise SIEM & BI Dashboard", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #0b1121; color: #e2e8f0; }
    .kpi-box { background-color: #162032; border-left: 4px solid #0ea5e9; padding: 15px; border-radius: 5px; text-align: center; }
    .kpi-title { font-size: 14px; color: #94a3b8; }
    .kpi-value { font-size: 28px; font-weight: bold; color: #38bdf8; margin: 5px 0;}
    .alert-text { border-left-color: #ef4444; }
    .warn-text { border-left-color: #f59e0b; }
    .success-text { border-left-color: #10b981; }
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { background-color: #1e293b; border-radius: 4px 4px 0 0; padding: 10px 20px; color: #94a3b8; }
    .stTabs [aria-selected="true"] { background-color: #38bdf8 !important; color: #0b1121 !important; font-weight: bold;}
</style>
""", unsafe_allow_html=True)

if 'last_valid_df' not in st.session_state: 
    st.session_state.last_valid_df = pd.DataFrame()

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

st.markdown("<h3 style='color: #38bdf8;'>🛡️ TỔNG TÀI DOANH NGHIỆP: SIEM & BUSINESS INTELLIGENCE</h3>", unsafe_allow_html=True)

if not df.empty:
    brute_df = df[(df['status'] == 401)]
    brute_ips = brute_df.groupby('ip').size().reset_index(name='count')
    brute_ips = brute_ips[brute_ips['count'] > 10]

    scan_df = df[(df['status'] == 404)]
    scan_ips = scan_df.groupby('ip').size().reset_index(name='count')
    scan_ips = scan_ips[scan_ips['count'] > 10]

    scrape_df = df[(df['status'] == 200)]
    scrape_ips = scrape_df.groupby('ip').size().reset_index(name='count')
    scrape_ips = scrape_ips[scrape_ips['count'] > 30]

    total_bad_ips = len(pd.concat([brute_ips['ip'], scan_ips['ip'], scrape_ips['ip']]).unique())

    tab_soc, tab_bi = st.tabs(["🚨 TRUNG TÂM SOC (AN NINH)", "📈 TRUNG TÂM BI (THUẦN KINH DOANH)"])

    with tab_soc:
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(f'<div class="kpi-box"><div class="kpi-title">Tổng Requests</div><div class="kpi-value">{len(df)}</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="kpi-box alert-text"><div class="kpi-title">Lỗi 401 (Brute Force)</div><div class="kpi-value" style="color:#f87171;">{len(brute_ips)} IP</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="kpi-box warn-text"><div class="kpi-title">Lỗi 404 (Scanner)</div><div class="kpi-value" style="color:#f59e0b;">{len(scan_ips)} IP</div></div>', unsafe_allow_html=True)
        c4.markdown(f'<div class="kpi-box alert-text"><div class="kpi-title">Tổng IP Sổ Đen</div><div class="kpi-value" style="color:#ef4444;">{total_bad_ips}</div></div>', unsafe_allow_html=True)
        
        st.write("")
        soc_t1, soc_t2, soc_t3 = st.tabs(["🌍 TỔNG QUAN TÌNH HÌNH", "🔥 TẤN CÔNG XÂM NHẬP (401/404)", "🤖 BOT CÀO DỮ LIỆU (200)"])

        with soc_t1:
            map_col, gauge_col = st.columns([2, 1])
            with map_col:
                st.markdown("<h5 style='color: #e2e8f0;'>🗺️ BẢN ĐỒ CÁC MỐI ĐE DỌA</h5>", unsafe_allow_html=True)
                bad_ip_list = pd.concat([brute_ips, scan_ips, scrape_ips]).drop_duplicates(subset=['ip'])
                if not bad_ip_list.empty:
                    bad_ip_list['lat'], bad_ip_list['lon'] = zip(*bad_ip_list['ip'].apply(get_lat_lon))
                    fig_map = px.scatter_geo(bad_ip_list, lat='lat', lon='lon', size='count', color='count', color_continuous_scale="Reds", projection="natural earth")
                    fig_map.update_layout(uirevision='map', margin={"r":0,"t":0,"l":0,"b":0}, geo=dict(bgcolor='#0b1121', showland=True, landcolor='#1e293b', showocean=True, oceancolor='#0f172a', showcountries=True, countrycolor='#334155'), paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', coloraxis_showscale=False)
                    st.plotly_chart(fig_map, width='stretch', key="map")
                else: 
                    st.info("Hệ thống an toàn.")
            
            with gauge_col:
                st.markdown("<h5 style='color: #e2e8f0;'>⏱️ MỨC ĐỘ ĐE DỌA</h5>", unsafe_allow_html=True)
                max_gauge_val = max(150, total_bad_ips + 50) 
                fig_gauge = go.Figure(go.Indicator(
                    mode="gauge+number", value=total_bad_ips, 
                    gauge={'axis': {'range': [0, max_gauge_val]}, 'bar': {'color': "#ef4444"},
                        'steps': [{'range': [0, max_gauge_val * 0.3], 'color': "#064e3b"},
                                  {'range': [max_gauge_val * 0.3, max_gauge_val * 0.7], 'color': "#b45309"},
                                  {'range': [max_gauge_val * 0.7, max_gauge_val], 'color': "#7f1d1d"}]}
                ))
                fig_gauge.update_layout(uirevision='gauge_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":10, "b":10, "l":20, "r":20}, font={'color': "white"})
                st.plotly_chart(fig_gauge, width='stretch', key="gauge")

            st.markdown("---")
            chart_col1, chart_col2 = st.columns([2, 1])
            with chart_col1:
                line_header, line_toggle = st.columns([2, 1])
                line_header.markdown("<h5 style='color: #e2e8f0;'>📈 DIỄN BIẾN LƯU LƯỢNG (60s)</h5>", unsafe_allow_html=True)
                freeze_chart = line_toggle.toggle("🔍 Khóa Line Chart", value=False)
                
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
                            st.session_state.frozen_start = start_time
                            st.session_state.frozen_latest = latest_time
                        plot_data = st.session_state.frozen_trend
                        plot_start = st.session_state.frozen_start
                        plot_latest = st.session_state.frozen_latest
                    else:
                        if 'frozen_trend' in st.session_state:
                            del st.session_state['frozen_trend']
                            del st.session_state['frozen_start']
                            del st.session_state['frozen_latest']
                        plot_data = trend
                        plot_start = start_time
                        plot_latest = latest_time

                    fig_line = px.area(plot_data, x='time_sec', y='Requests', template="plotly_dark")
                    fig_line.update_layout(xaxis=dict(range=[plot_start, plot_latest], fixedrange=not freeze_chart), yaxis=dict(fixedrange=not freeze_chart), paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":10})
                    fig_line.update_traces(mode='lines+markers', marker=dict(size=4), fillcolor='rgba(14, 165, 233, 0.2)')
                    st.plotly_chart(fig_line, width='stretch', key="line", config={'scrollZoom': True, 'displayModeBar': True})

            with chart_col2:
                st.markdown("<h5 style='color: #e2e8f0;'>🍩 MÃ TRẠNG THÁI HTTP</h5>", unsafe_allow_html=True)
                status_dist = df.groupby('status').size().reset_index(name='count')
                status_dist['status'] = status_dist['status'].astype(str)
                status_map = {'200': '200 (Hợp lệ)', '401': '401 (Sai Pass)', '404': '404 (Không thấy)', '500': '500 (Quá tải)'}
                status_dist['Tên'] = status_dist['status'].map(status_map).fillna(status_dist['status'])
                color_map = {'200 (Hợp lệ)': '#10b981', '401 (Sai Pass)': '#ef4444', '404 (Không thấy)': '#f59e0b', '500 (Quá tải)': '#f97316'}
                
                fig_pie = px.pie(status_dist, values='count', names='Tên', color='Tên', color_discrete_map=color_map, hole=0.5, template="plotly_dark")
                fig_pie.update_layout(uirevision='pie_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":10, "b":10}, legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5))
                st.plotly_chart(fig_pie, width='stretch', key="pie")

        with soc_t2:
            st.markdown("<h5 style='color: #e2e8f0;'>⚠️ DANH SÁCH KẺ TẤN CÔNG XÂM NHẬP HỆ THỐNG</h5>", unsafe_allow_html=True)
            tbl1, tbl2 = st.columns(2)
            with tbl1:
                if not brute_ips.empty:
                    brute_display = brute_ips.sort_values(by='count', ascending=False).head(100).rename(columns={'ip':'IP Dò Pass', 'count':'Lần thử'})
                    brute_display.index = range(1, len(brute_display) + 1)
                    st.dataframe(brute_display, width='stretch', height=250)
                else: st.info("Không phát hiện Brute Force.")
            with tbl2:
                if not scan_ips.empty:
                    scan_display = scan_ips.sort_values(by='count', ascending=False).head(100).rename(columns={'ip':'IP Quét Lỗi', 'count':'Lỗi 404'})
                    scan_display.index = range(1, len(scan_display) + 1)
                    st.dataframe(scan_display, width='stretch', height=250)
                else: st.info("Không phát hiện Rà quét lỗ hổng.")
            
            st.markdown("<h5 style='color: #e2e8f0; margin-top:20px;'>🎯 TOP ĐƯỜNG DẪN BỊ NHẮM TỚI (CỦA HACKER)</h5>", unsafe_allow_html=True)
            hacker_target_df = df[df['status'].isin([401, 404])]
            if not hacker_target_df.empty:
                url_dist = hacker_target_df.groupby('url').size().reset_index(name='count').sort_values(by='count')
                fig_bar = px.bar(url_dist, x='count', y='url', orientation='h', color='count', color_continuous_scale="Reds", template="plotly_dark")
                fig_bar.update_layout(uirevision='bar_lock_soc', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', coloraxis_showscale=False, margin={"t":10}, xaxis_title="", yaxis_title="")
                st.plotly_chart(fig_bar, width='stretch', key="bar_soc")

        with soc_t3:
            st.markdown("<h5 style='color: #e2e8f0;'>🤖 DANH SÁCH BOT CÀO DỮ LIỆU ĐỐI THỦ</h5>", unsafe_allow_html=True)
            col_bot1, col_bot2 = st.columns([1, 2])
            with col_bot1:
                if not scrape_ips.empty:
                    scrape_display = scrape_ips.sort_values(by='count', ascending=False).head(100).rename(columns={'ip':'IP Cào Data', 'count':'Trang bị copy'})
                    scrape_display.index = range(1, len(scrape_display) + 1)
                    st.dataframe(scrape_display, width='stretch', height=250)
                else: st.success("Không có dấu hiệu cào dữ liệu.")
            with col_bot2:
                if not scrape_ips.empty:
                    scrape_target_df = df[(df['ip'].isin(scrape_ips['ip'])) & (df['status'] == 200)]
                    scrape_url_dist = scrape_target_df.groupby('url').size().reset_index(name='count').sort_values(by='count')
                    fig_bot_bar = px.bar(scrape_url_dist, x='count', y='url', orientation='h', color='count', color_continuous_scale="Oranges", template="plotly_dark", title="Các trang bị Bot thu thập nhiều nhất")
                    fig_bot_bar.update_layout(uirevision='bar_lock_bot', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', coloraxis_showscale=False)
                    st.plotly_chart(fig_bot_bar, width='stretch', key="bar_bot")

    with tab_bi:
        bad_ips_set = set(pd.concat([brute_ips['ip'], scan_ips['ip'], scrape_ips['ip']]))
        real_user_df = df[(df['status'] == 200) & (~df['ip'].isin(bad_ips_set))]
        
        b1, b2, b3, b4 = st.columns(4)
        b1.markdown(f'<div class="kpi-box success-text"><div class="kpi-title">Khách hàng Xem SP</div><div class="kpi-value" style="color:#10b981;">{len(real_user_df[real_user_df["url"]=="/products"])}</div></div>', unsafe_allow_html=True)
        b2.markdown(f'<div class="kpi-box success-text"><div class="kpi-title">Khách thêm Giỏ hàng</div><div class="kpi-value" style="color:#10b981;">{len(real_user_df[real_user_df["url"]=="/cart"])}</div></div>', unsafe_allow_html=True)
        b3.markdown(f'<div class="kpi-box success-text"><div class="kpi-title">Khách chốt Đơn</div><div class="kpi-value" style="color:#10b981;">{len(real_user_df[real_user_df["url"]=="/checkout"])}</div></div>', unsafe_allow_html=True)
        
        visits = len(real_user_df[real_user_df["url"]=="/home"])
        sales = len(real_user_df[real_user_df["url"]=="/checkout"])
        conv_rate = round((sales/visits)*100, 2) if visits > 0 else 0
        b4.markdown(f'<div class="kpi-box"><div class="kpi-title">Tỷ lệ Chuyển đổi (CVR)</div><div class="kpi-value">{conv_rate}%</div></div>', unsafe_allow_html=True)

        st.write("")
        chart_bi1, chart_bi2 = st.columns([2, 1])
        
        with chart_bi1:
            st.markdown("<h5 style='color: #e2e8f0;'>📉 PHỄU CHUYỂN ĐỔI (Đã lọc rác từ Bot)</h5>", unsafe_allow_html=True)
            funnel_urls = ['/home', '/products', '/cart', '/checkout']
            funnel_data = real_user_df[real_user_df['url'].isin(funnel_urls)].groupby('url').size().reindex(funnel_urls).reset_index(name='count')
            funnel_data['url'] = ['Trang chủ', 'Xem Sản Phẩm', 'Vào Giỏ Hàng', 'Thanh Toán'] 
            
            fig_funnel = px.funnel(funnel_data, x='count', y='url', template="plotly_dark", color_discrete_sequence=['#10b981'])
            fig_funnel.update_layout(uirevision='funnel_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121')
            st.plotly_chart(fig_funnel, width='stretch')
            
        with chart_bi2:
            st.markdown("<h5 style='color: #e2e8f0;'>💡 TỶ LỆ TRUY CẬP CÁC TRANG</h5>", unsafe_allow_html=True)
            if not real_user_df.empty:
                page_dist = real_user_df.groupby('url').size().reset_index(name='count')
                fig_bi_pie = px.pie(page_dist, values='count', names='url', hole=0.4, template="plotly_dark", color_discrete_sequence=px.colors.sequential.Teal)
                fig_bi_pie.update_layout(uirevision='bi_pie_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', margin={"t":10, "b":10})
                st.plotly_chart(fig_bi_pie, width='stretch', key="bi_pie")

time.sleep(1)
st.rerun()