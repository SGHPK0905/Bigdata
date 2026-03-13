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
    .chart-desc { font-size: 12px; color: #94a3b8; font-style: italic; margin-bottom: 10px; display: block;}
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

col_title, col_toggle = st.columns([3, 1])
with col_title: 
    st.markdown("<h3 style='color: #38bdf8;'>🛡️ TỔNG TÀI DOANH NGHIỆP: SIEM & BUSINESS INTELLIGENCE</h3>", unsafe_allow_html=True)
with col_toggle: 
    st.write("")
    is_paused = st.toggle("⏸️ TẠM DỪNG ĐỂ PHÂN TÍCH", value=False)

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

    tab_soc, tab_bi = st.tabs(["🚨 TRUNG TÂM SOC (AN NINH)", "📈 TRUNG TÂM BI (KINH DOANH & SALES)"])

    with tab_soc:
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(f'<div class="kpi-box"><div class="kpi-title">Tổng Requests</div><div class="kpi-value">{len(df)}</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="kpi-box alert-text"><div class="kpi-title">Cảnh báo Brute Force (401)</div><div class="kpi-value" style="color:#f87171;">{len(brute_ips)} IP</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="kpi-box warn-text"><div class="kpi-title">Phát hiện Rà quét (404)</div><div class="kpi-value" style="color:#f59e0b;">{len(scan_ips)} IP</div></div>', unsafe_allow_html=True)
        c4.markdown(f'<div class="kpi-box warn-text"><div class="kpi-title">Phát hiện Cào Data (Bot)</div><div class="kpi-value" style="color:#f59e0b;">{len(scrape_ips)} IP</div></div>', unsafe_allow_html=True)
        
        st.write("")
        map_col, table_col = st.columns([2, 1])
        
        with map_col:
            st.markdown("<h5 style='color: #e2e8f0;'>🗺️ BẢN ĐỒ CÁC MỐI ĐE DỌA</h5>", unsafe_allow_html=True)
            bad_ip_list = pd.concat([brute_ips, scan_ips, scrape_ips]).drop_duplicates(subset=['ip'])
            if not bad_ip_list.empty:
                bad_ip_list['lat'], bad_ip_list['lon'] = zip(*bad_ip_list['ip'].apply(get_lat_lon))
                fig_map = px.scatter_geo(bad_ip_list, lat='lat', lon='lon', size='count', color='count', color_continuous_scale="Reds", projection="natural earth")
                fig_map.update_layout(uirevision='map', margin={"r":0,"t":0,"l":0,"b":0}, geo=dict(bgcolor='#0b1121', showland=True, landcolor='#1e293b', showocean=True, oceancolor='#0f172a'), paper_bgcolor='#0b1121', plot_bgcolor='#0b1121', coloraxis_showscale=False)
                st.plotly_chart(fig_map, width='stretch', key="map")
            else: 
                st.info("Hệ thống an toàn.")

        with table_col:
            st.markdown("<h5 style='color: #e2e8f0;'>⚠️ CHI TIẾT KẺ TẤN CÔNG</h5>", unsafe_allow_html=True)
            if not brute_ips.empty:
                st.error(f"Thám sát: {len(brute_ips)} IP đang dò Pass")
                st.dataframe(brute_ips.rename(columns={'ip':'IP Hacker', 'count':'Lần thử'}), width='stretch', height=120)
            if not scan_ips.empty:
                st.warning(f"Thám sát: {len(scan_ips)} IP đang tìm Lỗ hổng (404)")
                st.dataframe(scan_ips.rename(columns={'ip':'IP Scanner', 'count':'Số lỗi 404'}), width='stretch', height=120)

    with tab_bi:
        st.markdown("<span class='chart-desc'>Giao diện dành riêng cho Giám đốc Kinh doanh và Marketing để theo dõi hành vi mua hàng.</span>", unsafe_allow_html=True)
        
        sale_df = df[df['status'] == 200]
        
        b1, b2, b3, b4 = st.columns(4)
        b1.markdown(f'<div class="kpi-box success-text"><div class="kpi-title">Lượt xem Sản phẩm</div><div class="kpi-value" style="color:#10b981;">{len(sale_df[sale_df["url"]=="/products"])}</div></div>', unsafe_allow_html=True)
        b2.markdown(f'<div class="kpi-box success-text"><div class="kpi-title">Thêm Giỏ hàng (/cart)</div><div class="kpi-value" style="color:#10b981;">{len(sale_df[sale_df["url"]=="/cart"])}</div></div>', unsafe_allow_html=True)
        b3.markdown(f'<div class="kpi-box success-text"><div class="kpi-title">Đơn hàng Thành công</div><div class="kpi-value" style="color:#10b981;">{len(sale_df[sale_df["url"]=="/checkout"])}</div></div>', unsafe_allow_html=True)
        
        visits = len(sale_df[sale_df["url"]=="/home"])
        sales = len(sale_df[sale_df["url"]=="/checkout"])
        conv_rate = round((sales/visits)*100, 2) if visits > 0 else 0
        b4.markdown(f'<div class="kpi-box"><div class="kpi-title">Tỷ lệ Chuyển đổi (CVR)</div><div class="kpi-value">{conv_rate}%</div></div>', unsafe_allow_html=True)

        st.write("")
        chart_bi1, chart_bi2 = st.columns([2, 1])
        
        with chart_bi1:
            st.markdown("<h5 style='color: #e2e8f0;'>📉 PHỄU MUA HÀNG (Sales Funnel)</h5>", unsafe_allow_html=True)
            funnel_urls = ['/home', '/products', '/cart', '/checkout']
            funnel_data = sale_df[sale_df['url'].isin(funnel_urls)].groupby('url').size().reindex(funnel_urls).reset_index(name='count')
            funnel_data['url'] = ['Trang chủ', 'Xem Sản Phẩm', 'Vào Giỏ Hàng', 'Thanh Toán'] 
            
            fig_funnel = px.funnel(funnel_data, x='count', y='url', template="plotly_dark", color_discrete_sequence=['#10b981'])
            fig_funnel.update_layout(uirevision='funnel_lock', paper_bgcolor='#0b1121', plot_bgcolor='#0b1121')
            st.plotly_chart(fig_funnel, width='stretch')

        with chart_bi2:
            st.markdown("<h5 style='color: #e2e8f0;'>🍩 HÀNH VI BOT CÀO DỮ LIỆU</h5>", unsafe_allow_html=True)
            st.markdown("<span class='chart-desc'>Những IP có tốc độ tải trang quá nhanh (bị tình nghi là đối thủ đang copy giá sản phẩm).</span>", unsafe_allow_html=True)
            if not scrape_ips.empty:
                st.dataframe(scrape_ips.rename(columns={'ip':'IP Đối thủ', 'count':'Số trang bị copy'}), width='stretch', height=250)
            else:
                st.success("Tài sản Dữ liệu an toàn.")

if not is_paused:
    time.sleep(1)
    st.rerun()