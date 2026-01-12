import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="高校野球DB完全版", layout="wide", page_icon="⚾")
st.title("⚾ 高校野球 全記録データベース")

# CSS設定
st.markdown("""
<style>
    .stDataFrame {font-size: 0.95rem;}
    h3 {border-bottom: 2px solid #ddd; padding-bottom: 0.5rem; margin-top: 2rem;}
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_bq_client():
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

with st.sidebar:
    st.header("📂 メニュー")
    mode = st.radio("検索モード", ["🏆 大会から探す", "👤 選手から探す", "🏫 高校から探す"])

# ==========================================
# 🏆 モード: 大会記録 (Tournament Search)
# ==========================================
if mode == "🏆 大会から探す":
    st.subheader("🏆 大会記録・出場校チェック")
    
    df_years = client.query(f"SELECT DISTINCT Year FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` ORDER BY Year DESC").to_dataframe()
    
    col1, col2 = st.columns(2)
    with col1: sel_year = st.selectbox("年度", df_years['Year'].tolist())
    with col2: sel_season = st.selectbox("季節", ["夏", "春"])
    
    if sel_year and sel_season:
        t_info = client.query(f"SELECT Tournament, Champion FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` WHERE Year = '{sel_year}' AND Season = '{sel_season}'").to_dataframe()
        
        if not t_info.empty:
            st.info(f"🚩 **{t_info.iloc[0]['Tournament']}** （優勝：{t_info.iloc[0].get('Champion', '不明')}）")
            st.write("👇 **詳細を見たい高校の行をクリックしてください**")
            
            # 【重要】列指定エラーを防ぐため SELECT * を使用
            df_res = client.query(f"""
                SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                WHERE Year = '{sel_year}' AND Season = '{sel_season}'
                ORDER BY School_ID ASC
            """).to_dataframe()
            
            # 列名のゆらぎ対応（万が一 Game_Scores がない場合など）
            if 'Game_Scores' not in df_res.columns:
                df_res['Game_Scores'] = "-"
            if 'History_Label' not in df_res.columns:
                df_res['History_Label'] = "-"

            # 表示用
            display_df = df_res[['School', 'History_Label', 'Result', 'Game_Scores']].rename(columns={
                'School': '高校名', 'History_Label': '出場情報', 'Result': '成績', 'Game_Scores': '試合結果'
            })

            selection = st.dataframe(display_df, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
            
            if len(selection.selection.rows) > 0:
                row_idx = selection.selection.rows[0]
                row_data = df_res.iloc[row_idx]
                target_sid = row_data['School_ID']
                
                st.divider()
                st.markdown(f"## 🏫 **{row_data['School']}**")
                st.info(row_data['History_Label'])
                
                tab1, tab2 = st.tabs(["🦁 当時のメンバー", "📜 大会履歴"])
                
                with tab1:
                    # メンバーも SELECT * で安全に取得
                    m_query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'"
                    df_mem = client.query(m_query).to_dataframe()
                    
                    if not df_mem.empty:
                        # 存在する列だけリネームして表示
                        rename_cols = {'Name':'氏名','Grade':'学年','Uniform_Number':'背番号','Position':'守備','Throw_Bat':'投打','Captain':'役職'}
                        valid_cols = {k:v for k,v in rename_cols.items() if k in df_mem.columns}
                        
                        if 'Uniform_Number' in df_mem.columns:
                            df_mem = df_mem.sort_values('Uniform_Number', key=lambda x: pd.to_numeric(x, errors='coerce'))
                        if 'Captain' in df_mem.columns:
                            df_mem['Captain'] = df_mem['Captain'].apply(lambda x: "★主将" if "◎" in str(x) else "")
                            
                        st.dataframe(df_mem[valid_cols.keys()].rename(columns=valid_cols), use_container_width=True, hide_index=True)
                    else:
                        st.warning("メンバーデータなし")
                
                with tab2:
                    h_query = f"""
                        SELECT Year, Season, Result, Game_Scores, History_Label
                        FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                        WHERE School_ID = '{target_sid}' AND (Year < {sel_year} OR (Year = {sel_year} AND Season != '{sel_season}'))
                        ORDER BY Year DESC, Season DESC
                        LIMIT 10
                    """
                    try:
                        df_hist = client.query(h_query).to_dataframe()
                        st.dataframe(df_hist.rename(columns={'Year':'年度','Season':'季','Result':'成績','Game_Scores':'詳細','History_Label':'当時'}), use_container_width=True, hide_index=True)
                    except:
                        st.info("履歴データの取得に失敗しました")

# ==========================================
# 👤 モード: 選手検索
# ==========================================
elif mode == "👤 選手から探す":
    st.subheader("👤 選手検索")
    name_in = st.text_input("選手名")
    gen_in = st.number_input("世代", value=None, step=1)
    if name_in or gen_in:
        where = []
        if name_in: where.append(f"c.Name LIKE '%{name_in}%'")
        if gen_in: where.append(f"c.Generation = '{int(gen_in)}'")
        
        q = f"""
            SELECT c.*, m.Hometown, m.Pro_Team 
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` c 
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` m ON c.Player_ID = m.Player_ID 
            WHERE {' AND '.join(where)} ORDER BY c.Year ASC
        """
        try:
            df = client.query(q).to_dataframe()
            if not df.empty:
                df['lbl'] = df['Name'] + " (" + df['School'] + ")"
                sel = st.selectbox("選択", df['lbl'].unique())
                if sel:
                    p = df[df['lbl']==sel].iloc[0]
                    p_all = df[df['lbl']==sel]
                    st.markdown(f"## {p['Name']} ({p['School']})")
                    if pd.notna(p.get('Pro_Team')): st.success(f"🚀 {p['Pro_Team']}")
                    
                    # 存在する列だけ表示
                    cols = {'Year':'年度','Season':'季','Grade':'学年','Result':'成績','Game_Scores':'詳細'}
                    valid_cols = {k:v for k,v in cols.items() if k in p_all.columns}
                    st.dataframe(p_all[valid_cols.keys()].rename(columns=valid_cols), use_container_width=True, hide_index=True)
            else:
                st.warning("見つかりませんでした")
        except Exception as e:
            st.error(f"Error: {e}")

# ==========================================
# 🏫 モード: 高校検索
# ==========================================
elif mode == "🏫 高校から探す":
    st.subheader("🏫 高校検索")
    s_in = st.text_input("高校名")
    if s_in:
        df_s = client.query(f"SELECT DISTINCT School_ID, Latest_School_Name FROM `{PROJECT_ID}.{DATASET_ID}.DB_高校マスタ` WHERE School LIKE '%{s_in}%' LIMIT 20").to_dataframe()
        if not df_s.empty:
            sel = st.selectbox("選択", df_s['Latest_School_Name'].unique())
            if sel:
                sid = df_s[df_s['Latest_School_Name']==sel].iloc[0]['School_ID']
                df_h = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ` WHERE School_ID = '{sid}' ORDER BY Year DESC, Season DESC").to_dataframe()
                
                cols = {'Year':'年度','Season':'季','Result':'成績','Game_Scores':'詳細','History_Label':'情報'}
                valid_cols = {k:v for k,v in cols.items() if k in df_h.columns}
                st.dataframe(df_h[valid_cols.keys()].rename(columns=valid_cols), use_container_width=True, hide_index=True)
