import streamlit as st
from google.cloud import bigquery
import pandas as pd

# --- 1. アプリ設定 ---
st.set_page_config(page_title="高校野球DB完全版", layout="wide", page_icon="⚾")

# スタイル調整：ちらつき抑制と可読性向上
st.markdown("""
<style>
    .stDataFrame {font-size: 0.95rem;}
    h3 {border-bottom: 2px solid #ddd; padding-bottom: 0.5rem; margin-top: 2rem;}
    .stSpinner {text-align: center; margin: 20px;}
</style>
""", unsafe_allow_html=True)

st.title("⚾ 高校野球 全記録データベース")

PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])
        return bigquery.Client()
    except:
        return bigquery.Client(project=PROJECT_ID)

@st.cache_data(ttl=3600)
def run_query(query_string):
    client = get_bq_client()
    return client.query(query_string).to_dataframe()

# --- 2. サイドバー ---
with st.sidebar:
    st.header("📂 メニュー")
    mode = st.radio("検索モード", ["🏆 大会から探す", "👤 選手から探す", "🏫 高校から探す"])
    
    st.divider()
    if st.button("🔄 データを最新に更新"):
        st.cache_data.clear()
        st.rerun()

# ==========================================
# 🏆 モード: 大会記録
# ==========================================
if mode == "🏆 大会から探す":
    st.subheader("🏆 大会記録・出場校チェック")
    
    # 年度リスト取得
    df_years = run_query(f"SELECT DISTINCT Year FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` ORDER BY Year DESC")
    years_list = df_years['Year'].tolist() if not df_years.empty else []

    if years_list:
        col1, col2 = st.columns(2)
        with col1: sel_year = st.selectbox("年度", years_list)
        with col2: sel_season = st.selectbox("季節", ["夏", "春"])
        
        with st.spinner('大会データを読み込み中...'):
            # 大会基本情報
            t_info = run_query(f"SELECT Tournament, Champion FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` WHERE Year = '{sel_year}' AND Season = '{sel_season}'")
            
            if not t_info.empty:
                st.success(f"🚩 **{t_info.iloc[0]['Tournament']}** （優勝：{t_info.iloc[0]['Champion']}）")
                
                # 出場校一覧（修復済みの School カラムを使用）
                df_res = run_query(f"""
                    SELECT School as `高校名`, Rank as `成績`, History_Label as `出場情報`, School_ID
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                    WHERE Year = '{sel_year}' AND Season = '{sel_season}'
                    ORDER BY School_ID ASC
                """)
                
                if not df_res.empty:
                    st.write(f"👇 **出場 {len(df_res)} 校** （クリックで詳細表示）")
                    selection = st.dataframe(
                        df_res.drop(columns=['School_ID']),
                        use_container_width=True,
                        hide_index=True,
                        on_select="rerun",
                        selection_mode="single-row"
                    )
                    
                    # 詳細表示（戦績・メンバー）
                    if len(selection.selection.rows) > 0:
                        row_idx = selection.selection.rows[0]
                        target_sid = df_res.iloc[row_idx]['School_ID']
                        target_school = df_res.iloc[row_idx]['高校名']
                        
                        st.divider()
                        st.markdown(f"### 🏫 **{target_school}** の詳細")
                        
                        tab1, tab2 = st.tabs(["⚾ 今大会の戦績", "🦁 当時のメンバー"])
                        with tab1:
                            df_games = run_query(f"SELECT Round, Opponent, Score, Win_Loss, Game_Scores FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ` WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}' ORDER BY Round ASC")
                            st.dataframe(df_games.rename(columns={'Round':'回戦','Opponent':'対戦校','Score':'スコア','Win_Loss':'勝敗','Game_Scores':'詳細'}), use_container_width=True, hide_index=True)
                        with tab2:
                            df_mem = run_query(f"SELECT Name, Grade, Uniform_Number, Position FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手データ完全版` WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}' ORDER BY SAFE_CAST(Uniform_Number AS INT64)")
                            st.dataframe(df_mem.rename(columns={'Name':'氏名','Grade':'学年','Uniform_Number':'背番号','Position':'守備'}), use_container_width=True, hide_index=True)
            else:
                st.info("該当する大会データがありません。")

# ==========================================
# 🏫 モード: 高校検索
# ==========================================
elif mode == "🏫 高校から探す":
    st.subheader("🏫 高校検索")
    s_in = st.text_input("高校名を入力してください", placeholder="例: 光星")
    
    if s_in:
        with st.spinner('高校マスタを検索中...'):
            df_s = run_query(f"SELECT DISTINCT School_ID, Latest_School_Name, Prefecture FROM `{PROJECT_ID}.{DATASET_ID}.DB_高校マスタ` WHERE Latest_School_Name LIKE '%{s_in}%' OR Official_School_Name LIKE '%{s_in}%' LIMIT 20")
        
        if not df_s.empty:
            df_s['Label'] = df_s['Latest_School_Name'] + " (" + df_s['Prefecture'] + ")"
            sel = st.selectbox("高校を選択", df_s['Label'].unique())
            
            if sel:
                sid = df_s[df_s['Label']==sel].iloc[0]['School_ID']
                st.markdown(f"### 📜 {sel.split(' (')[0]} の出場履歴")
                
                with st.spinner('履歴を取得中...'):
                    df_h = run_query(f"SELECT Year, Season, School as `当時の校名`, Rank as `成績`, History_Label as `情報` FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績` WHERE School_ID = '{sid}' ORDER BY CAST(Year AS INT64) DESC, Season DESC")
                
                if not df_h.empty:
                    st.dataframe(df_h, use_container_width=True, hide_index=True, column_config={"Year": st.column_config.NumberColumn(format="%d")})
                else:
                    st.warning("出場履歴がありません。")

# ==========================================
# 👤 モード: 選手検索
# ==========================================
elif mode == "👤 選手から探す":
    st.subheader("👤 選手検索")
    p_name = st.text_input("選手名を入力")
    if p_name:
        with st.spinner('選手データを検索中...'):
            df_p = run_query(f"SELECT Name, School_Name_Now, Year, Season, Grade, Uniform_Number, Pro_Team FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手データ完全版` WHERE Name LIKE '%{p_name}%' ORDER BY Year DESC")
        if not df_p.empty:
            st.dataframe(df_p.rename(columns={'Name':'氏名','School_Name_Now':'所属','Year':'年度','Season':'季','Grade':'学年','Uniform_Number':'背番号','Pro_Team':'プロ入り'}), use_container_width=True, hide_index=True)
        else:
            st.warning("選手が見つかりません。")
