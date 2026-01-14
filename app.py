import streamlit as st
from google.cloud import bigquery
import pandas as pd

# --- 1. アプリ設定 ---
st.set_page_config(page_title="高校野球DB完全版", layout="wide", page_icon="⚾")

# UIのちらつき・ガタつきを抑えるためのCSS
st.markdown("""
<style>
    .stDataFrame { font-size: 0.95rem; }
    /* ローディング中の表示を安定させる */
    .stAlert { margin-top: 1rem; }
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
        return bigquery.Client()

# キャッシュを1時間有効に（TTL設定）
@st.cache_data(ttl=3600)
def run_query(query_string):
    client = get_bq_client()
    return client.query(query_string).to_dataframe()

# --- 2. サイドバー ---
with st.sidebar:
    st.header("📂 メニュー")
    mode = st.radio("検索モード", ["🏆 大会から探す", "👤 選手から探す", "🏫 高校から探す"])
    
    # 強制リフレッシュボタン（キャッシュを消して最新DBを見に行く）
    if st.button("🔄 データを最新に更新"):
        st.cache_data.clear()
        st.rerun()

# ==========================================
# 🏫 モード: 高校検索 (当時の校名を信じる版)
# ==========================================
if mode == "🏫 高校から探す":
    st.subheader("🏫 高校検索")
    s_in = st.text_input("高校名を入力してください", placeholder="例: 高松")
    
    if s_in:
        # 読み込みのちらつきを spinner で隠す
        with st.spinner('検索中...'):
            df_s = run_query(f"""
                SELECT DISTINCT School_ID, Latest_School_Name, Prefecture 
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_高校マスタ` 
                WHERE Latest_School_Name LIKE '%{s_in}%' 
                   OR Official_School_Name LIKE '%{s_in}%' 
                LIMIT 20
            """)
        
        if not df_s.empty:
            df_s['Label'] = df_s['Latest_School_Name'] + " (" + df_s['Prefecture'] + ")"
            sel = st.selectbox("高校を選択", df_s['Label'].unique())
            
            if sel:
                sid = df_s[df_s['Label']==sel].iloc[0]['School_ID']
                st.markdown(f"### 📜 {sel.split(' (')[0]} の出場履歴")
                
                with st.spinner('履歴を取得中...'):
                    # DBが修復されたので、シンプルに T1.School を取得する
                    df_h = run_query(f"""
                        SELECT Year, Season, School, Rank, History_Label
                        FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                        WHERE School_ID = '{sid}'
                        ORDER BY CAST(Year AS INT64) DESC, Season DESC
                    """)
                
                if not df_h.empty:
                    # 表形式に整理
                    display_df = df_h.rename(columns={
                        'Year': '年度', 'Season': '季', 'School': '当時の校名',
                        'Rank': '成績', 'History_Label': '情報'
                    })
                    
                    # ちらつきを抑えるため、一気に表示
                    st.dataframe(
                        display_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "年度": st.column_config.NumberColumn(format="%d"),
                            "当時の校名": st.column_config.TextColumn(width="medium"),
                        }
                    )
                else:
                    st.warning("出場履歴データがありません")
        else:
            st.warning("高校が見つかりませんでした")

# (大会から探す、選手から探すの部分も同様に run_query を使ってシンプルに記述)
