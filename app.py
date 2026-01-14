import streamlit as st
from google.cloud import bigquery
import pandas as pd

# --- 1. アプリ設定 ---
st.set_page_config(page_title="高校野球DB 履歴統合版", layout="wide")

PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

@st.cache_data(ttl=3600)
def run_query(query_string):
    client = get_bq_client()
    return client.query(query_string).to_dataframe()

st.title("🏫 高校別 歴代出場歩み")

# ==========================================
# 🏫 高校検索ロジック（あいまい検索 & 履歴統合）
# ==========================================
s_in = st.text_input("高校名を入力（例：光星、高松）", placeholder="光星")

if s_in:
    with st.spinner('データベースを照合中...'):
        # 1. 高校マスタから「最新校名」や「旧校名（高校列）」でヒットするIDをすべて抽出
        # 💡 ここで「光星」と打てば、八戸学院光星に関連するIDがすべて見つかるようにします
        df_master = run_query(f"""
            SELECT DISTINCT School_ID, Latest_School_Name, Prefecture 
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_高校マスタ` 
            WHERE Latest_School_Name LIKE '%{s_in}%' 
               OR 高校 LIKE '%{s_in}%'
               OR Official_School_Name LIKE '%{s_in}%'
        """)
    
    if not df_master.empty:
        # ユーザーに選択させるためのラベル作成
        df_master['Label'] = df_master['Latest_School_Name'] + " (" + df_master['Prefecture'] + ")"
        options = df_master['Label'].unique()
        sel = st.selectbox("該当する高校を選択してください", options)
        
        if sel:
            # 2. 選択された高校（最新名）に紐付く「すべてのSchool_ID」を取得
            # 💡 統合されたIDや過去のIDが複数あっても、これですべて網羅します
            selected_latest_name = sel.split(" (")[0]
            target_ids = df_master[df_master['Latest_School_Name'] == selected_latest_name]['School_ID'].unique().tolist()
            ids_str = "', '".join(target_ids)
            
            st.markdown(f"### 📜 {selected_latest_name} の歴代出場記録")
            
            # 3. 【重要】「当時の校名(School)」を表示しつつ、すべての時代の成績を出す
            df_history = run_query(f"""
                SELECT 
                    Year as `年度`, 
                    Season as `季`, 
                    School as `当時の校名`, 
                    Rank as `成績`, 
                    Tournament as `大会名`
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                WHERE School_ID IN ('{ids_str}')
                ORDER BY CAST(Year AS INT64) DESC, Season DESC
            """)
            
            if not df_history.empty:
                # ユーザー様への配慮：年度を整数表示に
                df_history['年度'] = pd.to_numeric(df_history['年度']).astype(int)
                
                st.dataframe(
                    df_history, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={"年度": st.column_config.NumberColumn(format="%d")}
                )
                
                # 統計情報（実績の妥当性を示す）
                st.caption(f"計 {len(df_history)} 回の甲子園出場が記録されています。")
            else:
                st.warning("出場成績テーブルに該当するIDのデータが見つかりませんでした。")
    else:
        st.error(f"「{s_in}」に一致する高校が見つかりません。")

# 強制更新ボタン（サイドバー）
if st.sidebar.button("🔄 キャッシュをクリアして更新"):
    st.cache_data.clear()
    st.rerun()
