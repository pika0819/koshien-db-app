import streamlit as st
from google.cloud import bigquery
import pandas as pd
import google.oauth2.service_account

# ページ設定
st.set_page_config(page_title="甲子園DB", layout="wide")

st.title("⚾️ 甲子園DB - 大会検索")

# --- 1. BigQuery接続設定 ---
@st.cache_resource
def get_bq_client():
    try:
        scopes = [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=scopes
        )
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception as e:
        st.error(f"認証エラー: {e}")
        st.stop()

client = get_bq_client()

# 変数設定（バッククォートを含めない純粋な名前のみ）
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
DATASET_ID = "koshien_data" 

# --- 2. データ取得関数 ---

@st.cache_data(ttl=600)
def get_tournaments():
    # テーブル名をバッククォートで確実に囲む
    query = f"SELECT Tournament, Year, Season FROM `{PROJECT_ID}.{DATASET_ID}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC"
    return client.query(query).to_dataframe()

@st.cache_data(ttl=600)
def get_results(tournament_name):
    # 改行やインデントで全角が混じらないよう、クエリを整理
    query = f"""
        SELECT 
            tr.School_Name_Then AS 高校名,
            reg.Region AS 地域,
            tr.Rank AS 結果,
            tr.History_Label AS 記録
        FROM `{PROJECT_ID}.{DATASET_ID}.t_results` AS tr
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.m_school` AS s ON tr.School_ID = s.School_ID
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.m_region` AS reg ON s.Prefecture = reg.Prefecture
        WHERE tr.Tournament = @tournament
        ORDER BY reg.Region_ID, s.Prefecture
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tournament", "STRING", tournament_name)
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()

# --- 3. UI ---

st.sidebar.header("🔍 検索条件")

try:
    df_tourney = get_tournaments()

    if not df_tourney.empty:
        df_tourney = df_tourney.fillna('')
        # セレクトボックス用のラベル作成
        tourney_options = []
        for _, row in df_tourney.iterrows():
            tourney_options.append(f"{row['Year']} {row['Season']} - {row['Tournament']}")
        
        selected_option = st.sidebar.selectbox("大会を選択", tourney_options)
        selected_tourney_name = selected_option.split(" - ")[1] if " - " in selected_option else selected_option

        st.subheader(f"🏟 {selected_tourney_name} 出場校一覧")
        
        with st.spinner('データを読み込み中...'):
            df_results = get_results(selected_tourney_name)
        
        if not df_results.empty:
            st.dataframe(df_results, use_container_width=True, hide_index=True)
            st.info(f"全 {len(df_results)} 校のデータが表示されています。")
        else:
            st.warning("この大会の出場校データは見つかりませんでした。")
    else:
        st.error("大会リストを取得できませんでした。")

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
