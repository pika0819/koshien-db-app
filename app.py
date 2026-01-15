import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ページ設定
st.set_page_config(page_title="甲子園DB", layout="wide")

# タイトル
st.title("⚾️ 甲子園DB - 大会検索")

# --- 1. BigQuery接続設定 ---
@st.cache_resource
def get_bq_client():
    try:
        # ロケーション指定は削除（USがデフォルトのため）
        return bigquery.Client.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
    except Exception as e:
        st.error(f"認証エラー: {e}")
        st.stop()

client = get_bq_client()

# --- 【重要】スクリーンショットに合わせて修正済み ---
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
DATASET_ID = "koshien_data" # koshien_db から koshien_data に修正
PREFIX = f"`{PROJECT_ID}.{DATASET_ID}"

# --- 2. データ取得関数 ---

@st.cache_data(ttl=600)
def get_tournaments():
    # 順序補正後の m_tournament を参照
    query = f"""
        SELECT Tournament, Year, Season 
        FROM {PREFIX}.m_tournament`
        ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=600)
def get_results(tournament_name):
    # m_school, m_region とJOINして地域情報を取得
    query = f"""
        SELECT 
            tr.School_Name_Then AS 高校名,
            reg.Region AS 地域,
            tr.Rank AS 結果,
            tr.History_Label AS 記録
        FROM {PREFIX}.t_results` AS tr
        LEFT JOIN {PREFIX}.m_school` AS s ON tr.School_ID = s.School_ID
        LEFT JOIN {PREFIX}.m_region` AS reg ON s.Prefecture = reg.Prefecture
        WHERE tr.Tournament = @tournament
        ORDER BY reg.Region_ID, s.Prefecture
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tournament", "STRING", tournament_name)
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()

# --- 3. UI（サイドバーと表示） ---

st.sidebar.header("🔍 検索条件")

try:
    df_tourney = get_tournaments()

    if not df_tourney.empty:
        # 欠損値対策
        df_tourney = df_tourney.fillna('')
        tourney_options = df_tourney.apply(
            lambda x: f"{x['Year']} {x['Season']} - {x['Tournament']}", axis=1
        ).tolist()
        
        selected_option = st.sidebar.selectbox("大会を選択", tourney_options)
        
        # 文字列から大会名だけを安全に抽出
        if " - " in selected_option:
            selected_tourney_name = selected_option.split(" - ")[1]
        else:
            selected_tourney_name = selected_option

        st.subheader(f"🏟 {selected_tourney_name} 出場校一覧")
        
        with st.spinner('データを読み込み中...'):
            df_results = get_results(selected_tourney_name)
        
        if not df_results.empty:
            st.dataframe(
                df_results, 
                use_container_width=True,
                hide_index=True
            )
            st.info(f"全 {len(df_results)} 校のデータが表示されています。")
        else:
            st.warning("この大会の出場校データが見つかりませんでした。")
    else:
        st.error("大会リストを取得できませんでした。")

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
