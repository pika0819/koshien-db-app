import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ページ設定
st.set_page_config(page_title="甲子園DB", layout="wide")

# タイトル
st.title("⚾️ 甲子園DB - 大会検索プロトタイプ")

# --- 1. BigQuery接続設定 ---
# Streamlit CloudのSecretsから認証情報を取得してクライアントを作成
try:
    # サービスアカウントキーを使って認証
    client = bigquery.Client.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
except Exception as e:
    st.error(f"BigQueryへの接続に失敗しました: {e}")
    st.stop()

# データセットの指定（あなたの環境に合わせてください）
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
DATASET_ID = "koshien_db" 
TABLE_PREFIX = f"{PROJECT_ID}.{DATASET_ID}"

# --- 2. データ取得関数 ---

# 大会リストを取得（キャッシュして高速化）
@st.cache_data(ttl=600)
def get_tournaments():
    query = f"""
        SELECT Tournament, Year, Season 
        FROM `{TABLE_PREFIX}.m_tournament`
        ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC
    """
    return client.query(query).to_dataframe()

# 選んだ大会の出場校と成績を取得
@st.cache_data(ttl=600)
def get_results(tournament_name):
    query = f"""
        SELECT 
            t.School_Name_Then AS 高校名,
            r.Region AS 地域,
            tr.Rank AS 結果,
            tr.Win_Loss AS 勝敗
        FROM `{TABLE_PREFIX}.t_results` AS tr
        LEFT JOIN `{TABLE_PREFIX}.m_school` AS s ON tr.School_ID = s.School_ID
        LEFT JOIN `{TABLE_PREFIX}.m_region` AS r ON s.Prefecture = r.Prefecture
        WHERE tr.Tournament = @tournament
        ORDER BY r.Region_ID, s.Prefecture
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tournament", "STRING", tournament_name)
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()

# --- 3. UI構築 ---

# サイドバー：大会選択
st.sidebar.header("🔍 検索条件")
df_tournaments = get_tournaments()

if not df_tournaments.empty:
    # 選択肢の作成（例: "2024 夏 - 第106回選手権"）
    # YearやSeasonがNULLの場合も考慮してstr()で囲む
    tourney_options = df_tournaments.apply(
        lambda x: f"{str(x['Year'])} {str(x['Season'])} - {str(x['Tournament'])}", axis=1
    ).tolist()
    
    selected_option = st.sidebar.selectbox("大会を選択", tourney_options)
    
    # 選択肢から大会名だけを取り出す（" - " で分割した後ろの部分）
    selected_tourney_name = selected_option.split(" - ")[1]

    # メイン画面：結果表示
    st.subheader(f"🏟 {selected_tourney_name} 出場校一覧")
    
    df_results = get_results(selected_tourney_name)
    
    if not df_results.empty:
        st.dataframe(
            df_results, 
            use_container_width=True, 
            hide_index=True
        )
        st.caption(f"出場校数: {len(df_results)} 校")
    else:
        st.info("この大会の出場データは見つかりませんでした。")

else:
    st.warning("大会データ（m_tournament）が読み込めませんでした。BigQueryを確認してください。")
