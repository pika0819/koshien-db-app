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

# 変数は直接指定（タイポ防止のため）
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
DATASET_ID = "koshien_data"

# --- 2. データ取得関数 ---

@st.cache_data(ttl=600)
def get_tournaments():
    # 改行を使わず、もっともシンプルな1行のクエリにします
    sql = "SELECT Tournament, Year, Season FROM `{}.{}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC".format(PROJECT_ID, DATASET_ID)
    return client.query(sql).to_dataframe()

@st.cache_data(ttl=600)
def get_results(tournament_name):
    # f-stringを使わず、format関数でパスを流し込みます
    sql = """
    SELECT 
        tr.School_Name_Then AS kousien_school,
        reg.Region AS region_name,
        tr.Rank AS result_rank,
        tr.History_Label AS record_label
    FROM `{0}.{1}.t_results` AS tr
    LEFT JOIN `{0}.{1}.m_school` AS s ON tr.School_ID = s.School_ID
    LEFT JOIN `{0}.{1}.m_region` AS reg ON s.Prefecture = reg.Prefecture
    WHERE tr.Tournament = @tournament
    ORDER BY reg.Region_ID, s.Prefecture
    """.format(PROJECT_ID, DATASET_ID)
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tournament", "STRING", tournament_name)
        ]
    )
    return client.query(sql, job_config=job_config).to_dataframe()

# --- 3. UI ---

st.sidebar.header("🔍 検索条件")

try:
    df_tourney = get_tournaments()

    if not df_tourney.empty:
        df_tourney = df_tourney.fillna('')
        # 安全なリスト作成
        tourney_options = []
        for i in range(len(df_tourney)):
            row = df_tourney.iloc[i]
            tourney_options.append("{} {} - {}".format(row['Year'], row['Season'], row['Tournament']))
        
        selected_option = st.sidebar.selectbox("大会を選択", tourney_options)
        
        # 選択肢から大会名を取り出す
        selected_tourney_name = selected_option.split(" - ")[1] if " - " in selected_option else selected_option

        st.subheader("🏟 {} 出場校一覧".format(selected_tourney_name))
        
        with st.spinner('データを読み込み中...'):
            df_results = get_results(selected_tourney_name)
        
        if not df_results.empty:
            # カラム名を日本語に直して表示
            df_display = df_results.rename(columns={
                'kousien_school': '高校名',
                'region_name': '地域',
                'result_rank': '結果',
                'record_label': '記録'
            })
            st.dataframe(df_display, use_container_width=True, hide_index=True)
            st.info("全 {} 校のデータが表示されています。".format(len(df_display)))
        else:
            st.warning("この大会の出場校データは見つかりませんでした。")
    else:
        st.error("大会リストを取得できませんでした。")

except Exception as e:
    st.error("エラーが発生しました: {}".format(e))
