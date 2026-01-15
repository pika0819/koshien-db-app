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
        # クライアント作成時に location を指定する
        return bigquery.Client.from_service_account_info(
            st.secrets["gcp_service_account"],
            location="asia-northeast1"  # 東京リージョンを指定
        )
    except Exception as e:
        st.error(f"認証エラー: {e}")
        st.stop()

client = get_bq_client()

# プロジェクトIDとデータセット名を指定
# ※BigQueryのコンソールで表示されている名前に書き換えてください
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
DATASET_ID = "koshien_db" 

# テーブル参照用の接頭辞（バッククォートで囲むのがコツ）
PREFIX = f"`{PROJECT_ID}.{DATASET_ID}"

# --- 2. データ取得関数 ---

@st.cache_data(ttl=600)
def get_tournaments():
    # m_tournament から大会名を取得
    # Yearが文字列でも数字でもソートできるようにSAFE_CASTを使用
    query = f"""
        SELECT Tournament, Year, Season 
        FROM {PREFIX}.m_tournament`
        ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=600)
def get_results(tournament_name):
    # t_results, m_school, m_region をJOINして詳細を取得
    # スプシの列名に合わせてエイリアスを調整
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
        # 選択肢の作成
        # データが欠損していても動くように fillna('') を追加
        df_tourney = df_tourney.fillna('')
        tourney_options = df_tourney.apply(
            lambda x: f"{x['Year']} {x['Season']} - {x['Tournament']}", axis=1
        ).tolist()
        
        selected_option = st.sidebar.selectbox("大会を選択", tourney_options)
        
        # 大会名のみ抽出
        selected_tourney_name = selected_option.split(" - ")[1]

        # メイン画面
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
    st.info("BigQueryのテーブル名やデータセット名、プロジェクトIDが正しいか確認してください。")
