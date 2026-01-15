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
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
DATASET_ID = "koshien_data"

# --- 2. データ取得関数群 ---

# 大会リスト取得
@st.cache_data(ttl=600)
def get_tournaments():
    sql = "SELECT Tournament, Year, Season FROM `{}.{}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC".format(PROJECT_ID, DATASET_ID)
    return client.query(sql).to_dataframe()

# 出場校一覧取得（重複排除済み）
@st.cache_data(ttl=600)
def get_results_list(tournament_name):
    # マスタ結合による重複を防ぐため DISTINCT を使用
    # 表示用と検索用（ID）を取得
    sql = """
    SELECT DISTINCT
        tr.District AS 地区,
        tr.School_Name_Then AS 校名,
        s.School_Name_Now AS 現在校名,
        tr.History_Label AS 出場回数,
        tr.Rank AS 成績,
        tr.School_ID  -- ドリルダウン用
    FROM `{0}.{1}.t_results` AS tr
    LEFT JOIN `{0}.{1}.m_school` AS s ON tr.School_ID = s.School_ID
    WHERE tr.Tournament = @tournament
    ORDER BY tr.District, tr.School_Name_Then
    """.format(PROJECT_ID, DATASET_ID)
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tournament", "STRING", tournament_name)
        ]
    )
    return client.query(sql, job_config=job_config).to_dataframe()

# 詳細データ取得関数（キャッシュ活用）
@st.cache_data(ttl=600)
def get_school_details(school_id, tournament_name):
    # 1. この大会の戦績 (t_scores)
    sql_scores = """
        SELECT MatchLink, Round, Win_Loss, Score, Opponent, Notes
        FROM `{0}.{1}.t_scores`
        WHERE Tournament = @tournament AND School_ID = @school_id
        ORDER BY Round
    """.format(PROJECT_ID, DATASET_ID)
    
    # 2. 当時のメンバー (m_player)
    sql_members = """
        SELECT Uniform_Number, Position, Name, Grade, Captain
        FROM `{0}.{1}.m_player`
        WHERE Tournament = @tournament AND School_ID = @school_id
        ORDER BY SAFE_CAST(Uniform_Number AS INT64)
    """.format(PROJECT_ID, DATASET_ID)

    # 3. 過去の成績 (t_results) - 最新5件
    sql_history = """
        SELECT Year, Season, Tournament, School_Name_Then, Rank
        FROM `{0}.{1}.t_results`
        WHERE School_ID = @school_id AND Tournament != @tournament
        ORDER BY SAFE_CAST(Year AS INT64) DESC
        LIMIT 10
    """.format(PROJECT_ID, DATASET_ID)

    # 4. 卒業生/プロ入り (m_player) - サンプル
    sql_alumni = """
        SELECT DISTINCT Name, Pro_Team, Draft_Year
        FROM `{0}.{1}.m_player`
        WHERE School_ID = @school_id AND Pro_Team IS NOT NULL
        ORDER BY Draft_Year DESC
    """.format(PROJECT_ID, DATASET_ID)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tournament", "STRING", tournament_name),
            bigquery.ScalarQueryParameter("school_id", "STRING", school_id)
        ]
    )

    return {
        "scores": client.query(sql_scores, job_config=job_config).to_dataframe(),
        "members": client.query(sql_members, job_config=job_config).to_dataframe(),
        "history": client.query(sql_history, job_config=job_config).to_dataframe(),
        "alumni": client.query(sql_alumni, job_config=job_config).to_dataframe()
    }

# --- 3. UI構築 ---

# サイドバー：大会選択
st.sidebar.header("🔍 設定")
df_tourney = get_tournaments()

if not df_tourney.empty:
    df_tourney = df_tourney.fillna('')
    tourney_options = ["{} {} - {}".format(row['Year'], row['Season'], row['Tournament']) for _, row in df_tourney.iterrows()]
    selected_option = st.sidebar.selectbox("大会を選択", tourney_options)
    selected_tourney_name = selected_option.split(" - ")[1] if " - " in selected_option else selected_option
else:
    st.error("大会データが取得できません")
    st.stop()

# メイン画面
st.subheader(f"🏟 {selected_tourney_name} 出場校一覧")

# 一覧取得
df_list = get_results_list(selected_tourney_name)

if not df_list.empty:
    # ユーザーが選択するためのUI（セレクトボックス）
    # 表形式で見せた上で、下で選ばせるスタイル
    st.dataframe(
        df_list[["地区", "校名", "現在校名", "出場回数", "成績"]], 
        use_container_width=True, 
        hide_index=True
    )

    st.markdown("---")
    st.write("🔽 **詳細を見たい高校を選択してください**")
    
    # 校名とIDを紐付けて選択肢作成
    school_options = {row['校名']: row['School_ID'] for _, row in df_list.iterrows()}
    selected_school_name = st.selectbox("高校を選択", list(school_options.keys()))
    
    if selected_school_name:
        school_id = school_options[selected_school_name]
        
        # 詳細データの取得
        with st.spinner(f'{selected_school_name} のデータを分析中...'):
            details = get_school_details(school_id, selected_tourney_name)
        
        st.header(f"🏫 {selected_school_name} の詳細")
        
        # タブで切り替え
        tab1, tab2, tab3, tab4 = st.tabs(["⚾️ この大会の戦績", "👥 当時のメンバー", "📜 過去の成績", "🌟 主なOB（プロ）"])
        
        with tab1:
            if not details["scores"].empty:
                st.dataframe(details["scores"], use_container_width=True, hide_index=True)
            else:
                st.info("戦績データがありません")
                
        with tab2:
            if not details["members"].empty:
                st.dataframe(details["members"], use_container_width=True, hide_index=True)
            else:
                st.info("メンバーデータがありません")
                
        with tab3:
            if not details["history"].empty:
                st.dataframe(details["history"], use_container_width=True, hide_index=True)
            else:
                st.info("過去の出場データがありません")

        with tab4:
            if not details["alumni"].empty:
                st.dataframe(details["alumni"], use_container_width=True, hide_index=True)
            else:
                st.info("プロ入りしたOBデータは見つかりませんでした")

else:
    st.warning("この大会の出場校データが見つかりませんでした。")
