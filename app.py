import streamlit as st
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
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

# ★設定変更：倉庫（スプシ連携）と、お店（高速ネイティブ）を分ける
RAW_DATASET_ID = "koshien_data"  # 今あるデータセット（スプレッドシート連携：遅い）
APP_DATASET_ID = "koshien_app"   # 新しく作るデータセット（ネイティブテーブル：爆速）

# --- 2. データ同期機能（ここがハイブリッドの肝！） ---

def sync_data():
    """スプレッドシートのデータを、高速なネイティブテーブルにコピーする"""
    status_text = st.empty()
    bar = st.progress(0)
    
    # 1. アプリ用データセットが存在するか確認し、なければ作る
    dataset_ref = client.dataset(APP_DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        status_text.text(f"データセット {APP_DATASET_ID} を作成中...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US" # ロケーション合わせる
        client.create_dataset(dataset)

    # 2. テーブルをコピー（CREATE OR REPLACE TABLE AS SELECT *）
    # 同期したいテーブル名をリストアップ
    tables = ["m_tournament", "m_school", "m_player", "t_results", "t_scores", "m_region"]
    
    for i, table_name in enumerate(tables):
        status_text.text(f"データを同期中: {table_name}...")
        
        # 魔法のSQL：スプシ(RAW)から読み込んで、アプリ用(APP)に書き込む
        query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{APP_DATASET_ID}.{table_name}` AS
        SELECT * FROM `{PROJECT_ID}.{RAW_DATASET_ID}.{table_name}`
        """
        job = client.query(query)
        job.result() # 完了まで待つ
        
        bar.progress((i + 1) / len(tables))

    status_text.text("同期完了！画面をリロードします。")
    st.success("最新データを読み込みました！")
    st.cache_data.clear() # キャッシュをクリア
    st.rerun()

# --- 3. データ取得関数群（参照先を APP_DATASET_ID に変更） ---

# 大会リスト取得
@st.cache_data(ttl=3600)
def get_tournaments():
    # 高速な APP_DATASET_ID から読む
    try:
        sql = "SELECT * FROM `{}.{}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC".format(PROJECT_ID, APP_DATASET_ID)
        df = client.query(sql).to_dataframe().drop_duplicates()
        return df
    except Exception:
        # まだ同期してなくてテーブルがない場合
        return pd.DataFrame()

# 大会データ一括読み込み
@st.cache_data(ttl=3600)
def load_tournament_data(year, season):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("year", "STRING", str(year)),
            bigquery.ScalarQueryParameter("season", "STRING", str(season))
        ]
    )

    sql_list = """
    SELECT tr.*, s.School_Name_Now
    FROM `{0}.{1}.t_results` AS tr
    LEFT JOIN `{0}.{1}.m_school` AS s ON tr.School_ID = s.School_ID
    WHERE tr.Year = @year AND tr.Season = @season
    """.format(PROJECT_ID, APP_DATASET_ID)
    
    sql_scores = "SELECT * FROM `{0}.{1}.t_scores` WHERE Year = @year AND Season = @season".format(PROJECT_ID, APP_DATASET_ID)
    sql_members = "SELECT * FROM `{0}.{1}.m_player` WHERE Year = @year AND Season = @season".format(PROJECT_ID, APP_DATASET_ID)

    df_list = client.query(sql_list, job_config=job_config).to_dataframe().drop_duplicates()
    df_scores = client.query(sql_scores, job_config=job_config).to_dataframe().drop_duplicates()
    df_members = client.query(sql_members, job_config=job_config).to_dataframe().drop_duplicates()

    rename_map = {'District': '地区', 'School_Name_Then': '校名', 'School_Name_Now': '現在校名', 'History_Label': '出場回数', 'Rank': '成績'}
    available_cols = [c for c in rename_map.keys() if c in df_list.columns]
    df_list_display = df_list.rename(columns=rename_map)
    if 'School_ID' in df_list.columns:
        df_list_display['School_ID'] = df_list['School_ID']

    return {"list": df_list_display, "scores": df_scores, "members": df_members}

# 過去データ・OB取得
@st.cache_data(ttl=3600)
def get_history_and_alumni(school_id):
    sql_history = """
        SELECT Year, Season, Tournament, School_Name_Then, Rank 
        FROM `{0}.{1}.t_results` 
        WHERE School_ID = @school_id 
        ORDER BY SAFE_CAST(Year AS INT64) DESC
    """.format(PROJECT_ID, APP_DATASET_ID)

    sql_alumni = """
        SELECT Name, Pro_Team, Draft_Year 
        FROM `{0}.{1}.m_player` 
        WHERE School_ID = @school_id AND (Pro_Team IS NOT NULL AND Pro_Team != '')
        ORDER BY Draft_Year DESC
    """.format(PROJECT_ID, APP_DATASET_ID)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("school_id", "STRING", school_id)]
    )

    return {
        "history": client.query(sql_history, job_config=job_config).to_dataframe().drop_duplicates(),
        "alumni": client.query(sql_alumni, job_config=job_config).to_dataframe().drop_duplicates()
    }

# --- 4. UI構築 ---

st.sidebar.header("🔍 設定")

# ★ハイブリッドの要：同期ボタン
st.sidebar.markdown("---")
st.sidebar.caption("管理者メニュー")
if st.sidebar.button("🔄 データを最新に更新"):
    with st.spinner("スプレッドシートからデータを取込中..."):
        sync_data()
st.sidebar.markdown("---")

df_tourney = get_tournaments()

if not df_tourney.empty:
    df_tourney = df_tourney.fillna('')
    tourney_map = {}
    y_col = 'Year' if 'Year' in df_tourney.columns else df_tourney.columns[1]
    s_col = 'Season' if 'Season' in df_tourney.columns else df_tourney.columns[2]
    t_col = 'Tournament' if 'Tournament' in df_tourney.columns else df_tourney.columns[0]

    for _, row in df_tourney.iterrows():
        label = "{} {} - {}".format(row[y_col], row[s_col], row[t_col])
        tourney_map[label] = {"year": row[y_col], "season": row[s_col], "name": row[t_col]}
    
    selected_label = st.sidebar.selectbox("大会を選択", list(tourney_map.keys()))
    selected_data = tourney_map[selected_label]

    # メイン画面
    st.subheader(f"🏟 {selected_label} 出場校一覧")

    with st.spinner("データを準備中..."):
        dataset = load_tournament_data(selected_data["year"], selected_data["season"])
        df_list = dataset["list"]

    if not df_list.empty:
        display_cols = [c for c in ["地区", "校名", "現在校名", "出場回数", "成績"] if c in df_list.columns]
        st.dataframe(df_list[display_cols], use_container_width=True, hide_index=True)

        st.markdown("---")
        st.write("🔽 **詳細を見たい高校を選択してください**")
        
        if '校名' in df_list.columns and 'School_ID' in df_list.columns:
            school_options = dict(zip(df_list['校名'], df_list['School_ID']))
            selected_school_name = st.selectbox("高校を選択", list(school_options.keys()))
            school_id = school_options[selected_school_name]
            
            # Python側フィルタリング
            this_scores = dataset["scores"][dataset["scores"]['School_ID'] == school_id]
            this_members = dataset["members"][dataset["members"]['School_ID'] == school_id]
            extra_data = get_history_and_alumni(school_id)

            st.header(f"🏫 {selected_school_name}")
            tab1, tab2, tab3, tab4 = st.tabs(["⚾️ 戦績", "👥 メンバー", "📜 過去成績", "🌟 卒業生"])
            
            def clean_df(df):
                if df.empty: return df
                cols_to_hide = ['School_ID', 'Year', 'Season', 'Tournament', 'MatchLink', 'ID']
                cols = [c for c in df.columns if c not in cols_to_hide]
                return df[cols]

            with tab1:
                if not this_scores.empty: st.dataframe(clean_df(this_scores), use_container_width=True, hide_index=True)
                else: st.info("データなし")
            with tab2:
                if not this_members.empty:
                    if 'Uniform_Number' in this_members.columns:
                        try: this_members = this_members.sort_values(by='Uniform_Number', key=lambda x: pd.to_numeric(x, errors='coerce'))
                        except: pass
                    st.dataframe(clean_df(this_members), use_container_width=True, hide_index=True)
                else: st.info("データなし")
            with tab3:
                st.dataframe(clean_df(extra_data["history"]), use_container_width=True, hide_index=True)
            with tab4:
                if not extra_data["alumni"].empty: st.dataframe(clean_df(extra_data["alumni"]), use_container_width=True, hide_index=True)
                else: st.info("プロ入りOBデータなし")
    else:
        st.warning("データが見つかりませんでした")

else:
    # まだ同期していない場合
    st.info("👈 左のサイドバーにある「🔄 データを最新に更新」ボタンを押して、初期データを読み込んでください！")
