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

# --- 2. データ取得関数群（高速化のため「まとめ読み」に変更） ---

# 大会リスト取得
@st.cache_data(ttl=3600)
def get_tournaments():
    sql = "SELECT * FROM `{}.{}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC".format(PROJECT_ID, DATASET_ID)
    df = client.query(sql).to_dataframe().drop_duplicates()
    return df

# 【高速化】その大会の「全データ（出場校・戦績・メンバー）」を一括で取ってくる
@st.cache_data(ttl=3600)
def load_tournament_data(year, season):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("year", "STRING", str(year)),
            bigquery.ScalarQueryParameter("season", "STRING", str(season))
        ]
    )

    # 1. 出場校リスト
    sql_list = """
    SELECT tr.*, s.School_Name_Now
    FROM `{0}.{1}.t_results` AS tr
    LEFT JOIN `{0}.{1}.m_school` AS s ON tr.School_ID = s.School_ID
    WHERE tr.Year = @year AND tr.Season = @season
    """.format(PROJECT_ID, DATASET_ID)
    
    # 2. 全試合の戦績（この大会の分すべて）
    sql_scores = """
    SELECT * FROM `{0}.{1}.t_scores` 
    WHERE Year = @year AND Season = @season
    """.format(PROJECT_ID, DATASET_ID)

    # 3. 全登録メンバー（この大会の分すべて）
    sql_members = """
    SELECT * FROM `{0}.{1}.m_player` 
    WHERE Year = @year AND Season = @season
    """.format(PROJECT_ID, DATASET_ID)

    # BigQueryへリクエスト（3つ並列で投げてもいいが、ここではシンプルに順次実行してキャッシュする）
    df_list = client.query(sql_list, job_config=job_config).to_dataframe().drop_duplicates()
    df_scores = client.query(sql_scores, job_config=job_config).to_dataframe().drop_duplicates()
    df_members = client.query(sql_members, job_config=job_config).to_dataframe().drop_duplicates()

    # 整形（列名マッピング）
    rename_map = {
        'District': '地区', 'School_Name_Then': '校名', 
        'School_Name_Now': '現在校名', 'History_Label': '出場回数', 'Rank': '成績'
    }
    available_cols = [c for c in rename_map.keys() if c in df_list.columns]
    df_list_display = df_list.rename(columns=rename_map)
    # IDは内部結合用に残すが、後で表示しない
    if 'School_ID' in df_list.columns:
        df_list_display['School_ID'] = df_list['School_ID']

    return {
        "list": df_list_display,
        "scores": df_scores,
        "members": df_members
    }

# 個別の「過去の成績」と「卒業生」だけはその都度取る（データ量が多いため）
@st.cache_data(ttl=3600)
def get_history_and_alumni(school_id):
    sql_history = """
        SELECT Year, Season, Tournament, School_Name_Then, Rank 
        FROM `{0}.{1}.t_results` 
        WHERE School_ID = @school_id 
        ORDER BY SAFE_CAST(Year AS INT64) DESC
    """.format(PROJECT_ID, DATASET_ID)

    sql_alumni = """
        SELECT Name, Pro_Team, Draft_Year 
        FROM `{0}.{1}.m_player` 
        WHERE School_ID = @school_id AND (Pro_Team IS NOT NULL AND Pro_Team != '')
        ORDER BY Draft_Year DESC
    """.format(PROJECT_ID, DATASET_ID)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("school_id", "STRING", school_id)]
    )

    return {
        "history": client.query(sql_history, job_config=job_config).to_dataframe().drop_duplicates(),
        "alumni": client.query(sql_alumni, job_config=job_config).to_dataframe().drop_duplicates()
    }

# --- 3. UI構築 ---

st.sidebar.header("🔍 設定")
df_tourney = get_tournaments()

if not df_tourney.empty:
    df_tourney = df_tourney.fillna('')
    tourney_map = {}
    # 列名判定
    y_col = 'Year' if 'Year' in df_tourney.columns else df_tourney.columns[1]
    s_col = 'Season' if 'Season' in df_tourney.columns else df_tourney.columns[2]
    t_col = 'Tournament' if 'Tournament' in df_tourney.columns else df_tourney.columns[0]

    for _, row in df_tourney.iterrows():
        label = "{} {} - {}".format(row[y_col], row[s_col], row[t_col])
        tourney_map[label] = {"year": row[y_col], "season": row[s_col], "name": row[t_col]}
    
    selected_label = st.sidebar.selectbox("大会を選択", list(tourney_map.keys()))
    selected_data = tourney_map[selected_label]
else:
    st.error("大会データが取得できません")
    st.stop()

# メイン画面
st.subheader(f"🏟 {selected_label} 出場校一覧")

# ★ここで「全データ」を一括読み込み（キャッシュされるので2回目以降は爆速）
with st.spinner("データを準備中..."):
    dataset = load_tournament_data(selected_data["year"], selected_data["season"])
    df_list = dataset["list"]

if not df_list.empty:
    # 一覧表示（IDは隠す）
    display_cols = [c for c in ["地区", "校名", "現在校名", "出場回数", "成績"] if c in df_list.columns]
    st.dataframe(df_list[display_cols], use_container_width=True, hide_index=True)

    st.markdown("---")
    st.write("🔽 **詳細を見たい高校を選択してください**")
    
    if '校名' in df_list.columns and 'School_ID' in df_list.columns:
        # 校名辞書作成
        school_options = dict(zip(df_list['校名'], df_list['School_ID']))
        selected_school_name = st.selectbox("高校を選択", list(school_options.keys()))
        school_id = school_options[selected_school_name]
        
        # ★Python側でフィルタリング（通信なしで高速！）
        # 全データから、選ばれたIDの行だけを抜き出す
        this_scores = dataset["scores"][dataset["scores"]['School_ID'] == school_id]
        this_members = dataset["members"][dataset["members"]['School_ID'] == school_id]
        
        # 過去データだけは別途取得（頻度が低いのでオンデマンドでOK）
        extra_data = get_history_and_alumni(school_id)

        st.header(f"🏫 {selected_school_name}")
        
        tab1, tab2, tab3, tab4 = st.tabs(["⚾️ 戦績", "👥 メンバー", "📜 過去成績", "🌟 卒業生"])
        
        # ID列を隠すための関数
        def clean_df(df):
            if df.empty: return df
            # School_ID, Year, Season, Tournament などの管理用カラムを隠す
            cols_to_hide = ['School_ID', 'Year', 'Season', 'Tournament', 'MatchLink', 'ID']
            cols = [c for c in df.columns if c not in cols_to_hide]
            return df[cols]

        with tab1:
            if not this_scores.empty:
                st.dataframe(clean_df(this_scores), use_container_width=True, hide_index=True)
            else:
                st.info("データなし")
        with tab2:
            if not this_members.empty:
                # メンバー表は見やすい順に（背番号順など）
                if 'Uniform_Number' in this_members.columns:
                     # 数値変換してソートを試みる（エラーならそのまま）
                    try:
                        this_members = this_members.sort_values(by='Uniform_Number', key=lambda x: pd.to_numeric(x, errors='coerce'))
                    except:
                        pass
                st.dataframe(clean_df(this_members), use_container_width=True, hide_index=True)
            else:
                st.info("データなし")
        with tab3:
            st.dataframe(clean_df(extra_data["history"]), use_container_width=True, hide_index=True)
        with tab4:
            if not extra_data["alumni"].empty:
                st.dataframe(clean_df(extra_data["alumni"]), use_container_width=True, hide_index=True)
            else:
                st.info("プロ入りOBデータなし")
else:
    st.warning("データが見つかりませんでした")
