import streamlit as st
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import google.oauth2.service_account

# --- ページ設定 ---
st.set_page_config(page_title="甲子園DB", layout="wide", page_icon="⚾")
st.title("⚾️ 甲子園DB")

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
RAW_DATASET_ID = "koshien_data"  # 倉庫（スプシ）
APP_DATASET_ID = "koshien_app"   # お店（高速）

# --- 2. データ同期機能 ---
def sync_data():
    """スプレッドシートのデータを高速テーブルにコピー"""
    status_text = st.empty()
    bar = st.progress(0)
    
    dataset_ref = client.dataset(APP_DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        status_text.text(f"初期設定中: {APP_DATASET_ID} を作成...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)

    # 同期対象テーブル
    tables = ["m_tournament", "m_school", "m_player", "t_results", "t_scores", "m_region"]
    
    for i, table_name in enumerate(tables):
        status_text.text(f"同期中: {table_name}...")
        query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{APP_DATASET_ID}.{table_name}` AS
        SELECT * FROM `{PROJECT_ID}.{RAW_DATASET_ID}.{table_name}`
        """
        job = client.query(query)
        job.result()
        bar.progress((i + 1) / len(tables))

    status_text.text("完了！リロードします。")
    st.success("データ更新完了")
    st.cache_data.clear()
    st.rerun()

# --- 3. データ取得関数群 ---

# 共通：データフレームの整形（不要な列を隠し、列名を日本語に）
def clean_and_rename(df, type="general"):
    if df.empty: return df
    
    # 隠す列
    drop_cols = ['School_ID', 'ID', 'MatchLink', 'Tournament_ID', 'Region_ID']
    cols = [c for c in df.columns if c not in drop_cols]
    df = df[cols]

    # 列名マッピング（日本語化）
    rename_map = {
        # 共通
        'Year': '年度', 'Season': '季節', 'Tournament': '大会名',
        'School_Name_Now': '現在校名', 'School_Name_Then': '当時の校名',
        'District': '地区', 'Prefecture': '都道府県',
        # 選手系
        'Uniform_Number': '背番号', 'Name': '氏名', 'Position': '守備',
        'Grade': '学年', 'Captain': '主将', 'Pro_Team': 'プロ入団', 'Draft_Year': 'ドラフト年',
        # 戦績系
        'Rank': '成績', 'Win_Loss': '勝敗', 'Score': 'スコア', 'Opponent': '対戦校',
        'Round': '回戦', 'Notes': '備考', 'History_Label': '出場回数'
    }
    return df.rename(columns=rename_map)

# A. 大会検索用
@st.cache_data(ttl=3600)
def get_tournaments():
    try:
        sql = "SELECT * FROM `{}.{}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC".format(PROJECT_ID, APP_DATASET_ID)
        return client.query(sql).to_dataframe().drop_duplicates()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_tournament_details(year, season):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("year", "STRING", str(year)),
            bigquery.ScalarQueryParameter("season", "STRING", str(season))
        ]
    )
    # 出場校一覧
    sql_list = """
    SELECT tr.District, tr.School_Name_Then, s.School_Name_Now, tr.History_Label, tr.Rank, tr.School_ID
    FROM `{0}.{1}.t_results` AS tr
    LEFT JOIN `{0}.{1}.m_school` AS s ON tr.School_ID = s.School_ID
    WHERE tr.Year = @year AND tr.Season = @season
    """.format(PROJECT_ID, APP_DATASET_ID)
    
    # 全データ取得（クライアント側フィルタ用）
    sql_scores = "SELECT * FROM `{0}.{1}.t_scores` WHERE Year = @year AND Season = @season".format(PROJECT_ID, APP_DATASET_ID)
    sql_members = "SELECT * FROM `{0}.{1}.m_player` WHERE Year = @year AND Season = @season".format(PROJECT_ID, APP_DATASET_ID)

    return {
        "list": client.query(sql_list, job_config=job_config).to_dataframe().drop_duplicates(),
        "scores": client.query(sql_scores, job_config=job_config).to_dataframe().drop_duplicates(),
        "members": client.query(sql_members, job_config=job_config).to_dataframe().drop_duplicates()
    }

# B. 高校検索用
@st.cache_data(ttl=3600)
def search_schools(query_text):
    # 名前(新・旧)で検索
    sql = """
    SELECT DISTINCT School_ID, School_Name_Now, Prefecture, School_Name_Then
    FROM `{0}.{1}.m_school`
    WHERE School_Name_Now LIKE @q OR School_Name_Then LIKE @q
    LIMIT 50
    """.format(PROJECT_ID, APP_DATASET_ID)
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("q", "STRING", f"%{query_text}%")]
    )
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_school_history_all(school_id):
    # その高校の全歴史
    sql = """
    SELECT Year, Season, Tournament, School_Name_Then, Rank
    FROM `{0}.{1}.t_results`
    WHERE School_ID = @school_id
    ORDER BY SAFE_CAST(Year AS INT64) DESC
    """.format(PROJECT_ID, APP_DATASET_ID)
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("school_id", "STRING", school_id)]
    )
    return client.query(sql, job_config=job_config).to_dataframe()

# C. 選手検索用
@st.cache_data(ttl=3600)
def search_players(query_text):
    sql = """
    SELECT p.Year, p.Season, s.School_Name_Now, p.School_Name_Then, p.Name, p.Position, p.Grade, p.Uniform_Number
    FROM `{0}.{1}.m_player` AS p
    LEFT JOIN `{0}.{1}.m_school` AS s ON p.School_ID = s.School_ID
    WHERE p.Name LIKE @q
    ORDER BY SAFE_CAST(p.Year AS INT64) DESC
    LIMIT 100
    """.format(PROJECT_ID, APP_DATASET_ID)
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("q", "STRING", f"%{query_text}%")]
    )
    return client.query(sql, job_config=job_config).to_dataframe()


# --- 4. UI構築 ---

# サイドバー設定
st.sidebar.header("🔍 検索モード")
search_mode = st.sidebar.radio("", ["🏟 大会から探す", "🏫 高校名から探す", "👤 選手名から探す"])

st.sidebar.markdown("---")
if st.sidebar.button("🔄 データを最新に更新"):
    with st.spinner("同期中..."):
        sync_data()

# ==========================================
# モード 1: 大会検索 (既存機能のブラッシュアップ)
# ==========================================
if search_mode == "🏟 大会から探す":
    df_tourney = get_tournaments()
    if df_tourney.empty:
        st.info("左下の更新ボタンを押してデータを読み込んでください。")
        st.stop()
        
    df_tourney = df_tourney.fillna('')
    # ラベル作成
    tourney_map = {}
    for _, row in df_tourney.iterrows():
        y, s, t = row.get('Year', ''), row.get('Season', ''), row.get('Tournament', '')
        label = f"{y} {s} - {t}"
        tourney_map[label] = {"year": y, "season": s, "name": t}
    
    selected_label = st.sidebar.selectbox("大会を選択", list(tourney_map.keys()))
    sel = tourney_map[selected_label]
    
    st.header(f"{selected_label}")
    
    with st.spinner("データ展開中..."):
        data = load_tournament_details(sel["year"], sel["season"])
        df_list = data["list"]

    if not df_list.empty:
        # 一覧表示
        st.dataframe(clean_and_rename(df_list), use_container_width=True, hide_index=True)
        
        st.divider()
        st.subheader("🔽 詳細データ閲覧")
        
        # 選択ボックス
        school_opts = dict(zip(df_list['School_Name_Then'], df_list['School_ID']))
        selected_school = st.selectbox("高校を選択してください", list(school_opts.keys()))
        
        if selected_school:
            sid = school_opts[selected_school]
            
            # フィルタリング
            my_scores = data["scores"][data["scores"]['School_ID'] == sid]
            my_members = data["members"][data["members"]['School_ID'] == sid]
            
            # タブ表示
            t1, t2 = st.tabs(["⚾️ 戦績・スコア", "👥 登録メンバー"])
            
            with t1:
                if not my_scores.empty:
                    # スコアを見やすく（重要な列を前に）
                    cols = ['Round', 'Opponent', 'Win_Loss', 'Score', 'Notes']
                    existing_cols = [c for c in cols if c in my_scores.columns]
                    st.dataframe(clean_and_rename(my_scores[existing_cols]), use_container_width=True, hide_index=True)
                else:
                    st.info("戦績データなし")

            with t2:
                if not my_members.empty:
                    # 背番号順にソート
                    if 'Uniform_Number' in my_members.columns:
                        try:
                            my_members['Uniform_Number_Int'] = pd.to_numeric(my_members['Uniform_Number'], errors='coerce')
                            my_members = my_members.sort_values('Uniform_Number_Int').drop(columns=['Uniform_Number_Int'])
                        except: pass
                    
                    # メンバー表らしい列順に
                    target_cols = ['Uniform_Number', 'Position', 'Name', 'Grade', 'Captain']
                    exist_target = [c for c in target_cols if c in my_members.columns]
                    st.dataframe(clean_and_rename(my_members[exist_target]), use_container_width=True, hide_index=True)
                else:
                    st.info("メンバーデータなし")

# ==========================================
# モード 2: 高校検索 (新規追加)
# ==========================================
elif search_mode == "🏫 高校名から探す":
    st.subheader("🏫 高校検索")
    q = st.text_input("高校名を入力してください（一部でもOK）", placeholder="例：大阪桐蔭、早稲田実")
    
    if q:
        res = search_schools(q)
        if not res.empty:
            st.write(f"検索結果: {len(res)} 件")
            
            # 高校を選択
            # 同じ名前でもIDが違う場合があるため、都道府県や旧名を混ぜて一意にする
            res['label'] = res.apply(lambda x: f"{x['School_Name_Now']} ({x['Prefecture']})", axis=1)
            school_select = st.selectbox("詳細を見る高校を選択", res['label'].unique())
            
            # 選択された高校のIDを取得
            selected_row = res[res['label'] == school_select].iloc[0]
            sid = selected_row['School_ID']
            
            st.divider()
            st.markdown(f"### 📜 {selected_row['School_Name_Now']} の甲子園全成績")
            
            history_df = get_school_history_all(sid)
            if not history_df.empty:
                st.dataframe(clean_and_rename(history_df), use_container_width=True, hide_index=True)
            else:
                st.warning("甲子園の出場記録は見つかりませんでした")
        else:
            st.warning("見つかりませんでした")

# ==========================================
# モード 3: 選手検索 (新規追加)
# ==========================================
elif search_mode == "👤 選手名から探す":
    st.subheader("👤 選手検索")
    q = st.text_input("選手名を入力してください", placeholder="例：松坂大輔、イチロー")
    
    if q:
        res = search_players(q)
        if not res.empty:
            st.write(f"ヒットしました: {len(res)} 件")
            # 選手一覧を表示
            st.dataframe(clean_and_rename(res), use_container_width=True, hide_index=True)
        else:
            st.warning("該当する選手は見つかりませんでした")
