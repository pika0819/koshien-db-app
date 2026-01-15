import streamlit as st
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import google.oauth2.service_account

# --- ページ設定 ---
st.set_page_config(page_title="甲子園DB", layout="wide", page_icon="⚾")
st.title("⚾️ 甲子園DB")

# スタイル調整
st.markdown("""
<style>
    .pro-box {
        padding: 15px; border-radius: 8px; background-color: #2e8b57; color: white;
        margin-bottom: 10px; font-weight: bold; border: 1px solid #1e5b38;
    }
    .japan-box {
        padding: 15px; border-radius: 8px; background-color: #DAA520; color: white;
        margin-bottom: 10px; font-weight: bold; border: 1px solid #B8860B;
    }
    .profile-meta {
        color: #666; font-size: 0.9em; margin-bottom: 15px;
    }
    /* リンクボタンのスタイル調整 */
    div[data-testid="stLinkButton"] p { font-weight: bold; }
</style>
""", unsafe_allow_html=True)

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
RAW_DATASET_ID = "koshien_data"
APP_DATASET_ID = "koshien_app"

# --- 2. データ同期機能 ---
def sync_data():
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

# --- 3. データ取得・整形関数 ---

def clean_and_rename(df):
    if df.empty: return df
    drop_cols = ['School_ID', 'ID', 'MatchLink', 'Tournament_ID', 'Region_ID']
    cols = [c for c in df.columns if c not in drop_cols]
    df = df[cols]
    rename_map = {
        'Year': '年度', 'Season': '季節', 'Tournament': '大会名',
        'School_Name_Now': '現在校名', 'School_Name_Then': '当時の校名',
        'District': '地区', 'Prefecture': '都道府県',
        'Uniform_Number': '背番号', 'Name': '氏名', 'Position': '守備',
        'Grade': '学年', 'Captain': '主将', 'Pro_Team': 'プロ入団', 
        'Draft_Year': 'ドラフト年', 'Draft_Rank': '順位', 'Throw_Bat': '投打',
        'Birth_Date': '生年月日', 'Generation': '世代', 
        # 新しい列名
        'U12': 'U12代表', 'U15': 'U15代表', 'U18': 'U18代表', 'U22': 'U22代表', 'JAPAN': '侍ジャパン',
        'Rank': '成績', 'Win_Loss': '勝敗', 'Score': 'スコア', 'Opponent': '対戦校',
        'Round': '回戦', 'Notes': '備考', 'History_Label': '出場回数',
        'Year_Link': '年度リンク', 'History_Link': '歴史館', 'Virtual_Koshien_Link': 'バーチャル'
    }
    return df.rename(columns=rename_map)

# A. 大会検索
@st.cache_data(ttl=3600)
def get_tournaments():
    try:
        sql = "SELECT * FROM `{}.{}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC".format(PROJECT_ID, APP_DATASET_ID)
        return client.query(sql).to_dataframe().drop_duplicates()
    except: return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_tournament_details(year, season):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("year", "STRING", str(year)),
            bigquery.ScalarQueryParameter("season", "STRING", str(season))
        ]
    )
    sql_list = """
    SELECT tr.District, tr.School_Name_Then, s.School_Name_Now, tr.History_Label, tr.Rank, tr.School_ID
    FROM `{0}.{1}.t_results` AS tr
    LEFT JOIN `{0}.{1}.m_school` AS s ON tr.School_ID = s.School_ID
    WHERE tr.Year = @year AND tr.Season = @season
    """.format(PROJECT_ID, APP_DATASET_ID)
    
    sql_scores = "SELECT * FROM `{0}.{1}.t_scores` WHERE Year = @year AND Season = @season".format(PROJECT_ID, APP_DATASET_ID)
    sql_members = "SELECT * FROM `{0}.{1}.m_player` WHERE Year = @year AND Season = @season".format(PROJECT_ID, APP_DATASET_ID)

    return {
        "list": client.query(sql_list, job_config=job_config).to_dataframe().drop_duplicates(),
        "scores": client.query(sql_scores, job_config=job_config).to_dataframe().drop_duplicates(),
        "members": client.query(sql_members, job_config=job_config).to_dataframe().drop_duplicates()
    }

# B. 選手検索
@st.cache_data(ttl=3600)
def search_players_list(query_text):
    sql = """
    SELECT DISTINCT Name, School_Name_Then, MAX(Year) as Last_Year
    FROM `{0}.{1}.m_player`
    WHERE Name LIKE @q
    GROUP BY Name, School_Name_Then
    ORDER BY Last_Year DESC
    LIMIT 50
    """.format(PROJECT_ID, APP_DATASET_ID)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("q", "STRING", f"%{query_text}%")]
    )
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_player_detail(name, school_then):
    sql = """
    SELECT p.*, tr.Rank as Tournament_Rank
    FROM `{0}.{1}.m_player` AS p
    LEFT JOIN `{0}.{1}.t_results` AS tr 
      ON p.School_ID = tr.School_ID AND p.Year = tr.Year AND p.Season = tr.Season
    WHERE p.Name = @name AND p.School_Name_Then = @school_then
    ORDER BY SAFE_CAST(p.Year AS INT64), p.Season
    """.format(PROJECT_ID, APP_DATASET_ID)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("name", "STRING", name),
            bigquery.ScalarQueryParameter("school_then", "STRING", school_then)
        ]
    )
    return client.query(sql, job_config=job_config).to_dataframe()

# C. 高校検索
@st.cache_data(ttl=3600)
def search_schools(query_text):
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


# --- 4. UI構築 ---

st.sidebar.header("🔍 検索モード")
search_mode = st.sidebar.radio("", ["🏟 大会から探す", "👤 選手名から探す", "🏫 高校名から探す"])

st.sidebar.markdown("---")
st.sidebar.caption("※スプレッドシートの列を追加・変更した場合は、必ず下のボタンを押して反映させてください。")
if st.sidebar.button("🔄 データを最新に更新"):
    with st.spinner("スプレッドシートから最新データを同期中..."):
        sync_data()

# === モード1: 大会検索 ===
if search_mode == "🏟 大会から探す":
    df_tourney = get_tournaments()
    if df_tourney.empty:
        st.info("左下の更新ボタンを押してデータを読み込んでください。")
        st.stop()
        
    df_tourney = df_tourney.fillna('')
    tourney_map = {}
    
    # 選択肢データの作成
    for _, row in df_tourney.iterrows():
        y, s, t = row.get('Year', ''), row.get('Season', ''), row.get('Tournament', '')
        label = f"{y} {s} - {t}"
        # 3種類のリンクを辞書に保持
        tourney_map[label] = {
            "year": y, "season": s, "name": t,
            "link_year": row.get('Year_Link', ''),
            "link_hist": row.get('History_Link', ''),
            "link_virt": row.get('Virtual_Koshien_Link', '')
        }
    
    selected_label = st.sidebar.selectbox("大会を選択", list(tourney_map.keys()))
    sel = tourney_map[selected_label]
    
    # === 3つのリンクボタンを並べて表示 ===
    st.header(f"{selected_label}")
    
    # リンクが存在するかチェック
    links_to_show = []
    if sel["link_year"] and sel["link_year"].startswith("http"):
        links_to_show.append(("🔗 大会情報 (主催者)", sel["link_year"]))
    if sel["link_hist"] and sel["link_hist"].startswith("http"):
        links_to_show.append(("🏛 甲子園歴史館", sel["link_hist"]))
    if sel["link_virt"] and sel["link_virt"].startswith("http"):
        links_to_show.append(("📺 バーチャル高校野球", sel["link_virt"]))
    
    if links_to_show:
        cols = st.columns(len(links_to_show))
        for i, (text, url) in enumerate(links_to_show):
            cols[i].link_button(text, url)
    
    st.divider()

    with st.spinner("データ展開中..."):
        data = load_tournament_details(sel["year"], sel["season"])
        df_list = data["list"]

    if not df_list.empty:
        st.dataframe(clean_and_rename(df_list), use_container_width=True, hide_index=True)
        st.subheader("🔽 詳細データ閲覧")
        
        school_opts = dict(zip(df_list['School_Name_Then'], df_list['School_ID']))
        selected_school = st.selectbox("高校を選択してください", list(school_opts.keys()))
        
        if selected_school:
            sid = school_opts[selected_school]
            my_scores = data["scores"][data["scores"]['School_ID'] == sid]
            my_members = data["members"][data["members"]['School_ID'] == sid]
            
            t1, t2 = st.tabs(["⚾️ 戦績・スコア", "👥 登録メンバー"])
            with t1:
                if not my_scores.empty:
                    cols = ['Round', 'Opponent', 'Win_Loss', 'Score', 'Notes']
                    existing_cols = [c for c in cols if c in my_scores.columns]
                    st.dataframe(clean_and_rename(my_scores[existing_cols]), use_container_width=True, hide_index=True)
                else: st.info("戦績データなし")
            with t2:
                if not my_members.empty:
                    if 'Uniform_Number' in my_members.columns:
                        try:
                            my_members['Unum'] = pd.to_numeric(my_members['Uniform_Number'], errors='coerce')
                            my_members = my_members.sort_values('Unum').drop(columns=['Unum'])
                        except: pass
                    target_cols = ['Uniform_Number', 'Position', 'Name', 'Grade', 'Captain', 'Pro_Team']
                    exist_target = [c for c in target_cols if c in my_members.columns]
                    st.dataframe(clean_and_rename(my_members[exist_target]), use_container_width=True, hide_index=True)
                else: st.info("メンバーデータなし")

# === モード2: 選手検索 ===
elif search_mode == "👤 選手名から探す":
    st.subheader("👤 選手検索")
    q = st.text_input("選手名を入力してください", placeholder="例：松坂大輔、山田脩也")
    
    if q:
        candidates = search_players_list(q)
        if not candidates.empty:
            candidates['label'] = candidates.apply(lambda x: f"{x['Name']} ({x['School_Name_Then']} - {x['Last_Year']}年頃)", axis=1)
            selected_candidate_label = st.selectbox("詳細を見る選手を選択", candidates['label'])
            
            if selected_candidate_label:
                sel_row = candidates[candidates['label'] == selected_candidate_label].iloc[0]
                details = get_player_detail(sel_row['Name'], sel_row['School_Name_Then'])
                
                if not details.empty:
                    profile = details.iloc[-1]
                    
                    st.markdown("---")
                    st.title(f"{profile['Name']}")
                    
                    meta_info = []
                    if 'School_Name_Then' in profile: meta_info.append(f"🏫 {profile['School_Name_Then']}")
                    if 'Birth_Date' in profile and pd.notna(profile['Birth_Date']): meta_info.append(f"🎂 {profile['Birth_Date']}")
                    if 'Prefecture' in profile and pd.notna(profile['Prefecture']): meta_info.append(f"📍 {profile['Prefecture']}")
                    if 'Generation' in profile and pd.notna(profile['Generation']): meta_info.append(f"📅 {profile['Generation']}世代")
                    st.markdown(f"<div class='profile-meta'>{'  |  '.join(meta_info)}</div>", unsafe_allow_html=True)

                    # 🚀 プロ入り情報（緑ボックス）
                    if 'Pro_Team' in profile and pd.notna(profile['Pro_Team']) and profile['Pro_Team'] != '':
                        draft_info = f"{profile.get('Draft_Year', '')}年"
                        rank_info = f"{profile.get('Draft_Rank', '')}位"
                        st.markdown(f"""
                        <div class='pro-box'>
                            🚀 NPB入団: {profile['Pro_Team']} ({draft_info} {rank_info})
                        </div>
                        """, unsafe_allow_html=True)

                    # 🥇 代表経験（金ボックス）- 5つの列をまとめて表示
                    japan_cols = ['U12', 'U15', 'U18', 'U22', 'JAPAN']
                    japan_history = []
                    
                    for col in japan_cols:
                        if col in profile and pd.notna(profile[col]) and str(profile[col]).strip() != '':
                            # 列名と値をセットで表示（例: "U18: アジア選手権"）
                            # 値が "TRUE" や "1" ではなく、大会名などが入っていると想定
                            japan_history.append(f"{col}: {profile[col]}")
                    
                    if japan_history:
                        history_text = " / ".join(japan_history)
                        st.markdown(f"""
                        <div class='japan-box'>
                            🇯🇵 代表経歴: {history_text}
                        </div>
                        """, unsafe_allow_html=True)

                    # 🏟 甲子園成績
                    st.subheader("🏟 甲子園 出場記録")
                    
                    display_cols = ['Year', 'Season', 'Grade', 'Uniform_Number', 'Position', 'Tournament_Rank']
                    if 'Throw_Bat' in details.columns: display_cols.insert(4, 'Throw_Bat')
                    
                    valid_cols = [c for c in display_cols if c in details.columns]
                    st.dataframe(clean_and_rename(details[valid_cols]), use_container_width=True, hide_index=True)
                else: st.error("データなし")
        else: st.warning("該当なし")

# === モード3: 高校検索 ===
elif search_mode == "🏫 高校名から探す":
    st.subheader("🏫 高校検索")
    q = st.text_input("高校名を入力", key="school_q")
    
    if q:
        res = search_schools(q)
        if not res.empty:
            res['label'] = res.apply(lambda x: f"{x['School_Name_Now']} ({x['Prefecture']})", axis=1)
            school_select = st.selectbox("高校を選択", res['label'].unique())
            
            sid = res[res['label'] == school_select].iloc[0]['School_ID']
            st.divider()
            st.markdown(f"### 📜 {school_select} の成績")
            history_df = get_school_history_all(sid)
            st.dataframe(clean_and_rename(history_df), use_container_width=True, hide_index=True)
        else: st.warning("見つかりませんでした")
