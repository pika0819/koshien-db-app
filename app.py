import streamlit as st
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import google.oauth2.service_account

# --- ページ設定 ---
st.set_page_config(page_title="甲子園DB", layout="wide", page_icon="⚾")
st.title("⚾️ 甲子園DB")

# --- デザインCSS ---
st.markdown("""
<style>
    /* プロ入り情報：落ち着いたグリーン */
    .pro-box {
        padding: 15px; border-radius: 8px; 
        background-color: #2F5C45; 
        color: white;
        margin-bottom: 10px; font-weight: bold; border: 1px solid #448060;
    }
    /* 代表経歴：侍ジャパンネイビー × ゴールド文字 */
    .japan-box {
        padding: 15px; border-radius: 8px; 
        background-color: #0F1C3F; 
        color: #D4AF37; 
        margin-bottom: 10px; font-weight: bold; 
        border: 1px solid #D4AF37;
    }
    /* プロフィール詳細：読みやすい明るいグレー */
    .profile-meta {
        font-size: 1.0em; 
        margin-bottom: 20px;
    }
    /* 選手名のスタイル */
    .player-name-title {
        font-size: 2.5em;
        font-weight: bold;
        margin-bottom: 5px;
    }
    .player-kana {
        font-size: 0.55em;
        margin-left: 12px;
        font-weight: normal;
    }
    /* リンクボタンのテキスト太字 */
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
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)

    tables = ["m_tournament", "m_school", "m_player", "t_results", "t_scores", "m_region"]
    for i, table_name in enumerate(tables):
        status_text.text(f"同期中： {table_name}...")
        query = f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{APP_DATASET_ID}.{table_name}` AS SELECT * FROM `{PROJECT_ID}.{RAW_DATASET_ID}.{table_name}`"
        client.query(query).result()
        bar.progress((i + 1) / len(tables))

    st.success("データ更新完了")
    st.cache_data.clear()
    st.rerun()

# --- 3. データ取得・整形関数 ---

def clean_and_rename(df):
    if df.empty: return df
    drop_cols = ['School_ID', 'ID', 'MatchLink', 'Tournament_ID', 'Region_ID']
    df = df[[c for c in df.columns if c not in drop_cols]]
    rename_map = {
        'Year': '年度', 'Season': '季節', 'Tournament': '大会名',
        'School_Name_Now': '現在校名', 'School_Name_Then': '当時の校名',
        'District': '地区', 'Prefecture': '都道府県',
        'Uniform_Number': '背番号', 'Name': '氏名', 'Name_Kana': 'フリガナ',
        'Position': '守備', 'Grade': '学年', 'Captain': '主将', 'Pro_Team': 'プロ入団', 
        'Draft_Year': 'ドラフト年', 'Draft_Rank': '順位', 'Throw_Bat': '投打',
        'BirthDate': '生年月日', 'Generation': '世代', 'Career_Path': '進路',
        'U12': 'U12代表', 'U15': 'U15代表', 'U18': 'U18代表', 'U22': 'U22代表', 'JAPAN': '侍ジャパン',
        'Rank': '成績', 'Win_Loss': '勝敗', 'Score': 'スコア', 'Opponent': '対戦校',
        'Round': '回戦', 'Notes': '備考', 'History_Label': '出場回数'
    }
    return df.rename(columns=rename_map)

@st.cache_data(ttl=3600)
def get_tournaments():
    try:
        sql = "SELECT * FROM `{}.{}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC".format(PROJECT_ID, APP_DATASET_ID)
        return client.query(sql).to_dataframe().drop_duplicates()
    except: return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_tournament_details(year, season):
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("year", "STRING", str(year)),
        bigquery.ScalarQueryParameter("season", "STRING", str(season))
    ])
    sql_list = f"SELECT tr.District, tr.School_Name_Then, s.School_Name_Now, tr.History_Label, tr.Rank, tr.School_ID FROM `{PROJECT_ID}.{APP_DATASET_ID}.t_results` AS tr LEFT JOIN `{PROJECT_ID}.{APP_DATASET_ID}.m_school` AS s ON tr.School_ID = s.School_ID WHERE tr.Year = @year AND tr.Season = @season"
    sql_scores = f"SELECT * FROM `{PROJECT_ID}.{APP_DATASET_ID}.t_scores` WHERE Year = @year AND Season = @season"
    sql_members = f"SELECT * FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player` WHERE Year = @year AND Season = @season"
    return {
        "list": client.query(sql_list, job_config=job_config).to_dataframe().drop_duplicates(),
        "scores": client.query(sql_scores, job_config=job_config).to_dataframe().drop_duplicates(),
        "members": client.query(sql_members, job_config=job_config).to_dataframe().drop_duplicates()
    }

@st.cache_data(ttl=3600)
def search_players_list(query_text):
    sql = f"SELECT Name, MAX(Name_Kana) as Name_Kana, School_Name_Then, MAX(Year) as Last_Year, MAX(Generation) as Generation FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player` WHERE Name LIKE @q GROUP BY Name, School_Name_Then ORDER BY Last_Year DESC LIMIT 50"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("q", "STRING", f"%{query_text}%")])
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_player_detail(name, school_then):
    sql = f"SELECT p.*, tr.Rank as Tournament_Rank FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player` AS p LEFT JOIN `{PROJECT_ID}.{APP_DATASET_ID}.t_results` AS tr ON p.School_ID = tr.School_ID AND p.Year = tr.Year AND p.Season = tr.Season WHERE p.Name = @name AND p.School_Name_Then = @school_then ORDER BY SAFE_CAST(p.Year AS INT64), p.Season"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("name", "STRING", name), bigquery.ScalarQueryParameter("school_then", "STRING", school_then)])
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def search_schools(query_text):
    sql = f"SELECT DISTINCT School_ID, School_Name_Now, Prefecture, School_Name_Then FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_school` WHERE School_Name_Now LIKE @q OR School_Name_Then LIKE @q LIMIT 50"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("q", "STRING", f"%{query_text}%")])
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_school_history_all(school_id):
    sql = f"SELECT Year, Season, Tournament, School_Name_Then, Rank FROM `{PROJECT_ID}.{APP_DATASET_ID}.t_results` WHERE School_ID = @school_id ORDER BY SAFE_CAST(Year AS INT64) DESC"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("school_id", "STRING", school_id)])
    return client.query(sql, job_config=job_config).to_dataframe()

# --- 4. UI構築 ---

st.sidebar.header("🔍 検索モード")
search_mode = st.sidebar.radio("", ["🏟 大会から探す", "👤 選手名から探す", "🏫 高校名から探す"])
st.sidebar.markdown("---")
if st.sidebar.button("🔄 データを最新に更新"):
    with st.spinner("同期中..."): sync_data()

# === モード1： 大会検索 ===
if search_mode == "🏟 大会から探す":
    df_tourney = get_tournaments()
    if df_tourney.empty:
        st.info("左下の更新ボタンを押してデータを読み込んでください。")
        st.stop()
    df_tourney = df_tourney.fillna('')
    tourney_map = {f"{r['Year']} {r['Season']} － {r['Tournament']}": {"year": r['Year'], "season": r['Season'], "name": r['Tournament'], "l1": r.get('Year_Link',''), "l2": r.get('History_Link',''), "l3": r.get('Virtual_Koshien_Link','')} for _, r in df_tourney.iterrows()}
    
    selected_label = st.sidebar.selectbox("大会を選択", list(tourney_map.keys()))
    sel = tourney_map[selected_label]
    st.header(selected_label)
    
    links = [("🔗 組み合わせ表", sel["l1"]), ("🏛 甲子園歴史館", sel["l2"]), ("📺 バーチャル高校野球", sel["l3"])]
    valid_links = [(t, u) for t, u in links if u and str(u).startswith("http")]
    if valid_links:
        cols = st.columns(len(valid_links))
        for i, (t, u) in enumerate(valid_links): cols[i].link_button(t, u)
    
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
            t1, t2 = st.tabs(["⚾️ 戦績・スコア", "👥 登録メンバー"])
            with t1:
                df_s = data["scores"][data["scores"]['School_ID'] == sid]
                if not df_s.empty: st.dataframe(clean_and_rename(df_s[['Round', 'Opponent', 'Win_Loss', 'Score', 'Notes']]), use_container_width=True, hide_index=True)
                else: st.info("データなし")
            with t2:
                df_m = data["members"][data["members"]['School_ID'] == sid]
                if not df_m.empty:
                    df_m = df_m.sort_values('Uniform_Number', key=lambda x: pd.to_numeric(x, errors='coerce'))
                    st.dataframe(clean_and_rename(df_m[['Uniform_Number', 'Position', 'Name', 'Name_Kana', 'Grade', 'Captain', 'Pro_Team']]), use_container_width=True, hide_index=True)
                else: st.info("データなし")

# === モード2： 選手検索 ===
elif search_mode == "👤 選手名から探す":
    st.subheader("👤 選手検索")
    q = st.text_input("選手名を入力してください", placeholder="例：松坂大輔、宮下朝陽")
    if q:
        candidates = search_players_list(q)
        if not candidates.empty:
            candidates['label'] = candidates.apply(lambda r: f"{r['Name']} （{r['School_Name_Then']} － {r['Generation'] if pd.notna(r['Generation']) else r['Last_Year']}世代）", axis=1)
            selected_candidate_label = st.selectbox("詳細を見る選手を選択", candidates['label'])
            if selected_candidate_label:
                sel_row = candidates[candidates['label'] == selected_candidate_label].iloc[0]
                details = get_player_detail(sel_row['Name'], sel_row['School_Name_Then'])
                if not details.empty:
                    profile = details.iloc[-1]
                    kana = f"（{profile['Name_Kana']}）" if pd.notna(profile.get('Name_Kana')) else ""
                    st.markdown(f"<div class='player-name-title'>{profile['Name']}<span class='player-kana'>{kana}</span></div>", unsafe_allow_html=True)
                    
                    meta = []
                    if 'School_Name_Then' in profile: meta.append(f"🏫 {profile['School_Name_Then']}")
                    if pd.notna(profile.get('BirthDate')): meta.append(f"🎂 {profile['BirthDate']}生")
                    if pd.notna(profile.get('Hometown')): meta.append(f"📍 {profile['Hometown']}出身")
                    if pd.notna(profile.get('Generation')): meta.append(f"📅 {profile['Generation']}世代")
                    if pd.notna(profile.get('Career_Path')): meta.append(f"👣 進路： {profile['Career_Path']}")
                    st.markdown(f"<div class='profile-meta'>{'　|　'.join(meta)}</div>", unsafe_allow_html=True)

                    if pd.notna(profile.get('Pro_Team')) and profile['Pro_Team'] != '':
                        st.markdown(f"<div class='pro-box'>🚀 NPB入団： {profile['Pro_Team']} （{profile.get('Draft_Year','')}年 {profile.get('Draft_Rank','')}位）</div>", unsafe_allow_html=True)

                    japan_h = [f"{c}： {profile[c]}" for c in ['U12', 'U15', 'U18', 'U22', 'JAPAN'] if pd.notna(profile.get(c)) and str(profile[c]).strip() != '']
                    if japan_h: st.markdown(f"<div class='japan-box'>🇯🇵 代表経歴： {' ／ '.join(japan_h)}</div>", unsafe_allow_html=True)

                    st.subheader("🏟 甲子園 出場記録")
                    cols = ['Year', 'Season', 'Grade', 'Uniform_Number', 'Position', 'Throw_Bat', 'Tournament_Rank']
                    st.dataframe(clean_and_rename(details[[c for c in cols if c in details.columns]]), use_container_width=True, hide_index=True)
                else: st.error("データなし")

# === モード3： 高校検索 ===
elif search_mode == "🏫 高校名から探す":
    st.subheader("🏫 高校検索")
    q = st.text_input("高校名を入力")
    if q:
        res = search_schools(q)
        if not res.empty:
            res['label'] = res.apply(lambda x: f"{x['School_Name_Now']} （{x['Prefecture']}）", axis=1)
            school_select = st.selectbox("高校を選択", res['label'].unique())
            sid = res[res['label'] == school_select].iloc[0]['School_ID']
            st.divider()
            st.markdown(f"### 📜 {school_select} の成績")
            st.dataframe(clean_and_rename(get_school_history_all(sid)), use_container_width=True, hide_index=True)
