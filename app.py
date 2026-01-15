import streamlit as st
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import google.oauth2.service_account

# --- ページ設定 ---
st.set_page_config(page_title="甲子園DB", layout="wide", page_icon="⚾")

# --- クエリパラメータによる状態管理（戻るボタン対応） ---
params = st.query_params
initial_mode = params.get("mode", "top")
initial_q = params.get("q", "")
initial_year = params.get("year", "")
initial_season = params.get("season", "")

# --- デザインCSS ---
st.markdown("""
<style>
    .pro-box { padding: 15px; border-radius: 8px; background-color: #2F5C45; color: white; margin-bottom: 10px; font-weight: bold; border: 1px solid #448060; }
    .japan-box { padding: 15px; border-radius: 8px; background-color: #0F1C3F; color: #D4AF37; margin-bottom: 10px; font-weight: bold; border: 1px solid #D4AF37; }
    .tag-link { background-color: #333; color: #ddd !important; padding: 4px 10px; border-radius: 15px; font-size: 0.9em; margin-right: 8px; border: 1px solid #555; display: inline-block; transition: all 0.2s; cursor: pointer; }
    .tag-link:hover { background-color: #555; border-color: #888; color: white !important; text-decoration: none; }
    .player-name-title { font-size: 2.5em; font-weight: bold; margin-bottom: 5px; }
    .player-kana { font-size: 0.55em; color: #bbbbbb; margin-left: 12px; font-weight: normal; }
    div[data-testid="stLinkButton"] p { font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# --- 1. BigQuery接続設定 ---
@st.cache_resource
def get_bq_client():
    try:
        scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception as e:
        st.error(f"認証エラー: {e}"); st.stop()

client = get_bq_client()
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
APP_DATASET_ID = "koshien_app"

# --- 2. データ取得関数群 ---

def clean_and_rename(df):
    if df.empty: return df
    drop_cols = ['School_ID', 'ID', 'MatchLink', 'Tournament_ID', 'Region_ID']
    df = df[[c for c in df.columns if c not in drop_cols]]
    rename_map = {
        'Year': '年度', 'Season': '季節', 'Tournament': '大会名', 'School_Name_Now': '現在校名', 'School_Name_Then': '当時の校名',
        'District': '地区', 'Prefecture': '都道府県', 'Uniform_Number': '背番号', 'Name': '氏名', 'Name_Kana': 'フリガナ',
        'Position': '守備', 'Grade': '学年', 'Captain': '主将', 'Pro_Team': 'プロ入団', 'Draft_Year': 'ドラフト年', 'Draft_Rank': '順位', 
        'Throw_Bat': '投打', 'BirthDate': '生年月日', 'Generation': '世代', 'Career_Path': '進路', 'Hometown': '出身地',
        'U12': 'U12', 'U15': 'U15', 'U18': 'U18', 'U22': 'U22', 'JAPAN': 'JAPAN', 'Rank': '成績', 'Win_Loss': '勝敗', 
        'Score': 'スコア', 'Opponent': '対戦校', 'Round': '回戦', 'Notes': '備考', 'History_Label': '出場回数'
    }
    return df.rename(columns=rename_map)

@st.cache_data(ttl=3600)
def get_tournaments():
    sql = f"SELECT * FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC"
    return client.query(sql).to_dataframe().drop_duplicates()

@st.cache_data(ttl=3600)
def load_tournament_details(year, season):
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("year", "STRING", str(year)), bigquery.ScalarQueryParameter("season", "STRING", str(season))])
    sql_list = f"SELECT tr.District, tr.School_Name_Then, s.School_Name_Now, tr.History_Label, tr.Rank, tr.School_ID FROM `{PROJECT_ID}.{APP_DATASET_ID}.t_results` AS tr LEFT JOIN `{PROJECT_ID}.{APP_DATASET_ID}.m_school` AS s ON tr.School_ID = s.School_ID WHERE tr.Year = @year AND tr.Season = @season"
    sql_scores = f"SELECT * FROM `{PROJECT_ID}.{APP_DATASET_ID}.t_scores` WHERE Year = @year AND Season = @season"
    sql_members = f"SELECT * FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player` WHERE Year = @year AND Season = @season"
    return {"list": client.query(sql_list, job_config=job_config).to_dataframe(), "scores": client.query(sql_scores, job_config=job_config).to_dataframe(), "members": client.query(sql_members, job_config=job_config).to_dataframe()}

@st.cache_data(ttl=3600)
def search_players_smart(q):
    sql = f"""SELECT Name, School_Name_Then, MAX(Year) as Last_Year, MAX(Generation) as Generation FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player` 
              WHERE Name LIKE @q OR School_Name_Then LIKE @q OR Hometown LIKE @q OR Career_Path LIKE @q OR CAST(Generation AS STRING) LIKE @q
              GROUP BY Name, School_Name_Then ORDER BY Last_Year DESC LIMIT 50"""
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("q", "STRING", f"%{q}%")])
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_player_detail_full(name, school_then):
    sql = f"SELECT p.*, tr.Rank as Tournament_Rank FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player` AS p LEFT JOIN `{PROJECT_ID}.{APP_DATASET_ID}.t_results` AS tr ON p.School_ID = tr.School_ID AND p.Year = tr.Year AND p.Season = tr.Season WHERE p.Name = @name AND p.School_Name_Then = @school_then ORDER BY SAFE_CAST(p.Year AS INT64), p.Season"
    return client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("name", "STRING", name), bigquery.ScalarQueryParameter("school_then", "STRING", school_then)])).to_dataframe()

@st.cache_data(ttl=3600)
def get_school_alumni(school_id):
    sql = f"SELECT DISTINCT Name, Year, Draft_Year, Pro_Team FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player` WHERE School_ID = @sid AND Pro_Team IS NOT NULL AND Pro_Team != '' ORDER BY Draft_Year DESC"
    return client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("sid", "STRING", school_id)])).to_dataframe()

# --- 3. UI構築 ---

st.title("⚾️ 甲子園DB")

# サイドバー設定
st.sidebar.header("🔍 検索モード")
search_mode = st.sidebar.radio("", ["🏟 大会から探す", "👤 選手・条件検索", "🏫 高校名から探す"])

# =========================================================
# モード1： 大会検索
# =========================================================
if search_mode == "🏟 大会から探す":
    df_t = get_tournaments()
    # 要望：ホーム画面に検索バー
    t_list = [f"{r['Year']} {r['Season']} － {r['Tournament']}" for _, r in df_t.iterrows()]
    selected_label = st.selectbox("大会名を入力または選択してください", t_list, index=0 if not initial_year else t_list.index(next(s for s in t_list if initial_year in s)))
    
    sel_r = df_t[df_t.apply(lambda r: f"{r['Year']} {r['Season']} － {r['Tournament']}" == selected_label, axis=1)].iloc[0]
    st.header(selected_label)
    
    # リンクボタン
    links = [("🔗 組み合わせ表", sel_r.get('Year_Link')), ("🏛 甲子園歴史館", sel_r.get('History_Link')), ("📺 バーチャル高校野球", sel_r.get('Virtual_Koshien_Link'))]
    cols = st.columns(len([l for l in links if l[1]]))
    for i, (t, u) in enumerate([l for l in links if l[1]]): cols[i].link_button(t, u)

    st.divider()
    with st.spinner("展開中..."):
        data = load_tournament_details(sel_r["Year"], sel_r["Season"])
        df_list = data["list"]

    if not df_list.empty:
        # 要望：チェックボックスで詳細データ
        st.write("一覧から高校を選択すると下に詳細が表示されます")
        event = st.dataframe(clean_and_rename(df_list), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single")
        
        if event.selection.rows:
            selected_idx = event.selection.rows[0]
            sid = df_list.iloc[selected_idx]['School_ID']
            sname = df_list.iloc[selected_idx]['School_Name_Then']
            
            st.subheader(f"🏫 {sname} の詳細")
            t1, t2 = st.tabs(["⚾️ 戦績", "👥 メンバー"])
            with t1:
                df_s = data["scores"][data["scores"]['School_ID'] == sid]
                st.dataframe(clean_and_rename(df_s[['Round', 'Opponent', 'Win_Loss', 'Score', 'Notes']]) if not df_s.empty else "なし", use_container_width=True, hide_index=True)
            with t2:
                df_m = data["members"][data["members"]['School_ID'] == sid].sort_values('Uniform_Number', key=lambda x: pd.to_numeric(x, errors='coerce'))
                st.dataframe(clean_and_rename(df_m[['Uniform_Number', 'Position', 'Name', 'Name_Kana', 'Grade', 'Captain', 'Pro_Team']]) if not df_m.empty else "なし", use_container_width=True, hide_index=True)

# =========================================================
# モード2： 選手・条件検索（要望：ドリルダウン機能）
# =========================================================
elif search_mode == "👤 選手・条件検索":
    st.subheader("👤 選手名・条件（出身地、世代、進路等）で検索")
    q = st.text_input("検索ワード", value=initial_q, placeholder="例：松坂大輔、大阪、1998世代、明治大")
    
    if q:
        st.query_params["q"] = q
        candidates = search_players_smart(q)
        if not candidates.empty:
            candidates['label'] = candidates.apply(lambda r: f"{r['Name']} （{r['School_Name_Then']} － {r['Generation'] if pd.notna(r['Generation']) else r['Last_Year']}世代）", axis=1)
            sel_p_label = st.selectbox("詳細を見る選手を選択", candidates['label'])
            
            if sel_p_label:
                p_row = candidates[candidates['label'] == sel_p_label].iloc[0]
                details = get_player_detail_full(p_row['Name'], p_row['School_Name_Then'])
                if not details.empty:
                    profile = details.iloc[-1]
                    st.markdown(f"<div class='player-name-title'>{profile['Name']}<span class='player-kana'>（{profile.get('Name_Kana','')}）</span></div>", unsafe_allow_html=True)
                    
                    # 要望：クリッカブルなタグ（ドリルダウン）
                    meta_tags = []
                    def tag(l, v): return f"<a href='./?mode=player&q={v}' target='_self' class='tag-link'>{l} {v}</a>" if pd.notna(v) else ""
                    
                    st.markdown(f"""
                        <div style='margin-bottom:15px;'>
                            {tag("🏫", profile['School_Name_Then'])} {tag("📍", profile.get('Hometown'))} 
                            {tag("📅", f"{profile['Generation']}世代" if pd.notna(profile.get('Generation')) else None)} {tag("👣", profile.get('Career_Path'))}
                        </div>
                    """, unsafe_allow_html=True)

                    if pd.notna(profile.get('Pro_Team')) and profile['Pro_Team'] != '':
                        st.markdown(f"<div class='pro-box'>🚀 NPB入団： {profile['Pro_Team']} （{profile.get('Draft_Year','')}年 {profile.get('Draft_Rank','')}位）</div>", unsafe_allow_html=True)

                    st.subheader("🏟 甲子園 出場記録")
                    # 要望：年度の横にチェックリスト（リンク）
                    df_d = clean_and_rename(details)
                    df_d['大会詳細'] = details.apply(lambda r: f"./?mode=tournament&year={r['Year']}&season={r['Season']}", axis=1)
                    cols = ['大会詳細', '年度', '季節', '学年', '背番号', '守備', '成績']
                    st.dataframe(df_d[[c for c in cols if c in df_d.columns]], use_container_width=True, hide_index=True, column_config={"大会詳細": st.column_config.LinkColumn("🔗", display_text="移動")})

# =========================================================
# モード3： 高校検索（要望：卒業生表示）
# =========================================================
elif search_mode == "🏫 高校名から探す":
    st.subheader("🏫 高校検索")
    q_s = st.text_input("高校名を入力")
    if q_s:
        res = search_schools(q_s)
        if not res.empty:
            res['label'] = res.apply(lambda x: f"{x['School_Name_Now']} （{x['Prefecture']}）", axis=1)
            s_sel = st.selectbox("高校を選択", res['label'].unique())
            sid = res[res['label'] == s_sel].iloc[0]['School_ID']
            
            tab_h1, tab_h2 = st.tabs(["📜 甲子園成績", "🌟 プロ入り卒業生"])
            with tab_h1:
                st.dataframe(clean_and_rename(get_school_history_all(sid)), use_container_width=True, hide_index=True)
            with tab_h2:
                # 要望：卒業生も出てきてほしい
                df_alumni = get_school_alumni(sid)
                if not df_alumni.empty:
                    st.dataframe(df_alumni.rename(columns={'Name':'氏名','Year':'最終出場年','Draft_Year':'ドラフト年度','Pro_Team':'入団球団'}), use_container_width=True, hide_index=True)
                else: st.info("プロ入り卒業生のデータはありません")
