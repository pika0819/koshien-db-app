import streamlit as st
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import google.oauth2.service_account

# -----------------------------------------------------------------------------
# 1. 設定 & デザイン (Config & CSS)
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="甲子園DB", 
    layout="wide", 
    page_icon="⚾",
    initial_sidebar_state="expanded"
)

# カスタムCSS（ご提示のデザイン + ボタンのリンク化調整）
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
        color: #333;
    }
    /* 選手名のスタイル */
    .player-name-title {
        font-size: 2.5em;
        font-weight: bold;
        margin-bottom: 5px;
        color: #1b4d3e;
    }
    .player-kana {
        font-size: 0.55em;
        margin-left: 12px;
        font-weight: normal;
        color: #666;
    }
    /* 検索結果などのカード */
    .result-card {
        padding: 15px;
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 10px;
        border-left: 5px solid #c0392b;
        transition: transform 0.2s;
    }
    .result-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    /* Streamlit標準ボタンをリンクっぽく見せるハック（任意） */
    div.stButton > button {
        width: 100%;
        border-radius: 6px;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. BigQuery接続設定 (既存の環境を維持)
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 3. ユーティリティ関数 (整形・遷移・同期)
# -----------------------------------------------------------------------------

def go_to(page, **kwargs):
    """画面遷移用ヘルパー関数"""
    params = {"page": page}
    params.update(kwargs)
    st.query_params.update(params)
    st.rerun()

def clean_and_rename(df):
    """データフレームの列名整理"""
    if df.empty: return df
    drop_cols = ['School_ID', 'ID', 'MatchLink', 'Tournament_ID', 'Region_ID']
    df = df[[c for c in df.columns if c not in drop_cols]]
    
    # Birth_Dateの揺らぎ吸収
    if 'Birth_Date' in df.columns: df = df.rename(columns={'Birth_Date': '生年月日'})
    if 'BirthDate' in df.columns: df = df.rename(columns={'BirthDate': '生年月日'})

    rename_map = {
        'Year': '年度', 'Season': '季節', 'Tournament': '大会名',
        'School_Name_Now': '現在校名', 'School_Name_Then': '当時の校名',
        'District': '地区', 'Prefecture': '都道府県',
        'Uniform_Number': '背番号', 'Name': '氏名', 'Name_Kana': 'フリガナ',
        'Position': '守備', 'Grade': '学年', 'Captain': '主将', 'Pro_Team': 'プロ入団', 
        'Draft_Year': 'ドラフト年', 'Draft_Rank': '順位', 'Throw_Bat': '投打',
        'Generation': '世代', 'Career_Path': '進路', 'Hometown': '出身地',
        'U12': 'U12代表', 'U15': 'U15代表', 'U18': 'U18代表', 'U22': 'U22代表', 'JAPAN': '侍ジャパン',
        'Rank': '成績', 'Win_Loss': '勝敗', 'Score': 'スコア', 'Opponent': '対戦校',
        'Round': '回戦', 'Notes': '備考', 'History_Label': '出場回数'
    }
    return df.rename(columns=rename_map)

def sync_data():
    """データ同期機能"""
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
        client.delete_table(f"{PROJECT_ID}.{APP_DATASET_ID}.{table_name}", not_found_ok=True)
        query = f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{APP_DATASET_ID}.{table_name}` AS SELECT * FROM `{PROJECT_ID}.{RAW_DATASET_ID}.{table_name}`"
        client.query(query).result()
        bar.progress((i + 1) / len(tables))

    st.success("最新のデータ構成で更新が完了しました！")
    st.cache_data.clear()
    st.rerun()

# -----------------------------------------------------------------------------
# 4. データ取得ロジック (Queries)
# -----------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def get_player_detail_full(name, school_then, year):
    """特定の選手の詳細情報を取得"""
    # 年度が重複する同姓同名対策でYearも条件に追加
    sql = f"""
        SELECT * FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player`
        WHERE Name = @name AND School_Name_Then = @school AND Year = @year
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("name", "STRING", name),
        bigquery.ScalarQueryParameter("school", "STRING", school_then),
        bigquery.ScalarQueryParameter("year", "STRING", year)
    ])
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_player_results(school_id, year, season):
    """選手の戦績（チーム成績）を取得"""
    sql = f"""
        SELECT Round, Match_Date, Opponent, Win_Loss, Score, Rank
        FROM `{PROJECT_ID}.{APP_DATASET_ID}.t_results`
        WHERE School_ID = @sid AND Year = @year AND Season = @season
        ORDER BY Match_Date
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("sid", "STRING", school_id),
        bigquery.ScalarQueryParameter("year", "STRING", year),
        bigquery.ScalarQueryParameter("season", "STRING", season)
    ])
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_teammates(school_id, year, season, exclude_name):
    """チームメイトを取得"""
    sql = f"""
        SELECT Name, Position, Uniform_Number, Grade, School_Name_Then, Year, Season
        FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player`
        WHERE School_ID = @sid AND Year = @year AND Season = @season AND Name != @name
        ORDER BY SAFE_CAST(Uniform_Number AS INT64)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("sid", "STRING", school_id),
        bigquery.ScalarQueryParameter("year", "STRING", year),
        bigquery.ScalarQueryParameter("season", "STRING", season),
        bigquery.ScalarQueryParameter("name", "STRING", exclude_name)
    ])
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_generation_stars(generation, exclude_name):
    """同世代の注目選手（ドラフト指名あり）を取得"""
    if not generation: return pd.DataFrame()
    sql = f"""
        SELECT Name, School_Name_Then, Year, Season, Pro_Team
        FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player`
        WHERE Generation = @gen AND Name != @name AND Draft_Year IS NOT NULL
        ORDER BY RAND() LIMIT 10
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("gen", "STRING", generation),
        bigquery.ScalarQueryParameter("name", "STRING", exclude_name)
    ])
    return client.query(sql, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def search_global(keyword):
    """統合検索: 選手と学校をまとめて検索"""
    # 選手検索
    sql_p = f"""
        SELECT 'Player' as Type, Name as Label, School_Name_Then as SubLabel, Year, Season, School_Name_Then as School
        FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_player`
        WHERE Name LIKE @q
        ORDER BY Year DESC LIMIT 20
    """
    # 学校検索
    sql_s = f"""
        SELECT 'School' as Type, School_Name_Now as Label, Prefecture as SubLabel, NULL as Year, NULL as Season, NULL as School
        FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_school`
        WHERE School_Name_Now LIKE @q OR School_Name_Then LIKE @q
        LIMIT 10
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("q", "STRING", f"%{keyword}%")])
    
    df_p = client.query(sql_p, job_config=job_config).to_dataframe()
    df_s = client.query(sql_s, job_config=job_config).to_dataframe()
    return pd.concat([df_p, df_s], ignore_index=True)

@st.cache_data(ttl=3600)
def get_tournaments_list():
    sql = f"SELECT * FROM `{PROJECT_ID}.{APP_DATASET_ID}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC"
    return client.query(sql).to_dataframe().drop_duplicates()

@st.cache_data(ttl=3600)
def load_tournament_details(year, season):
    # 特定大会の全出場校リスト
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("year", "STRING", str(year)),
        bigquery.ScalarQueryParameter("season", "STRING", str(season))
    ])
    sql = f"""
        SELECT tr.School_Name_Then, tr.Rank, tr.School_ID, tr.Win_Loss, tr.History_Label
        FROM `{PROJECT_ID}.{APP_DATASET_ID}.t_results` AS tr 
        WHERE tr.Year = @year AND tr.Season = @season
        ORDER BY 
            CASE WHEN Rank = '優勝' THEN 1 WHEN Rank = '準優勝' THEN 2 WHEN Rank LIKE '%4強%' THEN 3 ELSE 4 END
    """
    return client.query(sql, job_config=job_config).to_dataframe().drop_duplicates()

# -----------------------------------------------------------------------------
# 5. 各画面のビュー関数 (Views)
# -----------------------------------------------------------------------------

def view_home():
    """トップページ（検索 & メニュー）"""
    st.title("⚾ KOSHIEN DATABASE")
    
    # 検索バー
    with st.container():
        col1, col2 = st.columns([3, 1])
        with col1:
            q = st.text_input("選手名・高校名で検索", placeholder="例: 松坂大輔, 大阪桐蔭")
        with col2:
            st.write("") # Spacer
            st.write("") 
            search_btn = st.button("検索", type="primary", use_container_width=True)

    if q or search_btn:
        st.subheader(f"🔍 '{q}' の検索結果")
        results = search_global(q)
        if results.empty:
            st.info("該当データなし")
        else:
            for _, row in results.iterrows():
                # カードクリックのような挙動をボタンで実装
                with st.container():
                    c1, c2 = st.columns([4, 1])
                    with c1:
                        icon = "👤" if row['Type'] == 'Player' else "🏫"
                        label_main = f"**{row['Label']}**"
                        label_sub = f"<small>{row['SubLabel']}</small>"
                        if row['Type'] == 'Player':
                            label_sub += f" <small>({row['Year']} {row['Season']})</small>"
                        
                        st.markdown(f"""
                        <div class="result-card">
                            <span style="font-size:1.2em">{icon} {label_main}</span><br>
                            {label_sub}
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with c2:
                        st.write("")
                        if row['Type'] == 'Player':
                            if st.button("詳細", key=f"btn_{row['Label']}_{row['Year']}_{row['Season']}"):
                                go_to("player", name=row['Label'], school=row['School'], year=row['Year'])
                        else:
                            # 高校詳細は今回は簡易実装（または未実装）
                            if st.button("詳細", key=f"btn_sch_{row['Label']}"):
                                st.toast("高校詳細ページは準備中です") 

    st.divider()
    
    # 大会一覧（アコーディオン）
    st.subheader("🏟 過去の大会を見る")
    df_tourney = get_tournaments_list()
    if not df_tourney.empty:
        years = df_tourney['Year'].unique()
        selected_year = st.selectbox("年度を選択", years)
        
        filtered = df_tourney[df_tourney['Year'] == selected_year]
        for _, row in filtered.iterrows():
            col_t1, col_t2 = st.columns([3, 1])
            with col_t1:
                st.write(f"**{row['Season']}** : {row['Tournament']}")
            with col_t2:
                if st.button("大会結果", key=f"tourney_{row['ID']}"):
                    go_to("tournament", year=row['Year'], season=row['Season'], name=row['Tournament'])

def view_player_detail(name, school_then, year):
    """選手詳細画面"""
    if st.button("← 検索に戻る"):
        go_to("home")

    # データ取得
    df_player = get_player_detail_full(name, school_then, year)
    
    if df_player.empty:
        st.error("選手データが見つかりませんでした。")
        return

    profile = df_player.iloc[0]
    
    # 戦績取得
    df_results = get_player_results(profile['School_ID'], year, profile['Season'])
    
    # --- 表示ロジック（元のデザインを適用） ---
    kana = f"（{profile['Name_Kana']}）" if pd.notna(profile.get('Name_Kana')) else ""
    st.markdown(f"<div class='player-name-title'>{profile['Name']}<span class='player-kana'>{kana}</span></div>", unsafe_allow_html=True)
    
    meta = []
    meta.append(f"🏫 {profile['School_Name_Then']} ({profile['Year']} {profile['Season']})")
    
    bday = profile.get('Birth_Date') or profile.get('BirthDate')
    if pd.notna(bday): meta.append(f"🎂 {bday}生")
    if pd.notna(profile.get('Hometown')): meta.append(f"📍 {profile['Hometown']}出身")
    if pd.notna(profile.get('Generation')): meta.append(f"📅 {profile['Generation']}世代")
    
    st.markdown(f"<div class='profile-meta'>{'　|　'.join(meta)}</div>", unsafe_allow_html=True)

    # プロ入り & 代表情報
    if pd.notna(profile.get('Pro_Team')) and profile['Pro_Team'] != '':
        st.markdown(f"<div class='pro-box'>🚀 NPB入団： {profile['Pro_Team']} （{profile.get('Draft_Year','')}年 {profile.get('Draft_Rank','')}位）</div>", unsafe_allow_html=True)

    japan_h = [f"{c}： {profile[c]}" for c in ['U12', 'U15', 'U18', 'U22', 'JAPAN'] if pd.notna(profile.get(c)) and str(profile[c]).strip() != '']
    if japan_h: st.markdown(f"<div class='japan-box'>🇯🇵 代表経歴： {' ／ '.join(japan_h)}</div>", unsafe_allow_html=True)

    # 戦績テーブル
    st.markdown("### 🏟 甲子園での軌跡")
    if not df_results.empty:
        st.dataframe(clean_and_rename(df_results[['Round', 'Opponent', 'Win_Loss', 'Score']]), hide_index=True)
        final = df_results.iloc[-1]['Rank']
        st.caption(f"最終結果: {final}")

    st.divider()

    # --- つながり機能 ---
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown(f"#### 🤝 {profile['School_Name_Then']} の仲間たち")
        df_team = get_teammates(profile['School_ID'], year, profile['Season'], name)
        if not df_team.empty:
            for _, row in df_team.iterrows():
                # ボタンで遷移
                label = f"{row['Name']} ({row['Position']})"
                if st.button(label, key=f"tm_{row['Name']}_{row['Uniform_Number']}"):
                    go_to("player", name=row['Name'], school=row['School_Name_Then'], year=row['Year'])
        else:
            st.caption("データなし")

    with c2:
        gen = profile.get('Generation')
        if gen:
            st.markdown(f"#### ✨ {gen} のスター選手")
            df_gen = get_generation_stars(gen, name)
            if not df_gen.empty:
                for _, row in df_gen.iterrows():
                    label = f"{row['Name']} ({row['School_Name_Then']})"
                    if st.button(label, key=f"gen_{row['Name']}_{row['School_Name_Then']}"):
                        go_to("player", name=row['Name'], school=row['School_Name_Then'], year=row['Year'])
            else:
                st.caption("他データなし")

def view_tournament_detail(year, season, name):
    """大会詳細画面"""
    if st.button("← トップに戻る"):
        go_to("home")
    
    st.header(f"{year} {season} - {name}")
    
    df_list = load_tournament_details(year, season)
    
    if not df_list.empty:
        st.markdown("#### 出場校一覧")
        # 出場校をクリックするとその学校のメンバー表などの詳細へ行けるように拡張可能
        # 今回は簡易的にリスト表示
        st.dataframe(clean_and_rename(df_list), use_container_width=True, hide_index=True)
    else:
        st.info("データがありません")

# -----------------------------------------------------------------------------
# 6. メインルーティング (Main Router)
# -----------------------------------------------------------------------------
def main():
    # サイドバー（管理者用機能）
    st.sidebar.title("KOSHIEN DB")
    if st.sidebar.button("🏠 ホームへ戻る"):
        go_to("home")
    
    st.sidebar.markdown("---")
    st.sidebar.caption("管理者メニュー")
    if st.sidebar.button("🔄 データを最新に同期"):
        with st.spinner("同期中..."): 
            sync_data()

    # URLパラメータによるルーティング
    params = st.query_params
    page = params.get("page", "home")

    if page == "home":
        view_home()
    
    elif page == "player":
        # 必須パラメータ取得
        p_name = params.get("name")
        p_school = params.get("school")
        p_year = params.get("year")
        
        if p_name and p_school and p_year:
            view_player_detail(p_name, p_school, p_year)
        else:
            st.error("パラメータ不足")
            if st.button("戻る"): go_to("home")

    elif page == "tournament":
        t_year = params.get("year")
        t_season = params.get("season")
        t_name = params.get("name")
        view_tournament_detail(t_year, t_season, t_name)

if __name__ == "__main__":
    main()
