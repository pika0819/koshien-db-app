import streamlit as st
import pandas as pd
from google.cloud import bigquery

# -----------------------------------------------------------------------------
# 1. 設定 & デザイン (Config & CSS)
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="KOSHIEN DB - 高校野球データベース",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS: 甲子園カラー (白, 深緑, 土色) と カードデザイン
st.markdown("""
<style>
    /* 全体のフォントと背景 */
    .stApp {
        background-color: #f9f9f9;
        font-family: 'Helvetica Neue', Arial, 'Hiragino Kaku Gothic ProN', sans-serif;
    }
    
    /* ヘッダーの装飾 */
    h1, h2, h3 {
        color: #1b4d3e; /* 甲子園のフェンス色 */
        font-weight: 700;
    }
    
    /* カード風コンテナ */
    .info-card {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 20px;
        border-left: 5px solid #c0392b; /* アンツーカー色 */
    }
    
    /* 選手情報のタグ */
    .tag {
        display: inline-block;
        padding: 4px 12px;
        margin: 4px 2px;
        background-color: #e8f5e9;
        color: #1b4d3e;
        border-radius: 15px;
        font-size: 0.85em;
        font-weight: bold;
        border: 1px solid #1b4d3e;
    }
    
    /* 特別なタグ（ドラフトなど） */
    .tag-highlight {
        background-color: #fff3e0;
        color: #e67e22;
        border: 1px solid #e67e22;
    }

    /* リンクボタンのスタイル調整 */
    .stButton button {
        width: 100%;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. データ接続 & 取得ロジック (BigQuery Connection)
# -----------------------------------------------------------------------------
# キャッシュを使ってAPIコール数を節約
@st.cache_data(ttl=3600)
def run_query(query):
    try:
        # st.connectionを使用してBigQueryに接続
        # secrets.tomlの設定が必要です
        conn = st.connection('bigquery', type='sql')
        return conn.query(query).to_df()
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# 3. 画面コンポーネント (Views)
# -----------------------------------------------------------------------------

def show_player_detail(name, school, year, season):
    """選手詳細画面: 詳細情報と『つながり』を表示"""
    
    # 3-1. 選手情報の取得
    sql_player = f"""
        SELECT * FROM `koshien_app.m_player`
        WHERE Name = '{name}' 
          AND School_Name_Then = '{school}' 
          AND Year = '{year}'
          AND Season = '{season}'
        LIMIT 1
    """
    df_player = run_query(sql_player)

    if df_player.empty:
        st.error("選手データが見つかりませんでした。")
        if st.button("トップに戻る"):
            st.query_params.clear()
            st.rerun()
        return

    player = df_player.iloc[0]

    # 3-2. 戦績情報の取得 (学校・年度・季節で結合)
    sql_results = f"""
        SELECT Round, Match_Date, Opponent, Win_Loss, Score, Rank
        FROM `koshien_app.t_results`
        WHERE School_Name_Then = '{school}'
          AND Year = '{year}'
          AND Season = '{season}'
        ORDER BY Match_Date
    """
    df_results = run_query(sql_results)

    # --- UI 構築 ---
    
    # ナビゲーション
    if st.button("← 検索に戻る", key="back_btn"):
        st.query_params.clear()
        st.rerun()

    # ヘッダーエリア
    st.markdown(f"## {player['Name']} <small>({player['Name_Kana']})</small>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # 基本情報カード
        st.markdown(f"""
        <div class="info-card">
            <h3>⚾ 基本プロフィール</h3>
            <p>
                <b>所属:</b> {player['School_Name_Then']} ({player['Year']}年 {player['Season']})<br>
                <b>ポジション:</b> {player['Position']} (背番号 {player['Uniform_Number']})<br>
                <b>学年:</b> {player['Grade']}　<b>投打:</b> {player['Throw_Bat']}<br>
                <b>出身:</b> {player['Hometown']} ({player['BirthDate']})
            </p>
        </div>
        """, unsafe_allow_html=True)

        # マニアック情報タグ
        tags = []
        if player.get('Captain') == '主将': tags.append('<span class="tag">主将</span>')
        if player.get('U18'): tags.append(f'<span class="tag tag-highlight">U-18代表</span>')
        if player.get('Draft_Rank'): tags.append(f'<span class="tag tag-highlight">{player["Draft_Year"]}年 ドラフト{player["Draft_Rank"]}位 ({player["Pro_Team"]})</span>')
        if player.get('Generation'): tags.append(f'<span class="tag">{player["Generation"]}</span>')
        
        st.markdown(" ".join(tags), unsafe_allow_html=True)
        
        # 進路情報
        if player.get('Career_Path'):
            st.info(f"🎓 進路・経歴: {player['Career_Path']}")

    with col2:
        # その大会の戦績
        st.markdown("### 🏆 大会戦績")
        if not df_results.empty:
            st.dataframe(
                df_results[['Round', 'Opponent', 'Win_Loss', 'Score']],
                hide_index=True,
                use_container_width=True
            )
            final_rank = df_results.iloc[-1]['Rank']
            st.metric("最終結果", final_rank)
        else:
            st.write("戦績データなし")

    # --- つながり (Connections) エリア ---
    st.markdown("---")
    st.subheader("🔗 つながる球児たち")
    
    c_col1, c_col2 = st.columns(2)

    with c_col1:
        st.markdown("#### 🤝 同チーム (チームメイト)")
        # 同じ学校・年度・季節の選手
        sql_teammates = f"""
            SELECT Name, Position, Uniform_Number, Grade
            FROM `koshien_app.m_player`
            WHERE School_Name_Then = '{school}' 
              AND Year = '{year}' 
              AND Season = '{season}'
              AND Name != '{name}'
            ORDER BY CAST(Uniform_Number AS INT64)
        """
        df_team = run_query(sql_teammates)
        
        for _, row in df_team.iterrows():
            if st.button(f"{row['Name']} ({row['Position']})", key=f"tm_{row['Name']}"):
                st.query_params.update({"page": "player", "name": row['Name'], "school": school, "year": year, "season": season})
                st.rerun()

    with c_col2:
        generation_name = player.get('Generation')
        if generation_name:
            st.markdown(f"#### ✨ {generation_name} (同世代の注目選手)")
            # 同じ世代かつ、ドラフト指名された選手などをピックアップ（ランダムまたは注目度順）
            sql_gen = f"""
                SELECT Name, School_Name_Then, Year, Season
                FROM `koshien_app.m_player`
                WHERE Generation = '{generation_name}'
                  AND Name != '{name}'
                  AND Draft_Rank IS NOT NULL  -- 注目選手としてドラフト指名選手を表示
                LIMIT 10
            """
            df_gen = run_query(sql_gen)
            
            for _, row in df_gen.iterrows():
                label = f"{row['Name']} ({row['School_Name_Then']})"
                if st.button(label, key=f"gen_{row['Name']}_{row['School_Name_Then']}"):
                    st.query_params.update({"page": "player", "name": row['Name'], "school": row['School_Name_Then'], "year": row['Year'], "season": row['Season']})
                    st.rerun()
        else:
            st.write("世代データなし")


def show_search_page():
    """トップページ: 検索とピックアップ"""
    
    st.title("⚾ KOSHIEN DATABASE")
    st.caption("13年の情熱が詰まった、高校野球（甲子園）記録アーカイブ")

    # 検索フィルター
    with st.expander("🔍 選手・学校を検索", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            search_name = st.text_input("選手名 (一部でも可)")
        with col2:
            search_school = st.text_input("高校名")
        with col3:
            # データベースから存在する年代を取得しても良いが、ここでは簡易的にリスト化
            years = [str(y) for y in range(2024, 1914, -1)]
            search_year = st.selectbox("年度", ["指定なし"] + years)

        search_btn = st.button("検索する", type="primary")

    # 検索実行ロジック
    if search_btn:
        conditions = []
        if search_name: conditions.append(f"Name LIKE '%{search_name}%'")
        if search_school: conditions.append(f"School_Name_Then LIKE '%{search_school}%'")
        if search_year != "指定なし": conditions.append(f"Year = '{search_year}'")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        limit = 50 if conditions else 10 # 条件なしなら少なめに

        # 結果表示用クエリ
        sql = f"""
            SELECT Name, School_Name_Then, Year, Season, Position, Generation
            FROM `koshien_app.m_player`
            WHERE {where_clause}
            ORDER BY Year DESC, School_Name_Then
            LIMIT {limit}
        """
        
        st.markdown("### 検索結果")
        df_results = run_query(sql)
        
        if df_results.empty:
            st.warning("該当するデータが見つかりませんでした。条件を変えて検索してください。")
        else:
            # カード形式でリスト表示
            for _, row in df_results.iterrows():
                with st.container():
                    c1, c2 = st.columns([3, 1])
                    with c1:
                        st.markdown(f"**{row['Name']}** <small>{row['School_Name_Then']} ({row['Year']} {row['Season']})</small>", unsafe_allow_html=True)
                        if row['Generation']:
                            st.caption(f"世代: {row['Generation']}")
                    with c2:
                        if st.button("詳細", key=f"btn_{row['Name']}_{row['Year']}"):
                            # URLパラメータをセットしてリロード
                            st.query_params.update({
                                "page": "player",
                                "name": row['Name'],
                                "school": row['School_Name_Then'],
                                "year": row['Year'],
                                "season": row['Season']
                            })
                            st.rerun()
                    st.divider()

    else:
        # 初期表示: 例えば「最近のドラフト指名選手」などを表示してワクワクさせる
        st.markdown("### 🌟 ピックアップ：プロ入りした球児たち")
        sql_pickup = """
            SELECT Name, School_Name_Then, Year, Season, Pro_Team
            FROM `koshien_app.m_player`
            WHERE Draft_Year IS NOT NULL
            ORDER BY RAND()
            LIMIT 6
        """
        df_pickup = run_query(sql_pickup)
        
        cols = st.columns(3)
        for i, row in df_pickup.iterrows():
            with cols[i % 3]:
                st.markdown(f"""
                <div class="info-card" style="border-left: 5px solid #1b4d3e;">
                    <b>{row['Name']}</b><br>
                    <small>{row['School_Name_Then']}</small><br>
                    <span style="color:#e67e22;">➡ {row['Pro_Team']}</span>
                </div>
                """, unsafe_allow_html=True)
                if st.button("詳細を見る", key=f"pick_{i}"):
                    st.query_params.update({
                        "page": "player",
                        "name": row['Name'],
                        "school": row['School_Name_Then'],
                        "year": row['Year'],
                        "season": row['Season']
                    })
                    st.rerun()

# -----------------------------------------------------------------------------
# 4. メインルーティング (Main Routing)
# -----------------------------------------------------------------------------
def main():
    # URLパラメータを取得
    params = st.query_params
    page = params.get("page", "home")

    if page == "player":
        name = params.get("name")
        school = params.get("school")
        year = params.get("year")
        season = params.get("season")
        
        if name and school and year:
            show_player_detail(name, school, year, season)
        else:
            st.error("パラメータが不足しています")
            if st.button("戻る"):
                st.query_params.clear()
                st.rerun()
    
    # 将来的に 'school' ページなどを追加可能
    # elif page == "school":
    #     show_school_detail(...)
    
    else:
        show_search_page()

if __name__ == "__main__":
    main()
