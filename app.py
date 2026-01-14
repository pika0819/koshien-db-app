import streamlit as st
from google.cloud import bigquery
import pandas as pd

# --- 1. アプリ設定 ---
st.set_page_config(page_title="高校野球DB完全版", layout="wide", page_icon="⚾")
st.title("⚾ 高校野球 全記録データベース")

# スタイル調整
st.markdown("""
<style>
    .stDataFrame {font-size: 0.95rem;}
    h3 {border-bottom: 2px solid #ddd; padding-bottom: 0.5rem; margin-top: 2rem;}
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: #f0f2f6; border-radius: 4px 4px 0 0; gap: 1px; padding-top: 10px; padding-bottom: 10px; }
    .stTabs [aria-selected="true"] { background-color: #ffffff; border-bottom: 2px solid #ff4b4b; }
</style>
""", unsafe_allow_html=True)

# 定数設定
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

# --- 2. BigQuery接続とキャッシュ関数 ---

@st.cache_resource
def get_bq_client():
    """BigQueryクライアントの初期化（リソースキャッシュ）"""
    try:
        # Streamlit CloudのSecretsまたはローカルの認証情報を使用
        if "gcp_service_account" in st.secrets:
            return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])
        return bigquery.Client()
    except Exception as e:
        st.error(f"BigQuery接続エラー: {e}")
        return None

@st.cache_data(ttl=3600)  # 1時間キャッシュ
def run_query(query_string):
    """クエリを実行してDataFrameを返す（データキャッシュ）"""
    client = get_bq_client()
    if not client:
        return pd.DataFrame()
    try:
        return client.query(query_string).to_dataframe()
    except Exception as e:
        st.warning(f"データ取得中にエラーが発生しました: {e}")
        return pd.DataFrame()

# --- 3. サイドバー ---
with st.sidebar:
    st.header("📂 メニュー")
    mode = st.radio("検索モード", ["🏆 大会から探す", "👤 選手から探す", "🏫 高校から探す"])

# ==========================================
# 🏆 モード: 大会記録
# ==========================================
if mode == "🏆 大会から探す":
    st.subheader("🏆 大会記録・出場校チェック")
    
    # 年度リスト取得
    df_years = run_query(f"SELECT DISTINCT Year FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` ORDER BY Year DESC")
    years_list = df_years['Year'].tolist() if not df_years.empty else []

    if not years_list:
        st.warning("年度データの読み込みに失敗しました。")
    else:
        col1, col2 = st.columns(2)
        with col1: sel_year = st.selectbox("年度", years_list)
        with col2: sel_season = st.selectbox("季節", ["夏", "春"])
        
        if sel_year and sel_season:
            # 大会情報
            t_info = run_query(f"SELECT Tournament, Champion FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` WHERE Year = '{sel_year}' AND Season = '{sel_season}'")
            
            if not t_info.empty:
                champ = t_info.iloc[0].get('Champion', '不明')
                st.success(f"🚩 **{t_info.iloc[0]['Tournament']}** （優勝：{champ}）")
                
                # 出場校一覧
                df_res = run_query(f"""
                    SELECT School, School_ID, Rank, History_Label
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                    WHERE Year = '{sel_year}' AND Season = '{sel_season}'
                    ORDER BY School_ID ASC
                """)
                
                if df_res.empty:
                    st.warning("出場データが見つかりません。")
                else:
                    st.write(f"👇 **出場 {len(df_res)} 校** （行をクリックで詳細表示）")

                    if 'History_Label' not in df_res.columns: df_res['History_Label'] = '-'
                    if 'Rank' not in df_res.columns: df_res['Rank'] = '-'
                    
                    display_df = df_res[['School', 'History_Label', 'Rank']].rename(columns={
                        'School': '高校名', 'History_Label': '出場情報', 'Rank': '成績'
                    })
                    
                    selection = st.dataframe(
                        display_df,
                        use_container_width=True,
                        hide_index=True,
                        on_select="rerun",
                        selection_mode="single-row"
                    )
                    
                    # 行選択時の詳細表示
                    if len(selection.selection.rows) > 0:
                        row_idx = selection.selection.rows[0]
                        row_data = df_res.iloc[row_idx]
                        target_sid = row_data.get('School_ID', '')
                        target_school = row_data.get('School', '不明')
                        
                        st.divider()
                        st.markdown(f"## 🏫 **{target_school}**")
                        st.caption(f"📝 {row_data['History_Label']}")
                        
                        tab1, tab2, tab3 = st.tabs(["⚾ この大会の戦績", "🦁 当時のメンバー", "📜 過去の歩み"])
                        
                        # Tab1: 戦績
                        with tab1:
                            df_games = run_query(f"""
                                SELECT Round, Opponent, Score, Win_Loss, Game_Scores
                                FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                                WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'
                                ORDER BY Round ASC
                            """)
                            if not df_games.empty:
                                cols = {'Round':'回戦', 'Opponent':'対戦校', 'Score':'スコア', 'Win_Loss':'勝敗', 'Game_Scores':'詳細'}
                                st.dataframe(df_games.rename(columns=cols), use_container_width=True, hide_index=True)
                            else:
                                st.info("試合データなし")

                        # Tab2: メンバー
                        with tab2:
                            df_mem = run_query(f"""
                                SELECT Name, Grade, Uniform_Number, Position, Captain
                                FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手データ完全版`
                                WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'
                                ORDER BY CASE WHEN Grade = '監督' THEN 1 ELSE 0 END, SAFE_CAST(Uniform_Number AS INT64)
                            """)
                            if not df_mem.empty:
                                if 'Captain' in df_mem.columns:
                                    df_mem['Captain'] = df_mem['Captain'].apply(lambda x: "★主将" if x and ("◎" in str(x) or "主将" in str(x)) else "")
                                r_map = {'Name':'氏名', 'Grade':'学年', 'Uniform_Number':'背番号', 'Position':'守備', 'Captain':'役職'}
                                st.dataframe(df_mem.rename(columns=r_map), use_container_width=True, hide_index=True)
                            else:
                                st.info("メンバーデータなし")
                        
                        # Tab3: 過去履歴
                        with tab3:
                            # 過去のデータを取得（現在選択中の大会以前）
                            df_hist = run_query(f"""
                                SELECT Year, Season, Rank, History_Label
                                FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                                WHERE School_ID = '{target_sid}' 
                                  AND (CAST(Year AS INT64) < {sel_year} OR (CAST(Year AS INT64) = {sel_year} AND Season != '{sel_season}'))
                                ORDER BY CAST(Year AS INT64) DESC, Season DESC
                                LIMIT 20
                            """)
                            if not df_hist.empty:
                                display_h = df_hist.rename(columns={'Year':'年度','Season':'季','Rank':'成績','History_Label':'当時の記録'})
                                st.dataframe(
                                    display_h, 
                                    use_container_width=True, 
                                    hide_index=True, 
                                    column_config={"年度": st.column_config.NumberColumn(format="%d")}
                                )
                            else:
                                st.info("過去の出場履歴なし")

# ==========================================
# 👤 モード: 選手検索
# ==========================================
elif mode == "👤 選手から探す":
    st.subheader("👤 選手検索（完全版データ）")
    col1, col2 = st.columns([2, 1])
    with col1: name_in = st.text_input("選手名（部分一致）")
    with col2: gen_in = st.number_input("世代（生まれ年）", value=None, step=1, placeholder="例: 2005")
    
    if name_in or gen_in:
        where = []
        if name_in: where.append(f"Name LIKE '%{name_in}%'")
        if gen_in: where.append(f"Generation = '{int(gen_in)}'")
        
        df = run_query(f"""
            SELECT Player_ID, Name, School_Name_Now, Year, Season, Grade, Uniform_Number, Result_ID,
                   Pro_Team, Hometown, Draft_Rank, Position, Throw_Bat
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手データ完全版`
            WHERE {' AND '.join(where)} 
            ORDER BY Generation DESC, CAST(Year AS INT64) ASC
        """)
        
        if not df.empty:
            df['Label'] = df['Name'] + " (" + df['School_Name_Now'] + ")"
            unique_options = df['Label'].unique()
            
            sel = st.selectbox("詳細を表示したい選手を選択", unique_options)
            
            if sel:
                t_name, t_school = sel.split(" (")[0], sel.split(" (")[1].replace(")", "")
                p_data = df[(df['Name'] == t_name) & (df['School_Name_Now'] == t_school)]
                
                if not p_data.empty:
                    latest = p_data.iloc[-1]
                    st.markdown(f"## ⚾ {latest['Name']}")
                    st.caption(f"所属: {latest['School_Name_Now']}")
                    
                    # プロフィール情報の整理
                    meta_info = []
                    if pd.notna(latest.get('Pro_Team')): meta_info.append(f"🚀 **{latest['Pro_Team']}**")
                    if pd.notna(latest.get('Draft_Rank')): meta_info.append(f"📝 ドラフト: {latest['Draft_Rank']}")
                    if pd.notna(latest.get('Hometown')): meta_info.append(f"📍 出身: {latest['Hometown']}")
                    if pd.notna(latest.get('Throw_Bat')): meta_info.append(f"⚾ {latest['Throw_Bat']}")
                    if meta_info: st.markdown(" / ".join(meta_info))
                    
                    st.divider()
                    st.markdown("##### 📅 甲子園での記録")
                    cols = {'Year':'年度', 'Season':'季', 'Grade':'学年', 'Uniform_Number':'背番号', 'Position':'守備', 'Result_ID':'大会ID'}
                    st.dataframe(
                        p_data[list(cols.keys())].rename(columns=cols),
                        use_container_width=True, 
                        hide_index=True,
                        column_config={"年度": st.column_config.NumberColumn(format="%d")}
                    )
        else:
            st.warning("該当する選手は見つかりませんでした。")

# # ==========================================
# 🏫 モード: 高校検索
# ==========================================
elif mode == "🏫 高校から探す":
    st.subheader("🏫 高校検索")
    s_in = st.text_input("高校名を入力してください")
    
    if s_in:
        # 修正箇所: School_Name ではなく Latest_School_Name と Official_School_Name を検索対象に変更
        df_s = run_query(f"""
            SELECT DISTINCT School_ID, Latest_School_Name, Prefecture 
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_高校マスタ` 
            WHERE Latest_School_Name LIKE '%{s_in}%' 
               OR Official_School_Name LIKE '%{s_in}%' 
            LIMIT 20
        """)
        
        if not df_s.empty:
            df_s['Label'] = df_s['Latest_School_Name'] + " (" + df_s['Prefecture'] + ")"
            sel = st.selectbox("高校を選択", df_s['Label'].unique())
            
            if sel:
                sid = df_s[df_s['Label']==sel].iloc[0]['School_ID']
                st.markdown(f"### 📜 {sel.split(' (')[0]} の出場履歴")
                
                df_h = run_query(f"""
                    SELECT Year, Season, Rank, History_Label
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                    WHERE School_ID = '{sid}'
                    ORDER BY CAST(Year AS INT64) DESC, Season DESC
                """)
                
                if not df_h.empty:
                    st.dataframe(
                        df_h.rename(columns={'Year':'年度','Season':'季','Rank':'成績','History_Label':'情報'}),
                        use_container_width=True,
                        hide_index=True,
                        column_config={"年度": st.column_config.NumberColumn(format="%d")}
                    )
                else:
                    st.warning("出場履歴データがありません")
        else:
            st.warning("高校が見つかりませんでした")
