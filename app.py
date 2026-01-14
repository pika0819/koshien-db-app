import streamlit as st
from google.cloud import bigquery
import pandas as pd

# --- 1. アプリ設定 ---
st.set_page_config(page_title="高校野球DB完全版", layout="wide", page_icon="⚾")
st.title("⚾ 高校野球 全記録データベース")

st.markdown("""
<style>
    .stDataFrame {font-size: 0.95rem;}
    h3 {border-bottom: 2px solid #ddd; padding-bottom: 0.5rem; margin-top: 2rem;}
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_bq_client():
    try:
        # Streamlit CloudなどのSecretsから認証情報を取得
        return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])
    except:
        # ローカル環境などでデフォルト認証を使う場合
        return bigquery.Client()

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

# --- 2. サイドバー ---
with st.sidebar:
    st.header("📂 メニュー")
    mode = st.radio("検索モード", ["🏆 大会から探す", "👤 選手から探す", "🏫 高校から探す"])

# ==========================================
# 🏆 モード: 大会記録
# ==========================================
if mode == "🏆 大会から探す":
    st.subheader("🏆 大会記録・出場校チェック")
    
    try:
        # 大会マスタから年度リストを取得
        df_years = client.query(f"SELECT DISTINCT Year FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` ORDER BY Year DESC").to_dataframe()
        years_list = df_years['Year'].tolist()
    except:
        st.warning("データ読み込み待機中...")
        years_list = []

    col1, col2 = st.columns(2)
    with col1: sel_year = st.selectbox("年度", years_list)
    with col2: sel_season = st.selectbox("季節", ["夏", "春"])
    
    if sel_year and sel_season:
        t_info = client.query(f"SELECT Tournament, Champion FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` WHERE Year = '{sel_year}' AND Season = '{sel_season}'").to_dataframe()
        
        if not t_info.empty:
            champ = t_info.iloc[0].get('Champion', '不明')
            st.info(f"🚩 **{t_info.iloc[0]['Tournament']}** （優勝：{champ}）")
            
            # 出場校一覧
            df_res = client.query(f"""
                SELECT School, School_ID, Rank, History_Label
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                WHERE Year = '{sel_year}' AND Season = '{sel_season}'
                ORDER BY School_ID ASC
            """).to_dataframe()
            
            if df_res.empty:
                st.warning("出場データが見つかりません。")
            else:
                st.write(f"👇 **出場 {len(df_res)} 校** （クリックで詳細表示）")

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
                
                # 詳細表示
                if len(selection.selection.rows) > 0:
                    row_idx = selection.selection.rows[0]
                    row_data = df_res.iloc[row_idx]
                    target_sid = row_data.get('School_ID', '')
                    target_school = row_data.get('School', '不明')
                    
                    st.divider()
                    st.markdown(f"## 🏫 **{target_school}**")
                    st.info(f"📝 {row_data['History_Label']}")
                    
                    tab1, tab2, tab3 = st.tabs(["⚾ この大会の戦績", "🦁 当時のメンバー", "📜 過去の歩み"])
                    
                    with tab1:
                        # 試合スコア
                        games_query = f"""
                            SELECT Round, Opponent, Score, Win_Loss, Game_Scores
                            FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                            WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'
                            ORDER BY Round ASC
                        """
                        try:
                            df_games = client.query(games_query).to_dataframe()
                            cols = {'Round':'回戦', 'Opponent':'対戦校', 'Score':'スコア', 'Win_Loss':'勝敗', 'Game_Scores':'詳細'}
                            valid = {k:v for k,v in cols.items() if k in df_games.columns}
                            st.dataframe(df_games[valid.keys()].rename(columns=valid), use_container_width=True, hide_index=True)
                        except:
                            st.write("試合データなし")

                    with tab2:
                        # メンバー表（DB_選手データ完全版から取得）
                        # 【修正】監督を最後に表示 (CASE WHEN Grade = '監督' THEN 1 ELSE 0 END)
                        m_query = f"""
                            SELECT 
                                Name, Grade, Uniform_Number, Position, Captain
                            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手データ完全版`
                            WHERE School_ID = '{target_sid}' 
                              AND Year = '{sel_year}' 
                              AND Season = '{sel_season}'
                            ORDER BY 
                                CASE WHEN Grade = '監督' THEN 1 ELSE 0 END, -- 0が先頭、1が最後
                                SAFE_CAST(Uniform_Number AS INT64)
                        """
                        try:
                            df_mem = client.query(m_query).to_dataframe()
                            if not df_mem.empty:
                                r_map = {'Name':'氏名', 'Grade':'学年', 'Uniform_Number':'背番号', 'Position':'守備', 'Captain':'役職'}
                                valid = {k:v for k,v in r_map.items() if k in df_mem.columns}
                                
                                # キャプテン表記
                                if 'Captain' in df_mem.columns:
                                    df_mem['Captain'] = df_mem['Captain'].apply(lambda x: "★主将" if "◎" in str(x) or "主将" in str(x) else "")

                                st.dataframe(df_mem[valid.keys()].rename(columns=valid), use_container_width=True, hide_index=True)
                            else:
                                st.warning("メンバーデータなし")
                        except Exception as e:
                            st.error(f"データ取得エラー: {e}")
                    
                    with tab3:
                        # 過去の成績
                        h_query = f"""
                            SELECT Year, Season, Rank, History_Label
                            FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                            WHERE School_ID = '{target_sid}' 
                              AND (CAST(Year AS INT64) < {sel_year} OR (CAST(Year AS INT64) = {sel_year} AND Season != '{sel_season}'))
                            ORDER BY CAST(Year AS INT64) DESC, Season DESC
                            LIMIT 20
                        """
                        try:
                            df_hist = client.query(h_query).to_dataframe()
                            if 'History_Label' not in df_hist.columns: df_hist['History_Label'] = '-'
                            st.dataframe(
                                df_hist.rename(columns={'Year':'年度','Season':'季','Rank':'成績','History_Label':'当時の記録'}), 
                                use_container_width=True, hide_index=True,
                                column_config={"年度": st.column_config.NumberColumn(format="%d")}
                            )
                        except:
                            st.info("過去の出場履歴なし")

# ==========================================
# 👤 モード: 選手検索
# ==========================================
elif mode == "👤 選手から探す":
    st.subheader("👤 選手検索（完全版データ）")
    name_in = st.text_input("選手名")
    gen_in = st.number_input("世代（生まれ年）", value=None, step=1)
    
    if name_in or gen_in:
        where = []
        if name_in: where.append(f"Name LIKE '%{name_in}%'")
        if gen_in: where.append(f"Generation = '{int(gen_in)}'")
        
        # 【修正】DB_選手データ完全版 のみから直接取得 (JOINなし)
        q = f"""
            SELECT 
                Player_ID, Name, School_Name_Now, Year, Season, 
                Grade, Uniform_Number, Result_ID,
                Pro_Team, Hometown, Draft_Rank, Position, Throw_Bat
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手データ完全版`
            WHERE {' AND '.join(where)} 
            ORDER BY Generation DESC, CAST(Year AS INT64) ASC
        """
        try:
            df = client.query(q).to_dataframe()
            if not df.empty:
                # 検索結果を一意に識別するためのラベル
                df['Label'] = df['Name'] + " (" + df['School_Name_Now'] + " / " + df['Year'] + ")"
                
                # 人物単位でまとめる（同じ人が複数年度出場していても、選択肢は1つに）
                unique_players = df[['Name', 'School_Name_Now']].drop_duplicates()
                unique_players['Display'] = unique_players['Name'] + " (" + unique_players['School_Name_Now'] + ")"
                
                sel = st.selectbox("詳細を表示したい選手を選択", unique_players['Display'].unique())
                
                if sel:
                    target_name = sel.split(" (")[0]
                    target_school = sel.split(" (")[1].replace(")", "")
                    
                    # その選手の全記録を抽出
                    p_data = df[(df['Name'] == target_name) & (df['School_Name_Now'] == target_school)]
                    
                    if not p_data.empty:
                        # 最新のレコードから基本情報を取る
                        latest = p_data.iloc[-1]
                        
                        st.markdown(f"## ⚾ {latest['Name']}")
                        st.text(f"所属: {latest['School_Name_Now']}")
                        
                        # プロフィール情報
                        info_cols = []
                        if pd.notna(latest.get('Pro_Team')): info_cols.append(f"🚀 **{latest['Pro_Team']}**")
                        if pd.notna(latest.get('Draft_Rank')): info_cols.append(f"📝 ドラフト: {latest['Draft_Rank']}")
                        if pd.notna(latest.get('Hometown')): info_cols.append(f"📍 出身: {latest['Hometown']}")
                        if pd.notna(latest.get('Throw_Bat')): info_cols.append(f"⚾ {latest['Throw_Bat']}")
                        
                        if info_cols:
                            st.markdown(" / ".join(info_cols))
                        
                        # 成績テーブル
                        cols = {'Year':'年度', 'Season':'季', 'Grade':'学年', 'Uniform_Number':'背番号', 'Position':'守備', 'Result_ID':'大会記録ID'}
                        # 存在する列だけ表示
                        valid_cols = {k:v for k,v in cols.items() if k in p_data.columns}
                        
                        st.table(p_data[valid_cols.keys()].rename(columns=valid_cols))
            else:
                st.warning("該当する選手は見つかりませんでした。")
        except Exception as e:
            st.error(f"検索エラー: {e}")

# ==========================================
# 🏫 モード: 高校検索
# ==========================================
elif mode == "🏫 高校から探す":
    st.subheader("🏫 高校検索")
    s_in = st.text_input("高校名")
    if s_in:
        # 高校マスタから検索
        df_s = client.query(f"""
            SELECT DISTINCT School_ID, Latest_School_Name, Prefecture 
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_高校マスタ` 
            WHERE School_Name LIKE '%{s_in}%' OR Latest_School_Name LIKE '%{s_in}%' 
            LIMIT 20
        """).to_dataframe()
        
        if not df_s.empty:
            df_s['Label'] = df_s['Latest_School_Name'] + " (" + df_s['Prefecture'] + ")"
            sel = st.selectbox("選択", df_s['Label'].unique())
            
            if sel:
                sid = df_s[df_s['Label']==sel].iloc[0]['School_ID']
                
                st.markdown(f"### 📜 {sel.split(' (')[0]} の出場履歴")
                
                h_query = f"""
                    SELECT Year, Season, Rank, History_Label
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                    WHERE School_ID = '{sid}'
                    ORDER BY CAST(Year AS INT64) DESC, Season DESC
                """
                try:
                    df_h = client.query(h_query).to_dataframe()
                    st.dataframe(
                        df_h.rename(columns={'Year':'年度','Season':'季','Rank':'成績','History_Label':'情報'}), 
                        use_container_width=True, hide_index=True,
                        column_config={"年度": st.column_config.NumberColumn(format="%d")}
                    )
                except:
                    st.warning("データなし")
        else:
            st.warning("見つかりませんでした")
ner_width=True, hide_index=True)
                except:
                    st.warning("データなし")
        else:
            st.warning("見つかりませんでした")
