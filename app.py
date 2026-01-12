import streamlit as st
from google.cloud import bigquery
import pandas as pd

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
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

with st.sidebar:
    st.header("📂 メニュー")
    mode = st.radio("検索モード", ["🏆 大会から探す", "👤 選手から探す", "🏫 高校から探す"])

# ==========================================
# 🏆 モード: 大会記録
# ==========================================
if mode == "🏆 大会から探す":
    st.subheader("🏆 大会記録・出場校チェック")
    
    try:
        # 大会マスタから年度取得
        df_years = client.query(f"SELECT DISTINCT Year FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` ORDER BY Year DESC").to_dataframe()
        years_list = df_years['Year'].tolist()
    except:
        years_list = []

    col1, col2 = st.columns(2)
    with col1: sel_year = st.selectbox("年度", years_list)
    with col2: sel_season = st.selectbox("季節", ["夏", "春"])
    
    if sel_year and sel_season:
        t_info = client.query(f"SELECT Tournament, Champion FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` WHERE Year = '{sel_year}' AND Season = '{sel_season}'").to_dataframe()
        
        if not t_info.empty:
            champ = t_info.iloc[0].get('Champion', '不明')
            st.info(f"🚩 **{t_info.iloc[0]['Tournament']}** （優勝：{champ}）")
            
            # ------------------------------------------------------------------
            # 【変更点】一覧は「DB_出場成績」から取る！
            # これが最も正しい「出場校リスト」であり、1校1行が保証される
            # ------------------------------------------------------------------
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
                if 'Rank' not in df_res.columns: df_res['Rank'] = '-' # 出場成績ではResultではなくRankカラムの場合が多い
                
                # 一覧表示
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
                
                # ドリルダウン
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
                        # 試合スコアは「戦績データ」から取る（ここはおまけデータとして正しい使い方）
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
                        # メンバー表
                        m_query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'"
                        df_mem = client.query(m_query).to_dataframe()
                        if not df_mem.empty:
                            r_map = {'Name':'氏名','Grade':'学年','Uniform_Number':'背番号','Position':'守備','Throw_Bat':'投打','Captain':'役職'}
                            valid = {k:v for k,v in r_map.items() if k in df_mem.columns}
                            if 'Uniform_Number' in df_mem.columns:
                                df_mem = df_mem.sort_values('Uniform_Number', key=lambda x: pd.to_numeric(x, errors='coerce'))
                            if 'Captain' in df_mem.columns:
                                df_mem['Captain'] = df_mem['Captain'].apply(lambda x: "★主将" if "◎" in str(x) else "")
                            st.dataframe(df_mem[valid.keys()].rename(columns=valid), use_container_width=True, hide_index=True)
                        else:
                            st.warning("メンバーデータなし")
                    
                    with tab3:
                        # 過去履歴も「出場成績」から取る（重複なくスッキリ出る）
                        h_query = f"""
                            SELECT Year, Season, Rank, History_Label
                            FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                            WHERE School_ID = '{target_sid}' 
                              AND (Year < {sel_year} OR (Year = {sel_year} AND Season != '{sel_season}'))
                            ORDER BY Year DESC, Season DESC
                            LIMIT 20
                        """
                        try:
                            df_hist = client.query(h_query).to_dataframe()
                            if 'History_Label' not in df_hist.columns: df_hist['History_Label'] = '-'
                            st.dataframe(df_hist.rename(columns={'Year':'年度','Season':'季','Rank':'成績','History_Label':'当時の記録'}), 
                                         use_container_width=True, hide_index=True)
                        except:
                            st.info("過去の出場履歴なし")

# ==========================================
# 👤 モード: 選手検索
# ==========================================
elif mode == "👤 選手から探す":
    st.subheader("👤 選手検索")
    name_in = st.text_input("選手名")
    gen_in = st.number_input("世代", value=None, step=1)
    
    if name_in or gen_in:
        where = []
        if name_in: where.append(f"c.Name LIKE '%{name_in}%'")
        if gen_in: where.append(f"c.Generation = '{int(gen_in)}'")
        
        q = f"""
            SELECT c.*, m.Hometown, m.Pro_Team 
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` c 
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` m ON c.Player_ID = m.Player_ID 
            WHERE {' AND '.join(where)} ORDER BY c.Year ASC
        """
        try:
            df = client.query(q).to_dataframe()
            if not df.empty:
                df = df.drop_duplicates(subset=['Name', 'School', 'Year', 'Season'])
                df['lbl'] = df['Name'] + " (" + df['School'] + ")"
                sel = st.selectbox("選択", df['lbl'].unique())
                if sel:
                    p = df[df['lbl']==sel].iloc[0]
                    p_all = df[df['lbl']==sel]
                    st.markdown(f"## {p['Name']} ({p['School']})")
                    if pd.notna(p.get('Pro_Team')): st.success(f"🚀 {p['Pro_Team']}")
                    
                    cols = {'Year':'年度','Season':'季','Grade':'学年','Result':'成績','Game_Scores':'詳細'}
                    valid = {k:v for k,v in cols.items() if k in p_all.columns}
                    st.dataframe(p_all[valid.keys()].rename(columns=valid), use_container_width=True, hide_index=True)
            else:
                st.warning("見つかりませんでした")
        except:
            st.error("検索エラー")

# ==========================================
# 🏫 モード: 高校検索
# ==========================================
elif mode == "🏫 高校から探す":
    st.subheader("🏫 高校検索")
    s_in = st.text_input("高校名")
    if s_in:
        df_s = client.query(f"SELECT DISTINCT School_ID, Latest_School_Name FROM `{PROJECT_ID}.{DATASET_ID}.DB_高校マスタ` WHERE School LIKE '%{s_in}%' OR Latest_School_Name LIKE '%{s_in}%' LIMIT 20").to_dataframe()
        if not df_s.empty:
            sel = st.selectbox("選択", df_s['Latest_School_Name'].unique())
            if sel:
                sid = df_s[df_s['Latest_School_Name']==sel].iloc[0]['School_ID']
                
                # ここも「出場成績」から取る
                h_query = f"""
                    SELECT Year, Season, Rank, History_Label
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                    WHERE School_ID = '{sid}'
                    ORDER BY Year DESC, Season DESC
                """
                try:
                    df_h = client.query(h_query).to_dataframe()
                    st.dataframe(df_h.rename(columns={'Year':'年度','Season':'季','Rank':'成績','History_Label':'情報'}), use_container_width=True, hide_index=True)
                except:
                    st.warning("データなし")
        else:
            st.warning("見つかりませんでした")
