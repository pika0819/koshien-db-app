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
    div[data-testid="stMetricValue"] {font-size: 1.4rem;}
</style>
""", unsafe_allow_html=True)

# --- 2. BigQuery接続 ---
@st.cache_resource
def get_bq_client():
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

# --- 3. サイドバー ---
with st.sidebar:
    st.header("📂 メニュー")
    mode = st.radio("検索モード", ["🏆 大会から探す", "👤 選手から探す", "🏫 高校から探す"])

# ==========================================
# 🏆 モード: 大会記録
# ==========================================
if mode == "🏆 大会から探す":
    st.subheader("🏆 大会記録・出場校チェック")
    
    # エラー回避: 年度リスト取得
    try:
        df_years = client.query(f"SELECT DISTINCT Year FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` ORDER BY Year DESC").to_dataframe()
        years_list = df_years['Year'].tolist()
    except:
        st.warning("大会データの読み込みに失敗しました。")
        years_list = []

    col1, col2 = st.columns(2)
    with col1: sel_year = st.selectbox("年度", years_list)
    with col2: sel_season = st.selectbox("季節", ["夏", "春"])
    
    if sel_year and sel_season:
        # 大会情報の取得
        t_info = client.query(f"SELECT Tournament, Champion FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` WHERE Year = '{sel_year}' AND Season = '{sel_season}'").to_dataframe()
        if not t_info.empty:
            champ = t_info.iloc[0].get('Champion', '不明')
            st.info(f"🚩 **{t_info.iloc[0]['Tournament']}** （優勝：{champ}）")
            st.write("👇 **詳細を見たい高校の行をクリックしてください**")
            
            # 戦績データの取得（SELECT * で安全に）
            df_res = client.query(f"""
                SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                WHERE Year = '{sel_year}' AND Season = '{sel_season}'
                ORDER BY School_ID ASC
            """).to_dataframe()
            
            # 【重要】列の存在チェック＆補完（KeyError対策）
            required_cols = {'School': '高校名', 'History_Label': '出場情報', 'Result': '成績', 'Game_Scores': '試合結果'}
            for col in required_cols.keys():
                if col not in df_res.columns:
                    df_res[col] = "-"  # 列がない場合はハイフンで埋める
            
            # 表示用データの作成
            display_df = df_res[list(required_cols.keys())].rename(columns=required_cols)
            
            # インタラクティブテーブル
            selection = st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row"
            )
            
            # ドリルダウン詳細
            if len(selection.selection.rows) > 0:
                row_idx = selection.selection.rows[0]
                row_data = df_res.iloc[row_idx]
                target_sid = row_data.get('School_ID', '')
                
                st.divider()
                st.markdown(f"## 🏫 **{row_data['School']}**")
                st.info(f"📝 {row_data['History_Label']}")
                
                tab1, tab2 = st.tabs(["🦁 当時のメンバー", "📜 大会履歴"])
                
                with tab1:
                    # メンバー表取得
                    if target_sid:
                        m_query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'"
                        df_mem = client.query(m_query).to_dataframe()
                        
                        if not df_mem.empty:
                            # 必要な列だけリネームして表示
                            rename_map = {'Name':'氏名','Grade':'学年','Uniform_Number':'背番号','Position':'守備','Throw_Bat':'投打','Captain':'役職'}
                            valid_cols = {k:v for k,v in rename_map.items() if k in df_mem.columns}
                            
                            # 背番号ソート
                            if 'Uniform_Number' in df_mem.columns:
                                df_mem = df_mem.sort_values('Uniform_Number', key=lambda x: pd.to_numeric(x, errors='coerce'))
                            if 'Captain' in df_mem.columns:
                                df_mem['Captain'] = df_mem['Captain'].apply(lambda x: "★主将" if "◎" in str(x) else "")
                                
                            st.dataframe(df_mem[valid_cols.keys()].rename(columns=valid_cols), use_container_width=True, hide_index=True)
                        else:
                            st.warning("メンバー登録データがありません。")
                
                with tab2:
                    # 過去戦績
                    if target_sid:
                        h_query = f"""
                            SELECT Year, Season, Result, Game_Scores, History_Label
                            FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                            WHERE School_ID = '{target_sid}' AND (Year < {sel_year} OR (Year = {sel_year} AND Season != '{sel_season}'))
                            ORDER BY Year DESC, Season DESC LIMIT 10
                        """
                        try:
                            df_hist = client.query(h_query).to_dataframe()
                            # 念のためここでも列チェック
                            if 'History_Label' not in df_hist.columns: df_hist['History_Label'] = '-'
                            if 'Game_Scores' not in df_hist.columns: df_hist['Game_Scores'] = '-'
                            
                            st.dataframe(df_hist.rename(columns={'Year':'年度','Season':'季','Result':'成績','Game_Scores':'詳細','History_Label':'当時'}), use_container_width=True, hide_index=True)
                        except:
                            st.info("履歴データなし")

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
                # 【修正】重複行を削除（Web表示の重複バグ対策）
                df = df.drop_duplicates(subset=['Name', 'School', 'Year', 'Season'])
                
                df['lbl'] = df['Name'] + " (" + df['School'] + ")"
                sel = st.selectbox("選択", df['lbl'].unique())
                
                if sel:
                    p = df[df['lbl']==sel].iloc[0]
                    p_all = df[df['lbl']==sel]
                    
                    st.markdown(f"## {p['Name']} ({p['School']})")
                    if pd.notna(p.get('Pro_Team')): st.success(f"🚀 {p['Pro_Team']}")
                    
                    # 必要な列のみ表示
                    cols = {'Year':'年度','Season':'季','Grade':'学年','Result':'成績','Game_Scores':'詳細'}
                    valid_cols = {k:v for k,v in cols.items() if k in p_all.columns}
                    st.dataframe(p_all[valid_cols.keys()].rename(columns=valid_cols), use_container_width=True, hide_index=True)
            else:
                st.warning("見つかりませんでした")
        except Exception as e:
            st.error(f"Error: {e}")

# ==========================================
# 🏫 モード: 高校検索
# ==========================================
elif mode == "🏫 高校から探す":
    st.subheader("🏫 高校検索")
    s_in = st.text_input("高校名", placeholder="例：光星")
    
    if s_in:
        # 1. まずIDを検索
        df_s = client.query(f"""
            SELECT DISTINCT School_ID, Latest_School_Name, School 
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_高校マスタ` 
            WHERE School LIKE '%{s_in}%' OR Latest_School_Name LIKE '%{s_in}%' 
            LIMIT 20
        """).to_dataframe()
        
        if not df_s.empty:
            sel = st.selectbox("選択", df_s['Latest_School_Name'].unique())
            if sel:
                # 選択された高校のIDを取得
                target_row = df_s[df_s['Latest_School_Name']==sel].iloc[0]
                sid = target_row['School_ID']
                sname = target_row['School'] # 検索用名称
                
                st.divider()
                st.markdown(f"## {sel}")
                
                # 【修正】IDで検索してダメなら、高校名(School)でも検索する「あいまい検索」ロジック
                # まずIDで検索
                df_h = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ` WHERE School_ID = '{sid}' ORDER BY Year DESC, Season DESC").to_dataframe()
                
                # データが0件なら、名前で再検索（IDの不一致対策）
                if df_h.empty:
                    df_h = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ` WHERE School = '{sname}' ORDER BY Year DESC, Season DESC").to_dataframe()
                
                if not df_h.empty:
                    # 列補完
                    if 'History_Label' not in df_h.columns: df_h['History_Label'] = '-'
                    if 'Game_Scores' not in df_h.columns: df_h['Game_Scores'] = '-'

                    cols = {'Year':'年度','Season':'季','Result':'成績','Game_Scores':'詳細','History_Label':'情報'}
                    valid_cols = {k:v for k,v in cols.items() if k in df_h.columns}
                    st.dataframe(df_h[valid_cols.keys()].rename(columns=valid_cols), use_container_width=True, hide_index=True)
                else:
                    st.warning("この高校の戦績データが見つかりませんでした。（ID不一致の可能性があります）")
        else:
            st.warning("高校が見つかりませんでした。")
