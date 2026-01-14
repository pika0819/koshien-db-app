import streamlit as st
from google.cloud import bigquery
import pandas as pd

# --- 1. アプリ設定 ---
st.set_page_config(page_title="高校野球DB完全版", layout="wide", page_icon="⚾")

# スタイル調整
st.markdown("""
<style>
    .stDataFrame {font-size: 0.95rem;}
    h3 {border-bottom: 2px solid #ddd; padding-bottom: 0.5rem; margin-top: 1rem;}
    .stSpinner {text-align: center; margin: 20px;}
</style>
""", unsafe_allow_html=True)

st.title("⚾ 高校野球 全記録データベース")

PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

# --- 2. BigQuery接続設定 ---
@st.cache_resource
def get_bq_client():
    try:
        # Secretsがある場合はそれを使う
        if "gcp_service_account" in st.secrets:
            return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])
        # ローカル環境などはデフォルト認証
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"DB接続エラー: {e}")
        return None

# クエリ実行関数（キャッシュ有効化で高速化）
@st.cache_data(ttl=3600)
def run_query(query_string):
    client = get_bq_client()
    if client:
        return client.query(query_string).to_dataframe()
    return pd.DataFrame()

# --- 3. サイドバー & モード選択 ---
with st.sidebar:
    st.header("📂 メニュー")
    mode = st.radio("検索モード", ["🏆 大会から探す", "👤 選手から探す", "🏫 高校から探す"])
    
    st.divider()
    if st.button("🔄 データを最新に更新"):
        st.cache_data.clear()
        st.rerun()

# ==========================================
# 🏆 モード: 大会検索
# ==========================================
if mode == "🏆 大会から探す":
    st.subheader("🏆 大会記録・出場校チェック")
    
    # 1. 年度のリストを取得
    df_years = run_query(f"SELECT DISTINCT Year FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` ORDER BY Year DESC")
    years_list = df_years['Year'].tolist() if not df_years.empty else []

    if years_list:
        col1, col2 = st.columns(2)
        with col1: sel_year = st.selectbox("年度", years_list)
        with col2: sel_season = st.selectbox("季節", ["夏", "春"])
        
        if sel_year and sel_season:
            # 2. 大会情報を取得
            t_query = f"""
                SELECT Tournament, Champion 
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` 
                WHERE Year = '{sel_year}' AND Season = '{sel_season}'
            """
            t_info = run_query(t_query)
            
            if not t_info.empty:
                champ = t_info.iloc[0].get('Champion', '不明')
                st.success(f"🚩 **{t_info.iloc[0]['Tournament']}** （優勝：{champ}）")
                
                # 3. 出場校一覧を取得（復元済みのSchoolカラムを使用）
                res_query = f"""
                    SELECT School as `高校名`, Rank as `成績`, History_Label as `出場情報`, School_ID
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                    WHERE Year = '{sel_year}' AND Season = '{sel_season}'
                    ORDER BY School_ID ASC
                """
                df_res = run_query(res_query)
                
                if not df_res.empty:
                    st.write(f"👇 **出場 {len(df_res)} 校** （表をクリックで詳細表示）")
                    
                    selection = st.dataframe(
                        df_res.drop(columns=['School_ID']), # IDは隠して表示
                        use_container_width=True,
                        hide_index=True,
                        on_select="rerun",
                        selection_mode="single-row"
                    )
                    
                    # 詳細表示
                    if len(selection.selection.rows) > 0:
                        row_idx = selection.selection.rows[0]
                        target_sid = df_res.iloc[row_idx]['School_ID']
                        target_school = df_res.iloc[row_idx]['高校名']
                        
                        st.divider()
                        st.markdown(f"### 🏫 **{target_school}** の詳細データ")
                        
                        tab1, tab2 = st.tabs(["⚾ この大会の戦績", "🦁 登録メンバー"])
                        
                        with tab1:
                            g_query = f"""
                                SELECT Round as `回戦`, Opponent as `対戦校`, Score as `スコア`, Win_Loss as `勝敗`
                                FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                                WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'
                                ORDER BY Round ASC
                            """
                            st.dataframe(run_query(g_query), use_container_width=True, hide_index=True)
                            
                        with tab2:
                            m_query = f"""
                                SELECT Name as `氏名`, Grade as `学年`, Uniform_Number as `背番号`, Position as `守備`
                                FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手データ完全版`
                                WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'
                                ORDER BY SAFE_CAST(Uniform_Number AS INT64)
                            """
                            st.dataframe(run_query(m_query), use_container_width=True, hide_index=True)

# ==========================================
# 👤 モード: 選手検索
# ==========================================
elif mode == "👤 選手から探す":
    st.subheader("👤 選手検索")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        p_name = st.text_input("選手名を入力（部分一致）", placeholder="例: 松坂")
    
    if p_name:
        with st.spinner('選手データを検索中...'):
            # シンプルに検索
            p_query = f"""
                SELECT 
                    Name as `氏名`, 
                    School_Name_Now as `所属高校`, 
                    Year as `年度`, 
                    Season as `季`, 
                    Grade as `学年`, 
                    Pro_Team as `プロ入り`
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手データ完全版` 
                WHERE Name LIKE '%{p_name}%' 
                ORDER BY Year DESC, Season DESC
                LIMIT 100
            """
            df_p = run_query(p_query)
            
        if not df_p.empty:
            st.dataframe(df_p, use_container_width=True, hide_index=True)
        else:
            st.warning("該当する選手が見つかりません。")

# ==========================================
# 🏫 モード: 高校検索 (高速化対応版)
# ==========================================
elif mode == "🏫 高校から探す":
    st.subheader("🏫 高校検索")
    
    # Step 1: まず高校マスタだけを検索（軽量）
    s_in = st.text_input("高校名を入力してください", placeholder="例: 光星, 高松")
    
    if s_in:
        with st.spinner('高校マスタを検索中...'):
            # 最新名、正式名、または「高校」列（旧称など）で検索
            m_query = f"""
                SELECT DISTINCT School_ID, Latest_School_Name, Prefecture, 高校
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_高校マスタ` 
                WHERE Latest_School_Name LIKE '%{s_in}%' 
                   OR Official_School_Name LIKE '%{s_in}%'
                   OR 高校 LIKE '%{s_in}%'
                LIMIT 50
            """
            df_master = run_query(m_query)
        
        if not df_master.empty:
            # 選択肢を作成（学校名 + 都道府県）
            df_master['Display_Label'] = df_master['Latest_School_Name'] + " (" + df_master['Prefecture'] + ")"
            # 重複を除去してリスト化
            school_options = df_master['Display_Label'].unique()
            
            # Step 2: ユーザーが高校を選択
            selected_label = st.selectbox("高校を選択してください", school_options)
            
            if selected_label:
                # 選択された高校に関連するすべてのIDを取得（合併などで複数IDがある場合に対応）
                target_latest_name = df_master[df_master['Display_Label'] == selected_label]['Latest_School_Name'].iloc[0]
                target_ids = df_master[df_master['Latest_School_Name'] == target_latest_name]['School_ID'].unique().tolist()
                
                # IDリストをSQL用文字列に変換 ('id1', 'id2', ...)
                ids_str = "', '".join(target_ids)
                
                st.markdown(f"### 📜 {target_latest_name} の歴史")
                
                # Step 3: IDを使って詳細データを取得（ここで初めて重いテーブルを見に行く）
                with st.spinner('詳細データを取得中...'):
                    # 出場履歴（復元済みのSchoolカラム = 当時の校名を表示）
                    h_query = f"""
                        SELECT 
                            Year as `年度`, 
                            Season as `季`, 
                            School as `当時の校名`, 
                            Rank as `成績`, 
                            Tournament as `大会名`
                        FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場成績`
                        WHERE School_ID IN ('{ids_str}')
                        ORDER BY CAST(Year AS INT64) DESC, Season DESC
                    """
                    df_history = run_query(h_query)
                
                if not df_history.empty:
                    # 見やすく表示
                    st.dataframe(
                        df_history, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config={
                            "年度": st.column_config.NumberColumn(format="%d"),
                            "当時の校名": st.column_config.TextColumn(width="medium")
                        }
                    )
                    st.caption(f"通算出場回数: {len(df_history)} 回")
                else:
                    st.warning("出場履歴データがありません。")
        else:
            st.warning("高校が見つかりませんでした。")
