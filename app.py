import streamlit as st
from google.cloud import bigquery
import pandas as pd

# --- ページ設定 ---
st.set_page_config(page_title="甲子園全記録DB v2", layout="wide")
st.title("⚾️ 甲子園全記録データベース")

@st.cache_resource
def get_bq_client():
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

# 表示用ラベル設定（内部英語：表示日本語）
COL_LABELS = {
    'Year': '年度', 'Tournament': '大会名', 'Season': '季',
    'Grade': '学年', 'Uniform_Number': '背番号', 'Throw_Bat': '投打',
    'Captain': '役職', 'Result': '成績', 'Game_Scores': '対戦詳細'
}

# --- サイドバー検索 ---
with st.sidebar:
    st.header("🔍 選手を探す")
    name_q = st.text_input("選手名（一部でも可）", placeholder="例：石垣元気")
    gen_q = st.number_input("世代（入学年）", value=None, step=1, placeholder="例：2007")

# --- メインコンテンツ ---
if name_q or gen_q:
    try:
        where = []
        if name_q: where.append(f"c.Name LIKE '%{name_q}%'")
        if gen_q:  where.append(f"c.Generation = '{int(gen_q)}'")
        
        query = f"""
            SELECT c.*, m.Hometown, m.Pro_Team, m.Draft_Year, m.Draft_Rank
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m ON c.Player_ID = m.Player_ID
            WHERE {" AND ".join(where)} 
            ORDER BY c.Year ASC
        """
        df = client.query(query).to_dataframe()

        if not df.empty:
            df['display'] = df['Name'] + " (" + df['School'].fillna('不明') + ")"
            target_player = st.selectbox("該当選手を選択", df['display'].unique())
            
            if target_player:
                p_all = df[df['display'] == target_player].copy()
                p = p_all.iloc[0] # プロフィール用
                
                # 1. 選手ヘッダー
                st.markdown(f"## **{p['Name']}** （{p['School']}）")
                
                # 2. プロフィール（ドラフト情報含む）
                meta = [f"📅 **世代:** {p['Generation']}年"]
                if pd.notna(p.get('Hometown')): meta.append(f"📍 **出身:** {p['Hometown']}")
                st.write(" / ".join(meta))
                
                if pd.notna(p.get('Pro_Team')) and str(p['Pro_Team']) != 'None':
                    st.success(f"🚀 **{p['Pro_Team']}** ({str(p['Draft_Year'])}年 ドラフト{p['Draft_Rank']}位)")

                st.divider()
                
                # 3. キャリア年表
                st.subheader("🏟️ 甲子園出場・対戦成績")
                p_all['Captain'] = p_all['Captain'].apply(lambda x: "★主将" if "◎" in str(x) else "-")
                
                # 表示用にリネームして表示
                show_cols = [c for c in COL_LABELS.keys() if c in p_all.columns]
                st.dataframe(p_all[show_cols].rename(columns=COL_LABELS), use_container_width=True, hide_index=True)

                # 4. 【ドリルダウン】大会詳細の選択
                st.divider()
                tourney_list = p_all['Tournament'].dropna().unique()
                if len(tourney_list) > 0:
                    selected_t = st.selectbox("⏬ 大会全体の戦績を詳しく見る", tourney_list)
                    if selected_t:
                        st.info(f"「{selected_t}」の全対戦データをロードしています...")
                        t_query = f"""
                            SELECT Round as 回戦, Win_Loss as 勝敗, Score as スコア, 
                                   School as 学校, Opponent as 対戦校, Game_Scores as 詳細
                            FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                            WHERE Tournament = '{selected_t}'
                            ORDER BY Round ASC
                        """
                        df_t = client.query(t_query).to_dataframe()
                        st.dataframe(df_t, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"システムエラーが発生しました。設定を確認してください。\nError: {e}")
else:
    st.info("左のサイドバーから選手名または世代を入力して検索を開始してください。")
