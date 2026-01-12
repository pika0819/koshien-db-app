import streamlit as st
from google.cloud import bigquery
import pandas as pd

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

with st.sidebar:
    st.header("🔍 選手を探す")
    name_q = st.text_input("選手名")
    gen_q = st.number_input("世代", value=None, step=1)

if name_q or gen_q:
    try:
        where = []
        if name_q: where.append(f"c.Name LIKE '%{name_q}%'")
        if gen_q:  where.append(f"c.Generation = '{int(gen_q)}'")
        
        # Tournamentを確実にcから取得し、mからは必要なプロフィールのみを取得
        query = f"""
            SELECT 
                c.Player_ID, c.Name, c.School, c.Generation, c.Year, c.Season, 
                c.Grade, c.Result, c.Tournament, c.Game_Scores, c.Throw_Bat, c.Uniform_Number, c.Captain,
                m.Hometown, m.Pro_Team, m.Draft_Year, m.Draft_Rank
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
                p = p_all.iloc[0]
                
                st.markdown(f"## **{p['Name']}** （{p['School']}）")
                
                # プロフィール（欠損値対策）
                meta = [f"📅 **世代:** {p.get('Generation', '不明')}年"]
                if pd.notna(p.get('Hometown')): meta.append(f"📍 **出身:** {p['Hometown']}")
                st.write(" / ".join(meta))
                
                if pd.notna(p.get('Pro_Team')) and str(p['Pro_Team']).lower() != 'none':
                    st.success(f"🚀 **{p['Pro_Team']}** ({str(p['Draft_Year'])}年 {p['Draft_Rank']}位)")

                st.divider()
                st.subheader("🏟️ 甲子園出場・対戦成績")
                
                # 役職表示の変換
                p_all['Captain'] = p_all['Captain'].apply(lambda x: "★主将" if "◎" in str(x) else "-")
                
                # 【ここが重要】Tournamentなどの列が「確実に存在するか」チェックしてから表示
                existing_cols = [c for c in COL_LABELS.keys() if c in p_all.columns]
                show_df = p_all[existing_cols].rename(columns=COL_LABELS)
                
                st.dataframe(show_df, use_container_width=True, hide_index=True)

                # ドリルダウン
                if 'Tournament' in p_all.columns:
                    tourneys = p_all['Tournament'].dropna().unique()
                    if len(tourneys) > 0:
                        selected_t = st.selectbox("⏬ 大会の全戦績を表示", tourneys)
                        if selected_t:
                            # 戦績データ(s)も英語化されている前提
                            t_query = f"""
                                SELECT Round, Win_Loss, Score, School, Opponent, Game_Scores
                                FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                                WHERE Tournament = '{selected_t}'
                                ORDER BY Round ASC
                            """
                            df_t = client.query(t_query).to_dataframe()
                            st.table(df_t)

    except Exception as e:
        st.error(f"Error: {e}")
