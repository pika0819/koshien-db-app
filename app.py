import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="甲子園全記録DB", layout="wide")
st.title("⚾️ 甲子園全記録データベース")

@st.cache_resource
def get_bq_client():
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

with st.sidebar:
    st.header("🔍 選手検索")
    name_input = st.text_input("選手名", placeholder="例：高橋宏斗")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

if name_input or year_input:
    try:
        # 検索クエリ
        where_clauses = []
        if name_input: where_clauses.append(f"c.`名前` LIKE '%{name_input}%'")
        if year_input: where_clauses.append(f"m.`世代` = {year_input}")
        where_sql = " AND ".join(where_clauses)
        
        query = f"""
            SELECT DISTINCT 
                c.`Player_ID`, c.`名前`, m.`高校`, m.`世代`, m.`出身`, m.`Position`, m.`生年月日`,
                m.`球団`, m.`ドラフト`, m.`順位`, m.`進路`,
                m.`U12`, m.`U15`, m.`U18`, m.`U22`, m.`侍JAPAN`
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m ON c.`Player_ID` = m.`Player_ID`
            WHERE {where_sql}
            LIMIT 100
        """
        df_players = client.query(query).to_dataframe()

        if not df_players.empty:
            df_players['display_label'] = df_players['名前'] + " （" + df_players['高校'].fillna('不明') + "）"
            selected_label = st.selectbox("選手を選択", options=df_players['display_label'].tolist())
            
            if selected_label:
                p = df_players[df_players['display_label'] == selected_label].iloc[0]
                
                st.markdown(f"## **{p['名前']}** （{p['高校']}）")
                
                # 生年月日と基本情報
                st.write(f"🎂 **生年月日:** {p.get('生年月日', '不明')} / **出身:** {p['出身']} / **世代:** {p['世代']}年")

                # プロ入り実績（欠損ガード）
                if pd.notna(p.get('球団')):
                    draft_info = f"🚀 **{p['球団']}**"
                    if pd.notna(p.get('ドラフト')): draft_info += f" / {str(p['ドラフト']).split('.')[0]}年"
                    if pd.notna(p.get('順位')):
                        rank = str(p['順位'])
                        draft_info += f" / {rank if '育成' in rank else rank + '位'}"
                    st.success(draft_info)

                # --- 代表歴の解読ロジック ---
                reps = []
                for col in ['U12', 'U15', 'U18', 'U22', '侍JAPAN']:
                    val = str(p.get(col, '')).strip()
                    if val and val not in ["None", "nan", ""]:
                        label = col
                        # 侍ジャパンの西暦解読 (*23 -> 2023年)
                        if col == '侍JAPAN' and val.startswith('*'):
                            year_short = val.replace('*', '')
                            label = f"侍JAPAN (20{year_short}年)"
                        elif val != "1": # 1以外の数字（背番号等）があれば表示
                            label = f"{col} (背番号:{val})"
                        reps.append(f"🇯🇵 {label}")
                
                if reps:
                    st.warning(f"🏅 **代表経験:** {' ／ '.join(reps)}")

                st.divider()
                # キャリア詳細（以前の柔軟結合を維持）
                st.subheader("🏟️ 甲子園出場記録")
                # ...（以前のcareer_queryと同様）
                
    except Exception as e:
        st.error(f"エラー: {e}")
