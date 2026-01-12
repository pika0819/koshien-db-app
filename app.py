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
    name_input = st.text_input("選手名", placeholder="例：古城大翔")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

if name_input or year_input:
    try:
        # 物理列（名前、世代）でシンプルに検索。JOINでマスタ(m)の基本情報を付与。
        where_sql = " AND ".join([f"c.`{k}` {'LIKE' if k=='名前' else '='} '{v if k=='世代' else '%'+v+'%'}'" 
                                 for k, v in {"名前": name_input, "世代": year_input}.items() if v])
        
        query = f"""
            SELECT c.*, m.`出身`, m.`Position`, m.`生年月日`, m.`球団`, m.`ドラフト`, m.`順位`, m.`侍JAPAN`
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m ON c.`Player_ID` = m.`Player_ID`
            WHERE {where_sql} LIMIT 100
        """
        df = client.query(query).to_dataframe()

        if not df.empty:
            df['display_label'] = df['名前'] + " （" + df['高校'].fillna('不明') + "）"
            selected = st.selectbox("選手を選択", options=df['display_label'].unique())
            
            if selected:
                p = df[df['display_label'] == selected].iloc[0]
                st.markdown(f"## **{p['名前']}** （{p['高校']}）")
                
                # プロフィール（世代、生年月日、プロ入りを1行でスマートに）
                bday = pd.to_datetime(p['生年月日']).strftime('%Y年%m月%d日') if pd.notna(p.get('生年月日')) else "不明"
                st.write(f"📅 **世代:** {int(p['世代'])}年 / 🎂 **生年月日:** {bday} / 🚀 **ドラフト:** {p.get('球団','-')} {p.get('順位','')}")

                st.divider()
                st.subheader("🏟️ 甲子園出場・詳細記録")
                
                # 詳細テーブル（◎判定含む）
                p_all = df[df['Player_ID'] == p['Player_ID']]
                p_all['役職'] = p_all['主将'].apply(lambda x: "★主将" if "◎" in str(x) else "-")
                st.dataframe(p_all[['Year', 'Season', '学年', '背番号', '投打', '役職', '成績']], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"エラー: {e}")
        
