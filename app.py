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

# 右寄せCSS
st.markdown("<style>[data-testid='stDataFrame'] td { text-align: right !important; }</style>", unsafe_allow_html=True)

with st.sidebar:
    st.header("🔍 選手検索")
    name_input = st.text_input("選手名", placeholder="例：古城大翔")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

if name_input or year_input:
    try:
        # 検索条件の構築
        where_list = []
        if name_input: where_list.append(f"c.`名前` LIKE '%{name_input}%'")
        if year_input: where_list.append(f"c.`世代` = {year_input}")
        where_sql = " AND ".join(where_list)

        # 【簡略化】DB_出場メンバー とのJOINを削除！
        # 必要な情報はすべて c (キャリア統合) に入っている
        query = f"""
            SELECT c.*, 
                   m.`出身`, m.`Position`, m.`生年月日`, m.`球団`, m.`ドラフト`, m.`順位`, m.`侍JAPAN`
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m ON c.`Player_ID` = m.`Player_ID`
            WHERE {where_sql} LIMIT 100
        """
        df = client.query(query).to_dataframe()

        if not df.empty:
            df['display_label'] = df['名前'] + " （" + df['高校'].fillna('不明') + "）"
            selected = st.selectbox("選手を選択", options=df['display_label'].unique())
            
            if selected:
                # 選択された選手の全データ（全ての年の記録）を取得
                p_all = df[df['display_label'] == selected].sort_values('Year')
                p = p_all.iloc[0] # プロフィール表示用に最新（または最初の）行を使用

                st.markdown(f"## **{p['名前']}** （{p['高校']}）")
                
                # プロフィール
                bday = pd.to_datetime(p['生年月日']).strftime('%Y年%m月%d日') if pd.notna(p.get('生年月日')) else "不明"
                gen = int(p['世代']) if pd.notna(p.get('世代')) else "不明"
                st.write(f"📅 **世代:** {gen}年 / 🎂 **生年月日:** {bday} / 📍 **出身:** {p.get('出身','-')}")

                # ドラフト情報
                if pd.notna(p.get('球団')) and str(p['球団']) != 'None':
                     st.success(f"🚀 **{p['球団']}** {p.get('ドラフト','').split('.')[0]}年 {p.get('順位','')}位")

                st.divider()
                st.subheader("🏟️ 甲子園出場・詳細記録")
                
                # 詳細テーブル：すでに統合されている列をそのまま表示するだけ
                # 役職判定もシンプルに
                p_all['役職'] = p_all['主将'].apply(lambda x: "★主将" if "◎" in str(x) else "-")
                
                cols = ['Year', 'Season', '学年', '背番号', '投打', '役職', '成績']
                st.dataframe(p_all[[c for c in cols if c in p_all.columns]], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"エラー: {e}")
        
