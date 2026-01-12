import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ページ設定
st.set_page_config(page_title="甲子園DB", layout="wide")

# 表示用ラベル設定（内部は英語、表示は日本語）
COL_LABELS = {
    'Year': '年度',
    'Tournament': '大会',
    'Season': '季',
    'Grade': '学年',
    'Uniform_Number': '背番号',
    'Throw_Bat': '投打',
    'Captain': '役職',
    'Result': '成績',
    'Game_Scores': '対戦詳細'
}

@st.cache_resource
def get_bq_client():
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

# サイドバー検索
with st.sidebar:
    name_input = st.text_input("選手名検索")
    gen_input = st.number_input("世代（入学年）", value=None, step=1)

if name_input or gen_input:
    try:
        # 英語カラムなのでバッククォートなしでスッキリ！
        where = []
        if name_input: where.append(f"c.Name LIKE '%{name_input}%'")
        if gen_input: where.append(f"c.Generation = '{int(gen_input)}'")
        
        query = f"""
            SELECT c.*, m.Hometown, m.Pro_Team, m.Draft_Year, m.Draft_Rank
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m ON c.Player_ID = m.Player_ID
            WHERE {" AND ".join(where)} LIMIT 100
        """
        df = client.query(query).to_dataframe()

        if not df.empty:
            df['display'] = df['Name'] + " (" + df['School'].fillna('不明') + ")"
            selected = st.selectbox("選手を選択", df['display'].unique())
            
            if selected:
                p_all = df[df['display'] == selected].sort_values('Year')
                p = p_all.iloc[0]
                
                st.header(f"{p['Name']}（{p['School']}）")
                
                # キャプテンマークの整形
                p_all['Captain'] = p_all['Captain'].apply(lambda x: "★主将" if "◎" in str(x) else "-")
                
                # テーブル表示
                show_df = p_all[list(COL_LABELS.keys())].rename(columns=COL_LABELS)
                st.dataframe(show_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"アプリエラー: {e}")
