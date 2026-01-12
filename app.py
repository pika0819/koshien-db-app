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
        # 1. 基本情報の検索（実績シートとの結合を一旦やめ、確実に動く形に）
        where_clauses = []
        if name_input: where_clauses.append(f"m.`名前` LIKE '%{name_input}%'")
        if year_input: where_clauses.append(f"m.`世代` = {year_input}")
        where_sql = " AND ".join(where_clauses)
        
        query = f"""
            SELECT m.*
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m
            WHERE {where_sql}
            LIMIT 50
        """
        df_players = client.query(query).to_dataframe()

        if not df_players.empty:
            st.subheader("📋 検索結果")
            df_players['display_label'] = df_players['名前'] + " (" + df_players['高校'] + ")"
            selected_label = st.selectbox("選手を選択", options=df_players['display_label'].tolist())
            
            if selected_label:
                p = df_players[df_players['display_label'] == selected_label].iloc[0]
                
                # --- 出場記録・詳細の取得 ---
                # 出場メンバー(mem)から「投打」「主将」「背番号」をまとめて取得します
                st.subheader("🏟️ 甲子園出場・詳細記録")
                
                career_query = f"""
                    SELECT 
                        c.`Year`, c.`Season`, c.`学年`, 
                        mem.`背番号`, mem.`主将`, mem.`投打`, c.`成績`
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` AS mem 
                        ON c.`Player_ID` = mem.`Player_ID` 
                        AND c.`Year` = mem.`Year` 
                        AND c.`Season` = mem.`Season`
                        AND c.`学年` = mem.`学年`
                    WHERE c.`Player_ID` = '{p['Player_ID']}'
                    ORDER BY c.`Year` ASC, c.`学年` ASC
                """
                
                df_career = client.query(career_query).to_dataframe()
                
                # --- プロフィール表示（出場記録から最新の投打を引用） ---
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"### **{p['名前']}** ({p['高校']})")
                    st.write(f"世代: {p['世代']}年 / 出身: {p['出身']}")
                with col2:
                    current_style = "-"
                    if not df_career.empty and '投打' in df_career.columns:
                        current_style = df_career.iloc[-1]['投打'] # 最新大会の投打を表示
                    st.write(f"守備: {p['Position']} / 投打: {current_style}")
                
                st.divider()

                if not df_career.empty:
                    # 主将表示の加工
                    if '主将' in df_career.columns:
                        df_career['役職'] = df_career['主将'].apply(lambda x: "★主将" if str(x) in ["1", "1.0", "主将"] else "-")
                    
                    display_cols = ['Year', 'Season', '学年', '背番号', '投打', '役職', '成績']
                    st.table(df_career[[c for c in display_cols if c in df_career.columns]])
                else:
                    st.info("詳細な出場記録は見つかりませんでした。")
        else:
            st.warning("選手が見つかりませんでした。")
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
else:
    st.info("選手名を入力して検索してください。")
