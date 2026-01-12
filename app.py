import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ページ基本設定
st.set_page_config(page_title="甲子園全記録DB", layout="wide")
st.title("⚾️ 甲子園全記録データベース")

@st.cache_resource
def get_bq_client():
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

# --- サイドバー検索 ---
with st.sidebar:
    st.header("🔍 選手検索")
    name_input = st.text_input("選手名", placeholder="例：高橋宏斗")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

# --- 検索処理 ---
if name_input or year_input:
    where_clauses = []
    if name_input: where_clauses.append(f"m.`名前` LIKE '%{name_input}%'")
    if year_input: where_clauses.append(f"m.`世代` = {year_input}")
    
    where_sql = " AND ".join(where_clauses)
    
    # 【修正】まずは基本情報だけで検索。エラーが出やすい実績シートは個別で慎重に取得します
    query = f"""
        SELECT m.`UUID`, m.`Player_ID`, m.`名前`, m.`高校`, m.`世代`, m.`出身`, m.`Position`
        FROM `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m
        WHERE {where_sql}
        LIMIT 50
    """
    
    try:
        df_players = client.query(query).to_dataframe()

        if not df_players.empty:
            st.subheader("📋 検索結果")
            df_players['display_name'] = df_players['名前'] + " (" + df_players['高校'] + ")"
            selected_label = st.selectbox("選手を選択して詳細を表示", options=df_players['display_name'].tolist())
            
            if selected_label:
                p = df_players[df_players['display_name'] == selected_label].iloc[0]
                
                # --- プロフィール表示 ---
                st.markdown(f"### **{p['名前']}** ({p['高校']})")
                st.write(f"**世代:** {p['世代']}年 / **出身:** {p['出身']} / **守備:** {p['Position']}")

                st.divider()

                # --- 出場メンバー＆キャリア情報の統合表示 ---
                # 主将フラグや背番号など、持っているデータを全て引き出します
                st.subheader("🏟️ 甲子園出場・詳細記録")
                
                career_query = f"""
                    SELECT c.`Year`, c.`Season`, c.`学年`, 
                           mem.`背番号`, mem.`主将フラグ`, c.`成績`
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` AS mem 
                        ON c.`Career_ID` = mem.`Career_ID`
                    WHERE c.`Player_ID` = '{p['Player_ID']}'
                    ORDER BY c.`Year` ASC, c.`学年` ASC
                """
                df_career = client.query(career_query).to_dataframe()

                if not df_career.empty:
                    # 主将フラグを「★主将」に変換
                    if '主将フラグ' in df_career.columns:
                        df_career['役職'] = df_career['主将フラグ'].apply(lambda x: "★主将" if str(x) == "1" else "-")
                    
                    display_cols = ['Year', 'Season', '学年', '背番号', '役職', '成績']
                    st.table(df_career[[c for c in display_cols if c in df_career.columns]])
                else:
                    st.info("出場記録の詳細は登録されていません。")

        else:
            st.warning("該当する選手が見つかりませんでした。")
    except Exception as e:
        st.error(f"エラーが発生しました: {e}")
else:
    st.info("左のサイドバーから検索してください。")
