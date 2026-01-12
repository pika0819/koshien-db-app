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

# --- ヘルパー関数：テーブルの列名一覧を取得 ---
def get_columns(table_name):
    table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{table_name}")
    return [field.name for field in table.schema]

# --- サイドバー検索 ---
with st.sidebar:
    st.header("🔍 選手検索")
    name_input = st.text_input("選手名", placeholder="例：高橋宏斗")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

if name_input or year_input:
    try:
        # 1. 基本情報の検索
        where_clauses = []
        if name_input: where_clauses.append(f"`名前` LIKE '%{name_input}%'")
        if year_input: where_clauses.append(f"`世代` = {year_input}")
        where_sql = " AND ".join(where_clauses)
        
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` WHERE {where_sql} LIMIT 50"
        df_players = client.query(query).to_dataframe()

        if not df_players.empty:
            st.subheader("📋 検索結果")
            df_players['display_label'] = df_players['名前'] + " (" + df_players['高校'] + ")"
            selected_label = st.selectbox("選手を選択", options=df_players['display_label'].tolist())
            
            if selected_label:
                p = df_players[df_players['display_label'] == selected_label].iloc[0]
                st.markdown(f"### **{p['名前']}** ({p['高校']})")
                st.write(f"世代: {p['世代']}年 / 出身: {p['出身']} / 守備: {p['Position']}")
                st.divider()

                # 2. キャリアとメンバー情報の統合取得（エラー対策版）
                st.subheader("🏟️ 甲子園出場・詳細記録")
                
                # 列名を事前確認
                mem_cols = get_columns("DB_出場メンバー")
                car_cols = get_columns("DB_選手キャリア統合")
                
                # JOIN（結合）のキーとなる列があるかチェック
                join_key = "Career_ID" if "Career_ID" in mem_cols and "Career_ID" in car_cols else None
                
                if join_key:
                    career_query = f"""
                        SELECT c.`Year`, c.`Season`, c.`学年`, mem.`背番号`, mem.`主将フラグ`, c.`成績`
                        FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` AS mem ON c.`{join_key}` = mem.`{join_key}`
                        WHERE c.`Player_ID` = '{p['Player_ID']}'
                        ORDER BY c.`Year` ASC, c.`学年` ASC
                    """
                else:
                    # キーがなければキャリア単体で出す
                    st.warning(f"⚠️ 内部キー不一致のため、出場メンバー情報（背番号等）をスキップします。")
                    career_query = f"""
                        SELECT `Year`, `Season`, `学年`, `成績`
                        FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合`
                        WHERE `Player_ID` = '{p['Player_ID']}'
                        ORDER BY `Year` ASC, `学年` ASC
                    """
                
                df_career = client.query(career_query).to_dataframe()
                if not df_career.empty:
                    if '主将フラグ' in df_career.columns:
                        df_career['役職'] = df_career['主将フラグ'].apply(lambda x: "★主将" if str(x) == "1" else "-")
                    st.table(df_career)
                else:
                    st.info("詳細な出場記録は見つかりませんでした。")
        else:
            st.warning("選手が見つかりませんでした。")
    except Exception as e:
        st.error(f"エラーが発生しました。設定を確認してください。\n{e}")
else:
    st.info("選手名を入力して検索してください。")
