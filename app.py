import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ページ基本設定
st.set_page_config(page_title="甲子園全記録DB", layout="wide")

st.title("⚾️ 甲子園全記録データベース")

# BigQueryクライアント初期化
@st.cache_resource
def get_bq_client():
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

# --- サイドバー：検索・フィルタ ---
with st.sidebar:
    st.header("🔍 選手を探す")
    name_input = st.text_input("選手名", placeholder="例：沢村栄治")
    
    # 世代（西暦）での絞り込みも追加
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

# --- メイン処理 ---
# 検索クエリの組み立て
where_clauses = []
if name_input:
    where_clauses.append(f"`名前` LIKE '%{name_input}%'")
if year_input:
    where_clauses.append(f"`世代` = {year_input}")

if where_clauses:
    where_sql = " AND ".join(where_clauses)
    query = f"""
        SELECT `UUID`, `名前`, `高校`, `世代`, `出身`, `Position`
        FROM `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報`
        WHERE {where_sql}
        LIMIT 100
    """
    
    df_players = client.query(query).to_dataframe()

    if not df_players.empty:
        st.subheader("選手一覧")
        st.write("詳細を見たい選手を選択してください：")
        
        # 選択用のデータフレーム表示
        # column_configを使ってUUIDを隠しつつ選択可能にする
        selected_rows = st.dataframe(
            df_players,
            hide_index=True,
            on_select="rerun",
            selection_mode="single_row",
            use_container_width=True,
            column_config={"UUID": None} # UUIDは裏側で使うので非表示
        )

        # 選手が選択された場合の処理
        if len(selected_rows.selection.rows) > 0:
            selected_idx = selected_rows.selection.rows[0]
            player_uuid = df_players.iloc[selected_idx]["UUID"]
            player_name = df_players.iloc[selected_idx]["名前"]

            st.divider()
            st.subheader(f"🛡️ {player_name} 選手のキャリア実績")

            # キャリア統合テーブルからデータを取得
            career_query = f"""
                SELECT `Year`, `Season`, `学年`, `背番号`, `成績`
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合`
                WHERE `Player_ID` = (
                    SELECT `Player_ID` FROM `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` WHERE `UUID` = '{player_uuid}'
                )
                ORDER BY `Year` ASC, `学年` ASC
            """
            df_career = client.query(career_query).to_dataframe()

            if not df_career.empty:
                st.table(df_career) # タイムライン風にテーブル表示
            else:
                st.info("キャリア詳細は登録されていません。")
    else:
        st.warning("該当する選手が見つかりませんでした。")
else:
    st.info("サイドバーから選手名や世代を入力して検索を開始してください。")
