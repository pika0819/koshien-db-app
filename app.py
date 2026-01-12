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
    name_input = st.text_input("選手名", placeholder="例：坂本勇人")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

# --- メイン処理 ---
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
        LIMIT 50
    """
    
    try:
        df_players = client.query(query).to_dataframe()

        if not df_players.empty:
            st.subheader("選手一覧")
            # 検索結果を一覧表示
            st.dataframe(df_players.drop(columns=['UUID']), use_container_width=True)
            
            # --- 選手詳細の表示（ラジオボタンで選択） ---
            st.divider()
            st.write("### 🛡️ キャリア詳細を表示する選手を選択")
            
            # 選択肢用のラベル作成（例：坂本勇人 (光星学院)）
            df_players['label'] = df_players['名前'] + " (" + df_players['高校'] + ")"
            selected_label = st.selectbox("詳細を見たい選手を選んでください", options=df_players['label'].tolist())
            
            if selected_label:
                # 選択された選手のUUIDを取得
                selected_player = df_players[df_players['label'] == selected_label].iloc[0]
                player_uuid = selected_player['UUID']
                player_name = selected_player['名前']

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

                st.write(f"#### {player_name} 選手の出場記録")
                if not df_career.empty:
                    st.table(df_career)
                else:
                    st.info("キャリア詳細は登録されていません。")
        else:
            st.warning("該当する選手が見つかりませんでした。")
    except Exception as e:
        st.error(f"データ取得中にエラーが発生しました。")
        st.info("列名が正しくBigQueryに登録されているか確認してください。")
else:
    st.info("左のサイドバーから選手名を入力して検索を開始してください。")
