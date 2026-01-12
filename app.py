import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ページ基本設定
st.set_page_config(page_title="究極・甲子園DB", layout="wide")
st.title("⚾️ 究極・甲子園全記録名鑑")

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
    
    # 基本情報と実績を結合して検索
    query = f"""
        SELECT m.`UUID`, m.`名前`, m.`高校`, m.`世代`, m.`出身`, m.`Position`, 
               r.`投打`, r.`身長`, r.`体重`, r.`中学所属`
        FROM `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_選手実績` AS r ON m.`Player_ID` = r.`Player_ID`
        WHERE {where_sql}
        LIMIT 50
    """
    
    try:
        df_players = client.query(query).to_dataframe()

        if not df_players.empty:
            st.subheader("📋 検索結果")
            # 選択用ラベル
            df_players['display_name'] = df_players['名前'] + " (" + df_players['高校'] + ")"
            selected_label = st.selectbox("選手を選択して詳細を表示", options=df_players['display_name'].tolist())
            
            if selected_label:
                p = df_players[df_players['display_name'] == selected_label].iloc[0]
                
                # --- プロフィール表示 ---
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"### **{p['名前']}**")
                    st.write(f"**所属:** {p['高校']} ({p['世代']}年世代)")
                    st.write(f"**出身:** {p['出身']} / **守備:** {p['Position']}")
                with col2:
                    st.write(f"**投打:** {p.get('投打', '不明')} / **中学:** {p.get('中学所属', '不明')}")
                    st.write(f"**体格:** {p.get('身長', '-')}cm / {p.get('体重', '-')}kg")

                st.divider()

                # --- 出場メンバー＆キャリア情報の統合表示 ---
                st.subheader("🏟️ 甲子園出場・キャリア記録")
                
                # キャリアと出場メンバー情報を結合（主将・背番号を取得）
                career_query = f"""
                    SELECT c.`Year`, c.`Season`, c.`学年`, 
                           mem.`背番号`, mem.`主将フラグ`, c.`成績`
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` AS mem 
                        ON c.`Career_ID` = mem.`Career_ID`
                    WHERE c.`Player_ID` = (
                        SELECT `Player_ID` FROM `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` WHERE `UUID` = '{p['UUID']}'
                    )
                    ORDER BY c.`Year` ASC, c.`学年` ASC
                """
                df_career = client.query(career_query).to_dataframe()

                if not df_career.empty:
                    # 主将フラグがある場合に「★主将」と表示する加工
                    if '主将フラグ' in df_career.columns:
                        df_career['役職'] = df_career['主将フラグ'].apply(lambda x: "★主将" if x == 1 or x == "1" else "-")
                    
                    # 見やすい列順に整理
                    display_cols = ['Year', 'Season', '学年', '背番号', '役職', '成績']
                    st.table(df_career[[c for c in display_cols if c in df_career.columns]])
                else:
                    st.info("出場記録の詳細は登録されていません。")

        else:
            st.warning("該当する選手は見つかりませんでした。")
    except Exception as e:
        st.error(f"データ統合エラー: {e}")
else:
    st.info("選手名を入力して検索してください。")
