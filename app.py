import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

# ページ基本設定
st.set_page_config(page_title="甲子園全記録DB", layout="wide")

# --- BigQueryクライアント ---
@st.cache_resource
def get_bq_client():
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

client = get_bq_client()
PROJECT_ID = "koshien-db"
DATASET_ID = "koshien_data"

# --- 状態管理（どの画面を表示するか） ---
# URLパラメータから現在の「表示モード」を取得
params = st.query_params

# --- ヘルパー：背番号等の右寄せスタイル ---
st.markdown("""
    <style>
    [data-testid="stDataFrame"] td { text-align: right !important; }
    </style>
    """, unsafe_allow_html=True)

# --- 画面分岐ロジック ---

# 1. 高校詳細画面
if "school" in params:
    school_name = params["school"]
    st.button("🔙 検索に戻る", on_click=lambda: st.query_params.clear())
    st.title(f"🏫 {school_name} 歴代の甲子園実績")
    # ここに高校別の集計SQLを書く（次回実装）
    st.info(f"{school_name} の詳細情報は現在準備中です。")

# 2. 大会別出場校画面
elif "tournament" in params:
    t_name = params["tournament"]
    st.button("🔙 検索に戻る", on_click=lambda: st.query_params.clear())
    st.title(f"🏟️ {t_name} 出場校一覧")
    # ここに大会別出場校のSQLを書く（次回実装）

# 3. メイン：選手検索・詳細画面
else:
    st.title("⚾️ 甲子園全記録データベース")
    
    with st.sidebar:
        st.header("🔍 選手検索")
        name_input = st.text_input("選手名", placeholder="例：山田脩也")
        year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

    if name_input or year_input:
        try:
            # 検索
            where_clauses = [f"c.`名前` LIKE '%{name_input}%'"] if name_input else []
            if year_input: where_clauses.append(f"m.`世代` = {year_input}")
            where_sql = " AND ".join(where_clauses)
            
            query = f"""
                SELECT DISTINCT 
                    c.`Player_ID`, c.`名前`, m.`高校`, m.`世代`, m.`出身`, m.`Position`, m.`生年月日`,
                    m.`球団`, m.`ドラフト`, m.`順位`, m.`進路`,
                    m.`U12`, m.`U15`, m.`U18`, m.`U22`, m.`侍JAPAN`
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m ON c.`Player_ID` = m.`Player_ID`
                WHERE {where_sql} LIMIT 100
            """
            df_players = client.query(query).to_dataframe()

            if not df_players.empty:
                df_players['display_label'] = df_players['名前'] + " （" + df_players['高校'].fillna('不明').replace(r'\(', '（', regex=True).replace(r'\)', '）', regex=True) + "）"
                selected_label = st.selectbox("詳細を表示する選手を選択", options=df_players['display_label'].tolist())
                
                if selected_label:
                    p = df_players[df_players['display_label'] == selected_label].iloc[0]
                    
                    # 校名をリンクとして表示
                    school_link = p['高校'] if pd.notna(p['高校']) else "不明"
                    st.markdown(f"## **{p['名前']}** （[{school_link}](/?school={school_link})）")
                    
                    # 生年月日整形
                    bday = "不明"
                    if pd.notna(p.get('生年月日')):
                        try: bday = pd.to_datetime(p['生年月日']).strftime('%Y年%m月%d日')
                        except: bday = str(p['生年月日'])
                    
                    st.write(f"🎂 **生年月日:** {bday} / **出身:** {p.get('出身','None')} / **世代:** {p.get('世代','<NA>')}年")

                    # プロ入り
                    if pd.notna(p.get('球団')) and str(p['球団']) != 'None':
                        d_info = [f"🚀 **{p['球団']}**"]
                        if pd.notna(p.get('ドラフト')): d_info.append(f"{str(p['ドラフト']).split('.')[0]}年")
                        if pd.notna(p.get('順位')): d_info.append(f"{p['順位']}位")
                        st.success(" / ".join(d_info))

                    # 代表歴
                    reps = []
                    for col in ['U12', 'U15', 'U18', 'U22', '侍JAPAN']:
                        val = str(p.get(col, '')).strip()
                        if val and val not in ["None", "nan", "", "0"]:
                            label = col
                            if col == '侍JAPAN' and val.startswith('*'): label = f"侍JAPAN (20{val.replace('*', '')}年)"
                            elif val not in ["1", "◎"]: label = f"{col} （背番号:{val}）"
                            reps.append(f"🇯🇵 {label}")
                    if reps: st.warning(f"🏅 **代表経験:** {' ／ '.join(reps)}")

                    st.divider()
                    st.subheader("🏟️ 甲子園出場・詳細記録")
                    
                    # キャリア詳細（Tournament列があることを想定）
                    career_query = f"""
                        SELECT DISTINCT c.`Year`, c.`Season`, c.`学年`, mem.`背番号`, 
                               mem.`主将` as `mem_capt`, c.`主将` as `car_capt`, mem.`投打`, c.`成績`
                        FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` AS mem 
                            ON c.`Player_ID` = mem.`Player_ID` AND c.`Year` = mem.`Year` AND c.`Season` = mem.`Season`
                        WHERE c.`Player_ID` = '{p['Player_ID']}' ORDER BY c.`Year` ASC
                    """
                    df_career = client.query(career_query).to_dataframe()

                    if not df_career.empty:
                        def judge_captain(row):
                            return "★主将" if "◎" in str(row.get('mem_capt', '')) or "◎" in str(row.get('car_capt', '')) else "-"
                        df_career['役職'] = df_career.apply(judge_captain, axis=1)
                        
                        # 背番号等の表示列を右寄せ・整理
                        display_cols = ['Year', 'Season', '学年', '背番号', '投打', '役職', '成績']
                        st.dataframe(df_career[display_cols], use_container_width=True, hide_index=True)
                    else:
                        st.info("出場記録はありません。")

        except Exception as e:
            st.error(f"エラー: {e}")
    else:
        st.info("選手名を入力して検索してください。")
