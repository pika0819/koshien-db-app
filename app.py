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

# --- URLパラメータによる画面遷移の制御 ---
params = st.query_params

# --- 右寄せCSS ---
st.markdown("""
    <style>
    [data-testid="stDataFrame"] td { text-align: right !important; }
    </style>
    """, unsafe_allow_html=True)

# 1. 高校詳細画面
if "school" in params:
    school_name = params["school"]
    if st.button("🔙 検索に戻る"):
        st.query_params.clear()
        st.rerun()
    st.title(f"🏫 {school_name} 歴代の甲子園実績")
    st.info(f"{school_name} の詳細データ（歴代勝敗・選手一覧）を構築中です。")

# 2. 大会詳細画面
elif "tournament" in params:
    t_key = params["tournament"]
    if st.button("🔙 検索に戻る"):
        st.query_params.clear()
        st.rerun()
    st.title(f"🏟️ 大会詳細: {t_key}")
    st.info(f"{t_key} の全出場校・トーナメント表を構築中です。")

# 3. メイン：選手検索・詳細画面
else:
    st.title("⚾️ 甲子園全記録データベース")
    
    with st.sidebar:
        st.header("🔍 選手検索")
        name_input = st.text_input("選手名", placeholder="例：山田脩也")
        year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

    if name_input or year_input:
        try:
            where_clauses = [f"c.`名前` LIKE '%{name_input}%'"] if name_input else []
            if year_input: where_clauses.append(f"m.`世代` = {year_input}")
            where_sql = " AND ".join(where_clauses)
            
            query = f"""
                SELECT DISTINCT 
                    c.`Player_ID`, c.`名前`, c.`School_ID`, m.`高校`, m.`世代`, m.`出身`, m.`Position`, m.`生年月日`,
                    m.`球団`, m.`ドラフト`, m.`順位`, m.`進路`,
                    m.`U12`, m.`U15`, m.`U18`, m.`U22`, m.`侍JAPAN`
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m ON c.`Player_ID` = m.`Player_ID`
                WHERE {where_sql} LIMIT 100
            """
            df_players = client.query(query).to_dataframe()

            if not df_players.empty:
                # 校名がマスタにない場合はキャリアから補完
                df_players['高校'] = df_players['高校'].fillna('不明').replace(r'\(', '（', regex=True).replace(r'\)', '）', regex=True)
                df_players['display_label'] = df_players['名前'] + " （" + df_players['高校'] + "）"
                selected_label = st.selectbox("選手を選択", options=df_players['display_label'].tolist())
                
                if selected_label:
                    p = df_players[df_players['display_label'] == selected_label].iloc[0]
                    
                    # 校名をリンク化
                    school_display = p['高校']
                    st.markdown(f"## **{p['名前']}** （[{school_display}](/?school={school_display})）")
                    
                    # プロフィール表示（欠損を「不明」等にせず、データがある時だけ出す）
                    profile_line = []
                    if pd.notna(p.get('生年月日')) and str(p['生年月日']) != 'None':
                        try: bday = pd.to_datetime(p['生年月日']).strftime('%Y年%m月%d日')
                        except: bday = str(p['生年月日'])
                        profile_line.append(f"🎂 **生年月日:** {bday}")
                    
                    if pd.notna(p.get('出身')) and str(p['出身']) != 'None':
                        profile_line.append(f"📍 **出身:** {p['出身']}")
                    
                    if profile_line:
                        st.write(" / ".join(profile_line))

                    # プロ入り実績
                    if pd.notna(p.get('球団')) and str(p['球団']) != 'None':
                        d_parts = [f"🚀 **{p['球団']}**"]
                        if pd.notna(p.get('ドラフト')): d_parts.append(f"{str(p['ドラフト']).split('.')[0]}年")
                        if pd.notna(p.get('順位')): d_parts.append(f"{p['順位']}位")
                        st.success(" / ".join(d_parts))

                    # 代表歴（全角カッコ対応）
                    reps = []
                    for col in ['U12', 'U15', 'U18', 'U22', '侍JAPAN']:
                        val = str(p.get(col, '')).strip()
                        if val and val not in ["None", "nan", "", "0"]:
                            label = col
                            if col == '侍JAPAN' and val.startswith('*'): label = f"侍JAPAN （20{val.replace('*', '')}年）"
                            elif val not in ["1", "◎"]: label = f"{col} （背番号:{val}）"
                            reps.append(f"🇯🇵 {label}")
                    if reps: st.warning(f"🏅 **代表経験:** {' ／ '.join(reps)}")

                    st.divider()
                    st.subheader("🏟️ 甲子園出場・詳細記録")
                    
                    # Tournament情報を取得。 c.`Tournament` がある想定
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
                        
                        # 表示列を右寄せにするため、数値型を文字列に変換
                        df_display = df_career[['Year', 'Season', '学年', '背番号', '投打', '役職', '成績']].copy()
                        st.dataframe(df_display, use_container_width=True, hide_index=True)
                    else:
                        st.info("詳細な出場記録はありません。")

        except Exception as e:
            st.error(f"エラー: {e}")
