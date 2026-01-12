import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

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
    name_input = st.text_input("選手名", placeholder="例：山田脩也")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

if name_input or year_input:
    try:
        # 選手検索クエリ
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
                
                # --- プロフィール表示 ---
                st.markdown(f"## **{p['名前']}** （{p['高校']}）")
                
                # 1. 生年月日の整形 (2002年08月19日 形式)
                bday_display = "不明"
                if pd.notna(p.get('生年月日')):
                    try:
                        bday_dt = pd.to_datetime(p['生年月日'])
                        bday_display = bday_dt.strftime('%Y年%m月%d日')
                    except:
                        bday_display = str(p['生年月日'])
                
                st.write(f"🎂 **生年月日:** {bday_display} / **出身:** {p['出身']} / **世代:** {p['世代']}年")

                # 2. プロ入り実績 (育成も位を表示)
                if pd.notna(p.get('球団')):
                    draft_parts = [f"🚀 **{p['球団']}**"]
                    if pd.notna(p.get('ドラフト')): draft_parts.append(f"{str(p['ドラフト']).split('.')[0]}年")
                    if pd.notna(p.get('順位')):
                        r = str(p['順位'])
                        # 育成Xもそのまま「位」を付ける (例: 育成4位)
                        draft_parts.append(f"{r}位")
                    st.success(" / ".join(draft_parts))

                # 3. 代表歴解読
                reps = []
                for col in ['U12', 'U15', 'U18', 'U22', '侍JAPAN']:
                    val = str(p.get(col, '')).strip()
                    if val and val not in ["None", "nan", ""]:
                        label = col
                        if col == '侍JAPAN' and val.startswith('*'):
                            label = f"侍JAPAN (20{val.replace('*', '')}年)"
                        elif val not in ["1", "〇", "◎"]:
                            label = f"{col} (背番号:{val})"
                        reps.append(f"🇯🇵 {label}")
                if reps: st.warning(f"🏅 **代表経験:** {' ／ '.join(reps)}")

                st.divider()

                # --- 詳細な出場記録 ---
                st.subheader("🏟️ 甲子園出場・詳細記録")
                
                # キャリア統合(c)と出場メンバー(mem)の両方の主将・主主将列をチェック
                career_query = f"""
                    SELECT DISTINCT c.`Year`, c.`Season`, c.`学年`, mem.`背番号`, 
                           mem.`主将` as `mem_capt`, c.`主主将` as `car_capt`, mem.`投打`, c.`成績`
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` AS mem 
                        ON c.`Player_ID` = mem.`Player_ID` AND c.`Year` = mem.`Year` AND c.`Season` = mem.`Season`
                    WHERE c.`Player_ID` = '{p['Player_ID']}' ORDER BY c.`Year` ASC
                """
                df_career = client.query(career_query).to_dataframe()

                if not df_career.empty:
                    # 主将判定ロジック強化 (◎を追加)
                    def judge_captain(row):
                        # チェック対象の値をリスト化
                        vals = [str(row.get('mem_capt', '')), str(row.get('car_capt', ''))]
                        capt_marks = ["1", "1.0", "主将", "〇", "◎"]
                        if any(any(m in v for m in capt_marks) for v in vals):
                            return "★主将"
                        return "-"

                    df_career['役職'] = df_career.apply(judge_captain, axis=1)
                    
                    # 表示列の整理
                    display_cols = ['Year', 'Season', '学年', '背番号', '投打', '役職', '成績']
                    st.dataframe(df_career[[c for c in display_cols if c in df_career.columns]], use_container_width=True, hide_index=True)
                else:
                    st.info("出場記録の詳細はありません。")

    except Exception as e:
        st.error(f"データ取得エラー: {e}")
else:
    st.info("選手名を入力して検索してください。")
