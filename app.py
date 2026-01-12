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
    st.info("全登録メンバーから検索可能です")
    name_input = st.text_input("選手名", placeholder="例：立浪和義")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

if name_input or year_input:
    try:
        # 1. 検索の主役を「キャリア統合（全メンバー）」に切り替え、基本情報を合流させる
        where_clauses = []
        if name_input: where_clauses.append(f"c.`名前` LIKE '%{name_input}%'")
        if year_input: where_clauses.append(f"m.`世代` = {year_input}")
        where_sql = " AND ".join(where_clauses)
        
        # DISTINCTで重複を除去しつつ、全メンバーを網羅
        query = f"""
            SELECT DISTINCT 
                c.`Player_ID`, c.`名前`, c.`School_ID`, m.`高校`, m.`世代`, m.`出身`, m.`Position`,
                m.`球団`, m.`ドラフト`, m.`順位`, m.`進路`,
                m.`U12`, m.`U15`, m.`U18`, m.`U22`, m.`侍JAPAN`
            FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m ON c.`Player_ID` = m.`Player_ID`
            WHERE {where_sql}
            LIMIT 100
        """
        df_players = client.query(query).to_dataframe()

        if not df_players.empty:
            st.subheader(f"📋 該当選手: {len(df_players)}名")
            # 校名の全角カッコ対応
            df_players['高校'] = df_players['高校'].fillna('不明').replace(r'\(', '（', regex=True).replace(r'\)', '）', regex=True)
            df_players['display_label'] = df_players['名前'] + " （" + df_players['高校'] + "）"
            
            selected_label = st.selectbox("詳細を表示する選手を選択", options=df_players['display_label'].tolist())
            
            if selected_label:
                p = df_players[df_players['display_label'] == selected_label].iloc[0]
                
                # --- プロフィール表示（欠損ガード付き） ---
                st.markdown(f"## **{p['名前']}** （{p['高校']}）")
                
                # ドラフト・進路情報の整理
                info_parts = []
                if pd.notna(p.get('球団')) and p['球団'] != 'None': info_parts.append(f"**{p['球団']}**")
                if pd.notna(p.get('ドラフト')) and p['ドラフト'] != 'None': 
                    info_parts.append(f"{str(p['ドラフト']).split('.')[0]}年ドラフト")
                if pd.notna(p.get('順位')) and p['順位'] != 'None':
                    rank = str(p['順位'])
                    info_parts.append(rank if "育成" in rank else f"{rank}位")
                
                if info_parts: st.success(f"🚀 **プロ入り実績:** {' / '.join(info_parts)}")
                
                # 代表歴
                reps = [f"🇯🇵 {c}" for c in ['U12', 'U15', 'U18', 'U22', '侍JAPAN'] if c in p and pd.notna(p[c]) and str(p[c]).strip() not in ["", "None"]]
                if reps: st.warning(f"🏅 **代表経験:** {' ／ '.join(reps)}")

                col1, col2 = st.columns(2)
                with col1: st.write(f"**世代:** {p['世代']}年 / **出身:** {p['出身']}")
                with col2: st.write(f"**ポジション:** {p['Position']} / **進路:** {p.get('進路', '-')}")

                st.divider()

                # --- 激闘の出場記録（重複排除・柔軟結合） ---
                st.subheader("🏟️ 甲子園出場・詳細記録")
                
                # 今朝丸選手の重複（同じ大会が2行出る）を避けるため DISTINCT を使用
                career_query = f"""
                    SELECT DISTINCT
                        c.`Year`, c.`Season`, c.`学年`, 
                        mem.`背番号`, mem.`主将`, mem.`投打`, c.`成績`
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` AS mem 
                        ON c.`Player_ID` = mem.`Player_ID` 
                        AND c.`Year` = mem.`Year` 
                        AND c.`Season` = mem.`Season`
                    WHERE c.`Player_ID` = '{p['Player_ID']}'
                    ORDER BY c.`Year` ASC, c.`Season` DESC
                """
                df_career = client.query(career_query).to_dataframe()

                if not df_career.empty:
                    # 主将判定の強化
                    if '主将' in df_career.columns:
                        df_career['役職'] = df_career['主将'].apply(lambda x: "★主将" if str(x).strip() in ["1", "1.0", "主将", "〇", "1"] else "-")
                    
                    # 中央寄せ＆表示整理
                    df_display = df_career[['Year', 'Season', '学年', '背番号', '投打', '役職', '成績']].copy()
                    st.dataframe(df_display, use_container_width=True, hide_index=True)
                else:
                    st.info("出場記録の詳細は登録されていません。")

    except Exception as e:
        st.error(f"データ取得エラー: {e}")
else:
    st.info("左のサイドバーから全球児を検索できます。")
