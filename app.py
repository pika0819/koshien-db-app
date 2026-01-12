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
    name_input = st.text_input("選手名", placeholder="例：立浪和義")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

if name_input or year_input:
    try:
        # 1. 基本情報の検索
        where_clauses = []
        if name_input: where_clauses.append(f"m.`名前` LIKE '%{name_input}%'")
        if year_input: where_clauses.append(f"m.`世代` = {year_input}")
        where_sql = " AND ".join(where_clauses)
        
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_マスタ_基本情報` AS m WHERE {where_sql} LIMIT 50"
        df_players = client.query(query).to_dataframe()

        if not df_players.empty:
            st.subheader("📋 検索結果")
            # 校名を全角カッコに置換して表示
            df_players['display_label'] = df_players['名前'] + " （" + df_players['高校'] + "）"
            selected_label = st.selectbox("選手を選択", options=df_players['display_label'].tolist())
            
            if selected_label:
                p = df_players[df_players['display_label'] == selected_label].iloc[0]
                
                # --- プロフィール表示 ---
                st.markdown(f"## **{p['名前']}** （{p['高校']}）")
                
                # 指名実績の整形（欠損値がある場合は表示しない）
                info_parts = []
                if pd.notna(p.get('球団')) and str(p['球団']) != 'None': 
                    info_parts.append(f"**{p['球団']}**")
                if pd.notna(p.get('ドラフト')) and str(p['ドラフト']) != 'None': 
                    d_year = str(p['ドラフト']).split('.')[0]
                    info_parts.append(f"{d_year}年ドラフト")
                if pd.notna(p.get('順位')) and str(p['順位']) != 'None': 
                    rank_val = str(p['順位'])
                    rank_display = rank_val if "育成" in rank_val else f"{rank_val}位"
                    info_parts.append(rank_display)
                
                if info_parts:
                    st.success(f"🚀 **プロ入り実績:** {' / '.join(info_parts)}")

                # 代表経験
                rep_list = [f"🇯🇵 {c}" for c in ['U12', 'U15', 'U18', 'U22', '侍JAPAN'] if c in p and pd.notna(p[c]) and str(p[c]).strip() not in ["", "None"]]
                if rep_list:
                    st.warning(f"🏅 **代表経験:** {' ／ '.join(rep_list)}")

                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**世代:** {p['世代']}年 / **出身:** {p['出身']}")
                with col2:
                    st.write(f"**ポジション:** {p['Position']} / **進路:** {p.get('進路', '-')}")
                
                st.divider()

                # 2. キャリアとメンバー情報の統合取得
                # データ型の違いを吸収するため、SQL側でCAST（型変換）を行います
                st.subheader("🏟️ 甲子園出場・詳細記録")
                
                career_query = f"""
                    SELECT 
                        c.`Year`, c.`Season`, c.`学年`, 
                        mem.`背番号`, mem.`主将`, mem.`投打`, c.`成績`
                    FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` AS c
                    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` AS mem 
                        ON CAST(c.`Player_ID` AS STRING) = CAST(mem.`Player_ID` AS STRING)
                        AND CAST(c.`Year` AS STRING) = CAST(mem.`Year` AS STRING)
                        AND CAST(c.`Season` AS STRING) = CAST(mem.`Season` AS STRING)
                        AND CAST(c.`学年` AS STRING) = CAST(mem.`学年` AS STRING)
                    WHERE CAST(c.`Player_ID` AS STRING) = '{p['Player_ID']}'
                    ORDER BY c.`Year` ASC, c.`学年` ASC
                """
                df_career = client.query(career_query).to_dataframe()
                
                if not df_career.empty:
                    # 主将表示の加工
                    if '主将' in df_career.columns:
                        df_career['役職'] = df_career['主将'].apply(lambda x: "★主将" if str(x).strip() in ["1", "1.0", "主将", "〇"] else "-")
                    
                    # 見栄えの調整（背番号を文字列にして中央寄せの準備）
                    display_cols = ['Year', 'Season', '学年', '背番号', '投打', '役職', '成績']
                    df_display = df_career[[c for c in display_cols if c in df_career.columns]].copy()
                    
                    # テーブル表示（背番号などを中央に寄せるためのスタイル設定）
                    st.dataframe(
                        df_display,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "背番号": st.column_config.TextColumn("背番号", help="大会時の背番号", width="small"),
                            "Year": st.column_config.TextColumn("Year"),
                            "学年": st.column_config.TextColumn("学年")
                        }
                    )
                else:
                    st.info("詳細な出場記録は見つかりませんでした。")
        else:
            st.warning("選手が見つかりませんでした。")
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
else:
    st.info("選手名を入力して検索してください。")
