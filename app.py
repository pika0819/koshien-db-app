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

# 右寄せCSS
st.markdown("<style>[data-testid='stDataFrame'] td { text-align: right !important; }</style>", unsafe_allow_html=True)

with st.sidebar:
    st.header("🔍 選手検索")
    name_input = st.text_input("選手名", placeholder="例：古城大翔")
    year_input = st.number_input("世代（西暦）", min_value=1915, max_value=2026, value=None, step=1)

if name_input or year_input:
    try:
        # 【修正の肝】検索は「DB_選手キャリア統合」だけを対象にする
        # ここに全項目の統合データがあるため、JOINは不要
        where_clauses = [f"`名前` LIKE '%{name_input}%'"] if name_input else []
        if year_input: where_clauses.append(f"`世代` = {year_input}")
        where_sql = " AND ".join(where_clauses)
        
        # すべての列をキャリア統合シートから持ってくる
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_選手キャリア統合` WHERE {where_sql} LIMIT 100"
        df_results = client.query(query).to_dataframe()

        if not df_results.empty:
            # 検索結果をPlayer_IDごとにまとめる
            player_list = df_results[['Player_ID', '名前', '高校']].drop_duplicates()
            player_list['display_label'] = player_list['名前'] + " （" + player_list['高校'].fillna('不明') + "）"
            
            selected_label = st.selectbox("選手を選択", options=player_list['display_label'].tolist())
            
            if selected_label:
                # 選択された選手の全データを抽出
                target_pid = player_list[player_list['display_label'] == selected_label].iloc[0]['Player_ID']
                p_data = df_results[df_results['Player_ID'] == target_pid]
                
                # プロフィール（1行目から取得）
                p = p_data.iloc[0]
                st.markdown(f"## **{p['名前']}** （{p['高校']}）")
                
                # 生年月日・出身などの基本情報もキャリア統合シート内の列から表示
                bday = "不明"
                if '生年月日' in p and pd.notna(p['生年月日']):
                    try: bday = pd.to_datetime(p['生年月日']).strftime('%Y年%m月%d日')
                    except: bday = str(p['生年月日'])
                
                st.write(f"🎂 **生年月日:** {bday} / 📍 **出身:** {p.get('出身','不明')} / **世代:** {p.get('世代','<NA>')}年")

                # プロ入り実績（キャリア統合シート内に列があれば表示）
                if '球団' in p and pd.notna(p['球団']) and str(p['球団']) != 'None':
                    draft_info = f"🚀 **{p['球団']}**"
                    if pd.notna(p.get('ドラフト')): draft_info += f" / {str(p['ドラフト']).split('.')[0]}年"
                    if pd.notna(p.get('順位')): draft_info += f" / {p['順位']}位"
                    st.success(draft_info)

                st.divider()
                st.subheader("🏟️ 出場・詳細記録")
                
                # 重複を含めた全キャリア履歴を表示
                # 背番号、投打、成績、役職（◎判定）などを一覧化
                display_df = p_data[['Year', 'Season', '学年', '背番号', '投打', '成績']].copy()
                
                # 役職（◎）の判定
                if '主将' in p_data.columns:
                    display_df['役職'] = p_data['主将'].apply(lambda x: "★主将" if "◎" in str(x) else "-")
                
                st.dataframe(display_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"エラー: {e}")
