import streamlit as st
from google.cloud import bigquery
import pandas as pd
import google.oauth2.service_account

# ページ設定
st.set_page_config(page_title="甲子園DB", layout="wide")
st.title("⚾️ 甲子園DB - 大会検索")

# --- 1. BigQuery接続設定 ---
@st.cache_resource
def get_bq_client():
    try:
        scopes = [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=scopes
        )
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception as e:
        st.error(f"認証エラー: {e}")
        st.stop()

client = get_bq_client()
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
DATASET_ID = "koshien_data"

# --- 2. データ取得関数群 ---

# 大会リスト取得
@st.cache_data(ttl=600)
def get_tournaments():
    # 全列取得し、重複を排除
    sql = "SELECT * FROM `{}.{}.m_tournament` ORDER BY SAFE_CAST(Year AS INT64) DESC, Season DESC".format(PROJECT_ID, DATASET_ID)
    df = client.query(sql).to_dataframe().drop_duplicates()
    return df

# 出場校一覧取得
@st.cache_data(ttl=600)
def get_results_list(tournament_name):
    # 【重要】空白や表記揺れ対策として TRIM を入れ、確実に比較する
    sql = """
    SELECT tr.*, s.School_Name_Now
    FROM `{0}.{1}.t_results` AS tr
    LEFT JOIN `{0}.{1}.m_school` AS s ON tr.School_ID = s.School_ID
    WHERE TRIM(tr.Tournament) = @tournament
    """.format(PROJECT_ID, DATASET_ID)
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("tournament", "STRING", tournament_name.strip())]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    df = df.drop_duplicates()

    # スプシの列名に応じたマッピング
    rename_map = {
        'District': '地区',
        'School_Name_Then': '校名',
        'School_Name_Now': '現在校名',
        'History_Label': '出場回数',
        'Rank': '成績'
    }
    
    available_cols = [c for c in rename_map.keys() if c in df.columns]
    df_display = df[available_cols].rename(columns=rename_map)
    if 'School_ID' in df.columns:
        df_display['School_ID'] = df['School_ID']
        
    return df_display

# 詳細データ取得（各タブ用）
@st.cache_data(ttl=600)
def get_school_details(school_id, tournament_name):
    queries = {
        "scores": "SELECT * FROM `{0}.{1}.t_scores` WHERE TRIM(Tournament) = @tournament AND School_ID = @school_id".format(PROJECT_ID, DATASET_ID),
        "members": "SELECT * FROM `{0}.{1}.m_player` WHERE TRIM(Tournament) = @tournament AND School_ID = @school_id".format(PROJECT_ID, DATASET_ID),
        "history": "SELECT * FROM `{0}.{1}.t_results` WHERE School_ID = @school_id ORDER BY SAFE_CAST(Year AS INT64) DESC".format(PROJECT_ID, DATASET_ID),
        "alumni": "SELECT * FROM `{0}.{1}.m_player` WHERE School_ID = @school_id AND (Pro_Team IS NOT NULL AND Pro_Team != '')".format(PROJECT_ID, DATASET_ID)
    }

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tournament", "STRING", tournament_name.strip()),
            bigquery.ScalarQueryParameter("school_id", "STRING", school_id)
        ]
    )

    results = {}
    for key, sql in queries.items():
        results[key] = client.query(sql, job_config=job_config).to_dataframe().drop_duplicates()
    return results

# --- 3. UI構築 ---

st.sidebar.header("🔍 設定")
df_tourney = get_tournaments()

if not df_tourney.empty:
    # 列名の存在確認
    t_col = 'Tournament' if 'Tournament' in df_tourney.columns else df_tourney.columns[0]
    y_col = 'Year' if 'Year' in df_tourney.columns else df_tourney.columns[1]
    s_col = 'Season' if 'Season' in df_tourney.columns else df_tourney.columns[2]
    
    # 選択肢の作成
    df_tourney = df_tourney.fillna('')
    tourney_options = ["{} {} - {}".format(row[y_col], row[s_col], row[t_col]) for _, row in df_tourney.iterrows()]
    selected_option = st.sidebar.selectbox("大会を選択", tourney_options)
    
    # 「 - 」で分割して「大会名」だけを正確に取り出す
    selected_tourney_name = selected_option.split(" - ")[1]
else:
    st.error("大会データが取得できません")
    st.stop()

# メイン画面
st.subheader(f"🏟 {selected_tourney_name} 出場校一覧")

# デバッグ用（表示を確認したらコメントアウトしてください）
# st.write(f"検索キー: 『{selected_tourney_name}』")

df_list = get_results_list(selected_tourney_name)

if not df_list.empty:
    # 一覧表示（存在する列のみ）
    display_cols = [c for c in ["地区", "校名", "現在校名", "出場回数", "成績"] if c in df_list.columns]
    st.dataframe(df_list[display_cols], use_container_width=True, hide_index=True)

    st.markdown("---")
    st.write("🔽 **詳細を見たい高校を選択してください**")
    
    # 「校名」と「School_ID」のペアを作成
    if '校名' in df_list.columns and 'School_ID' in df_list.columns:
        school_options = dict(zip(df_list['校名'], df_list['School_ID']))
        selected_school_name = st.selectbox("高校を選択", list(school_options.keys()))
        school_id = school_options[selected_school_name]
        
        with st.spinner(f'{selected_school_name} のデータを取得中...'):
            details = get_school_details(school_id, selected_tourney_name)
        
        tab1, tab2, tab3, tab4 = st.tabs(["⚾️ 戦績", "👥 メンバー", "📜 過去成績", "🌟 卒業生"])
        
        with tab1: # 戦績 (t_scores)
            if not details["scores"].empty:
                st.dataframe(details["scores"], use_container_width=True, hide_index=True)
            else:
                st.info("この大会の戦績データはありません。")
        with tab2: # メンバー (m_player)
            if not details["members"].empty:
                st.dataframe(details["members"], use_container_width=True, hide_index=True)
            else:
                st.info("この大会のメンバーデータはありません。")
        with tab3: # 過去成績 (t_results)
            st.dataframe(details["history"], use_container_width=True, hide_index=True)
        with tab4: # 卒業生 (m_player)
            if not details["alumni"].empty:
                st.dataframe(details["alumni"], use_container_width=True, hide_index=True)
            else:
                st.info("プロ入りした卒業生データはありません。")
else:
    st.warning(f"「{selected_tourney_name}」のデータが見つかりませんでした。スプレッドシートのTournament列の表記を確認してください。")
