import streamlit as st
from google.cloud import bigquery

# ページ設定
st.set_page_config(page_title="甲子園DB", layout="wide")
st.title("⚾️ 甲子園全記録データベース")

# BigQueryクライアントの初期化
@st.cache_resource
def get_bq_client():
    # StreamlitのSecretsから認証情報を読み込む
    return bigquery.Client.from_service_account_info(st.secrets["gcp_service_account"])

try:
    client = get_bq_client()

    # サイドバー検索
    with st.sidebar:
        st.header("検索パネル")
        name_input = st.text_input("選手名を入力", placeholder="例：沢村栄治")

    if name_input:
        # SQLクエリ（プロジェクトIDなどは適宜修正が必要な場合があります）
        query = f"""
            SELECT `名前`, `高校`, `世代`, `出身`, `Position`
            FROM `koshien-db.koshien_data.DB_マスタ_基本情報`
            WHERE `名前` LIKE '%{name_input}%'
        """
        df = client.query(query).to_dataframe()

        if not df.empty:
            st.write(f"### 「{name_input}」の検索結果")
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("選手が見つかりませんでした。")
    else:
        st.info("左の検索パネルから選手名を入力してください。")

except Exception as e:
    st.error("認証エラー：Streamlit CloudのSecrets設定が必要です。")
    st.info("まずはデプロイ後、管理画面でGoogle CloudのJSONキーを設定してください。")
