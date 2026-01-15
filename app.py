# --- 1. BigQuery接続設定 (修正版) ---
@st.cache_resource
def get_bq_client():
    try:
        import google.auth
        # 認証スコープを明示的に指定（BigQuery + ドライブ + スプレッドシート）
        scopes = [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        
        # サービスアカウント情報とスコープを組み合わせて認証情報を作成
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=scopes
        )
        
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception as e:
        st.error(f"認証エラー: {e}")
        st.stop()
