# ==========================================
# 🏆 モード: 大会記録
# ==========================================
if mode == "🏆 大会から探す":
    st.subheader("🏆 大会記録・出場校チェック")
    
    try:
        df_years = client.query(f"SELECT DISTINCT Year FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` ORDER BY Year DESC").to_dataframe()
        years_list = df_years['Year'].tolist()
    except:
        years_list = []

    col1, col2 = st.columns(2)
    with col1: sel_year = st.selectbox("年度", years_list)
    with col2: sel_season = st.selectbox("季節", ["夏", "春"])
    
    if sel_year and sel_season:
        # 大会情報の取得
        t_info = client.query(f"SELECT Tournament, Champion FROM `{PROJECT_ID}.{DATASET_ID}.DB_大会マスタ` WHERE Year = '{sel_year}' AND Season = '{sel_season}'").to_dataframe()
        if not t_info.empty:
            champ = t_info.iloc[0].get('Champion', '不明')
            st.info(f"🚩 **{t_info.iloc[0]['Tournament']}** （優勝：{champ}）")
            
            # ---------------------------------------------------------
            # 【修正点】ここで重複を排除する（DISTINCT）
            # 試合ごとのスコア(Game_Scores)は一覧には出さず、ドリルダウンに回す
            # ---------------------------------------------------------
            df_res = client.query(f"""
                SELECT DISTINCT School, School_ID, Result, History_Label
                FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                WHERE Year = '{sel_year}' AND Season = '{sel_season}'
                ORDER BY School_ID ASC
            """).to_dataframe()
            
            st.write(f"👇 **出場 {len(df_res)} 校** （クリックで詳細表示）")

            # 欠損値埋め
            if 'History_Label' not in df_res.columns: df_res['History_Label'] = '-'
            
            # 一覧表用のデータ（Game_Scoresは外してスッキリさせる）
            display_df = df_res[['School', 'History_Label', 'Result']].rename(columns={
                'School': '高校名', 'History_Label': '出場情報', 'Result': '最高成績'
            })
            
            selection = st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row"
            )
            
            # --- ドリルダウン詳細 ---
            if len(selection.selection.rows) > 0:
                row_idx = selection.selection.rows[0]
                row_data = df_res.iloc[row_idx]
                target_sid = row_data['School_ID']
                
                st.divider()
                st.markdown(f"## 🏫 **{row_data['School']}**")
                st.info(row_data['History_Label'])
                
                tab1, tab2, tab3 = st.tabs(["⚾ この大会の戦績", "🦁 当時のメンバー", "📜 過去の歩み"])
                
                # タブ1: この大会の全試合結果（ここでスコアを見せる）
                with tab1:
                    games_query = f"""
                        SELECT Round, Match_Date, Opponent, Score, Win_Loss
                        FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                        WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'
                        ORDER BY Round ASC
                    """
                    # Game_Scoresがある場合とない場合に対応
                    # 簡易的に全列取得して表示
                    try:
                        df_games = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ` WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'").to_dataframe()
                        # 表示したい列だけピックアップ
                        cols_show = {'Round':'回戦', 'Opponent':'対戦校', 'Score':'スコア', 'Win_Loss':'勝敗', 'Game_Scores':'詳細'}
                        valid_cols = {k:v for k,v in cols_show.items() if k in df_games.columns}
                        st.dataframe(df_games[valid_cols.keys()].rename(columns=valid_cols), use_container_width=True, hide_index=True)
                    except:
                        st.write("戦績データの取得に失敗")

                with tab2:
                    # メンバー表
                    m_query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.DB_出場メンバー` WHERE School_ID = '{target_sid}' AND Year = '{sel_year}' AND Season = '{sel_season}'"
                    df_mem = client.query(m_query).to_dataframe()
                    if not df_mem.empty:
                        rename_map = {'Name':'氏名','Grade':'学年','Uniform_Number':'背番号','Position':'守備','Throw_Bat':'投打','Captain':'役職'}
                        valid_cols = {k:v for k,v in rename_map.items() if k in df_mem.columns}
                        if 'Uniform_Number' in df_mem.columns:
                            df_mem = df_mem.sort_values('Uniform_Number', key=lambda x: pd.to_numeric(x, errors='coerce'))
                        if 'Captain' in df_mem.columns:
                            df_mem['Captain'] = df_mem['Captain'].apply(lambda x: "★主将" if "◎" in str(x) else "")
                        st.dataframe(df_mem[valid_cols.keys()].rename(columns=valid_cols), use_container_width=True, hide_index=True)
                    else:
                        st.warning("メンバーデータなし")
                
                with tab3:
                    # 過去戦績
                    h_query = f"""
                        SELECT Year, Season, Result, Game_Scores, History_Label
                        FROM `{PROJECT_ID}.{DATASET_ID}.DB_戦績データ`
                        WHERE School_ID = '{target_sid}' AND (Year < {sel_year} OR (Year = {sel_year} AND Season != '{sel_season}'))
                        ORDER BY Year DESC, Season DESC LIMIT 10
                    """
                    try:
                        df_hist = client.query(h_query).to_dataframe()
                        # 重複排除（念のため）
                        df_hist = df_hist.drop_duplicates(subset=['Year', 'Season'])
                        st.dataframe(df_hist.rename(columns={'Year':'年度','Season':'季','Result':'成績','Game_Scores':'詳細','History_Label':'当時'}), use_container_width=True, hide_index=True)
                    except:
                        st.info("履歴データなし")
