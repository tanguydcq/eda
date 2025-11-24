import streamlit as st
import pandas as pd
import tools
import os

# --- Configuration ---
st.set_page_config(
    page_title="Exploration de motifs fréquents",
    page_icon="🧠",
    layout="wide"
)

# --- Helper Affichage & Export ---
def format_df_for_display(df):
    """Convertit frozensets en strings pour affichage et export CSV"""
    if df is None: return None
    df_disp = df.copy()
    cols = ['itemset', 'antecedents', 'consequents', 'antecedent', 'consequent']
    for col in cols:
        if col in df_disp.columns:
            df_disp[col] = df_disp[col].apply(
                lambda x: ', '.join(list(x)) if isinstance(x, (set, frozenset)) else str(x)
            )
    return df_disp

# --- Titre ---
st.title("🧠 Projet EDA - Outil d'exploration de motifs fréquents")

# ============================================================
# INITIALISATION DES ÉTATS
# ============================================================
for key in ['df', 'df_binary', 'pool_P', 'sampler', 'sample', 'out_sample', 'data_type']:
    if key not in st.session_state: st.session_state[key] = None

# Valeur par défaut pour data_type si non défini
if st.session_state.data_type is None:
    st.session_state.data_type = 'transactionnel'

# ============================================================
# SIDEBAR : CHARGEMENT
# ============================================================
with st.sidebar:
    st.header("Chargement des données")
    
    # Choix du type de données (nécessaire pour ton snippet)
    st.session_state.data_type = st.radio(
        "Type de données",
        ('transactionnel', 'séquentiel'),
        index=0,
        help="Transactionnel: Paniers d'achat sans ordre\nSéquentiel: Parcours avec ordre temporel"
    )

    file_format = st.selectbox("Format Fichier", ('csv', 'json', 'parquet'))
    uploaded_file = st.file_uploader("Fichier", type=[file_format])

    if uploaded_file is not None:
        try:
            # 1. Lecture en DataFrame Pandas UNIQUEMENT (Pas de binarisation ici)
            if file_format == 'csv':
                try:
                    df_loaded = pd.read_csv(uploaded_file)
                    # Détection format wide 'brut'
                    if df_loaded.shape[1] > 10 and 'Unnamed' in str(df_loaded.columns[0]):
                         uploaded_file.seek(0)
                         df_loaded = pd.read_csv(uploaded_file, header=None, sep=None, engine='python')
                except:
                    uploaded_file.seek(0)
                    df_loaded = pd.read_csv(uploaded_file, header=None, sep=None, engine='python')

            elif file_format == 'json':
                df_loaded = pd.read_json(uploaded_file)
            elif file_format == 'parquet':
                df_loaded = pd.read_parquet(uploaded_file)
            
            if df_loaded is not None:
                # On vérifie si c'est un nouveau fichier pour resetter les états
                if st.session_state.df is None or not df_loaded.equals(st.session_state.df):
                    st.session_state.df = df_loaded
                    st.session_state.pool_P = None
                    st.session_state.sampler = None
                    st.session_state.sample = None
                    st.session_state.out_sample = None
                    st.success(f"✅ Chargé: {len(df_loaded)} lignes")

        except Exception as e:
            st.error(f"Erreur chargement: {e}")

    st.divider()
    
    # --- SECTION EXEMPLES ---
    st.subheader("📥 Exemples")
    long_example = "transaction_id,item\n1,eggs\n1,milk\n2,bread"
    st.download_button("⬇️ Exemple Long", data=long_example, file_name="long.csv", mime="text/csv")
    wide_example = "eggs milk\nbread"
    st.download_button("⬇️ Exemple Wide", data=wide_example, file_name="wide.txt", mime="text/plain")
    
    st.caption("Version 1.1 - EDA Project")

# ============================================================
# PAGE PRINCIPALE
# ============================================================

# Si aucune donnée n'est chargée
if st.session_state.df is None:
    st.info("👋 Veuillez charger un fichier dans la barre latérale pour commencer.")

# Si données chargées + Transactionnel
elif st.session_state.df is not None and st.session_state.data_type == 'transactionnel':
    
    # --- TON SNIPPET INTÉGRÉ ICI ---
    st.header("Préparation des données")
    
    col_conf, col_preview = st.columns([1, 2])
    
    with col_conf:
        st.subheader("Format des données")
        
        # Essayer de détecter le format automatiquement
        detected_format = 'auto'
        try:
            # Vérifier si c'est un format long (transaction_id, item)
            if st.session_state.df.shape[1] >= 2:
                # Format long probable si peu de colonnes
                col_info = f"Le fichier contient {st.session_state.df.shape[1]} colonnes. "
                if st.session_state.df.shape[1] == 2:
                    col_info += "Format 'long' détecté (transaction_id, item)."
                    detected_format = 'long'
                else:
                    col_info += "Format ambigu. Vérifiez ci-dessous."
            else:
                col_info = "Format 'wide' probable (une transaction par ligne)."
                detected_format = 'wide'
            
            st.info(col_info)
        except:
            pass
        
        data_format = st.radio(
            "Sélectionnez le format réel :",
            options=['auto', 'long', 'wide'],
            index=0 if detected_format == 'auto' else (1 if detected_format == 'long' else 2),
            horizontal=True,
            help="**Long**: Colonnes (ID, Item)\n**Wide**: Items séparés par espaces/cellules"
        )

        # Bouton pour valider la transformation binaire
        if st.button("Valider et Binariser"):
            with st.spinner("Transformation en cours..."):
                try:
                    # Appel à tools.py avec le format choisi par l'utilisateur
                    st.session_state.df_binary = tools.load_transactions(
                        st.session_state.df, 
                        format_type=data_format
                    )
                    st.success(f"✅ Binarisation réussie : {st.session_state.df_binary.shape[1]} items uniques.")
                except Exception as e:
                    st.error(f"Erreur lors de la binarisation : {e}")

    with col_preview:
        st.subheader("Aperçu des données brutes")
        st.dataframe(st.session_state.df.head(5), use_container_width=True)

    st.divider()

    # --- SUITE DU PIPELINE (Onglets) ---
    # On n'affiche les onglets que si la binarisation a été faite
    if st.session_state.df_binary is not None:
        
        with st.expander("Voir la matrice binaire (Aperçu)"):
            st.dataframe(st.session_state.df_binary.head())

        tab1, tab2 = st.tabs(["🕵️ Pipeline 1 : Interactif", "🎲 Pipeline 2 : Output Sampling"])

        # --- ONGLET 1 ---
        with tab1:
            st.subheader("Extraction & Interaction")
            
            # A. Extraction
            if st.session_state.pool_P is None:
                ms = st.slider("Min Support", 0.001, 0.2, 0.01, key='ms1')
                if st.button("Extraire (Apriori)"):
                    with st.spinner("Calcul en cours..."):
                        freq = tools.extract_frequent_itemsets(st.session_state.df_binary, ms)
                        st.session_state.pool_P = tools.calc_all_metrics(freq, st.session_state.df_binary)
                        st.success(f"{len(st.session_state.pool_P)} motifs trouvés")
            
            # B. Sampling
            if st.session_state.pool_P is not None:
                c1, c2 = st.columns(2)
                strat = c1.selectbox("Stratégie", ['balanced', 'quality', 'diversity'])
                k = c2.slider("K", 5, 50, 10)
                
                if st.button("Générer Échantillon"):
                    if not st.session_state.sampler or st.session_state.sampler.strategy_name != strat:
                        st.session_state.sampler = tools.InteractiveSampler(st.session_state.pool_P, strat)
                    st.session_state.sample = st.session_state.sampler.importance_sampling(k)

                # C. Affichage
                if st.session_state.sample is not None:
                    st.write("#### Résultats")
                    
                    csv_data = format_df_for_display(st.session_state.sample).to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Télécharger (CSV)", data=csv_data, file_name="echantillon_interactif.csv", mime="text/csv")
                    
                    for idx, row in st.session_state.sample.iterrows():
                        cols = st.columns([4, 1, 1])
                        items = ', '.join(list(row['itemset']))
                        cols[0].write(f"**{items}** (Score: {row['score']:.2f})")
                        if cols[1].button("👍", key=f"l{idx}"):
                            st.session_state.sampler.add_feedback(row['sample_id'], 'like')
                            st.rerun()
                        if cols[2].button("👎", key=f"d{idx}"):
                            st.session_state.sampler.add_feedback(row['sample_id'], 'dislike')
                            st.rerun()
                    
                    st.info(st.session_state.sampler.get_feedback_summary())
                    
                    st.divider()
                    if st.button("Évaluer Pipeline 1"):
                        ev = tools.InteractiveEvaluator(st.session_state.df_binary, st.session_state.sampler, st.session_state.pool_P)
                        st.table(ev.evaluate_all(st.session_state.sample))

        # --- ONGLET 2 ---
        with tab2:
            st.subheader("Échantillonnage Direct")
            c1, c2, c3 = st.columns(3)
            meas = c1.selectbox("Mesure", ['lift', 'support'])
            nt = c2.number_input("Candidats", 100, 5000, 1000)
            ml = c3.slider("Longueur Max", 2, 5, 3)
            
            if st.button("Lancer Output Sampling"):
                ops = tools.OutputPatternSampler(st.session_state.df_binary, meas, nt, ml)
                st.session_state.out_sample = ops.generate_sample()
                st.session_state.ops_cls = tools.OutputPatternSampler

            if st.session_state.out_sample is not None:
                df_display = format_df_for_display(st.session_state.out_sample)
                
                csv_out = df_display.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Télécharger (CSV)", data=csv_out, file_name="echantillon_output.csv", mime="text/csv")
                
                st.dataframe(df_display, use_container_width=True)
                st.bar_chart(st.session_state.out_sample[meas])
                
                st.divider()
                if st.button("Évaluer Pipeline 2"):
                    ev = tools.OutputSamplingEvaluator(st.session_state.df_binary, st.session_state.ops_cls)
                    st.table(ev.evaluate_all(st.session_state.out_sample))

elif st.session_state.data_type == 'séquentiel':
    st.warning("⚠️ L'analyse séquentielle (ordre temporel) n'est pas encore implémentée dans ce module. Veuillez sélectionner 'Transactionnel'.")