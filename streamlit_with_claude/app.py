import streamlit as st
import pandas as pd
import tools  # le module avec load_transactions, extract_frequent_itemsets, etc.
import os

# --- Configuration de la page ---
st.set_page_config(
    page_title="Exploration de motifs fréquents",
    page_icon="🧠",
    layout="wide"
)

# --- Titre de l'application ---
st.title("🧠 Projet EDA - Outil d'exploration de motifs fréquents")

# --- Sidebar avec informations et exemples ---
with st.sidebar:
    st.header("📚 Guide d'utilisation")
    st.write("""
    **Étapes :**
    1. Chargez vos données (CSV, JSON, ou Parquet)
    2. Sélectionnez le format de données
    3. Lancez l'extraction de motifs
    4. Explorez et donnez votre feedback
    """)
    
    st.divider()
    
    st.subheader("📥 Exemples de données")
    
    # Exemple format long
    long_example = """transaction_id,item
1,eggs
1,milk
1,yogurt
2,eggs
2,bread
3,banana"""
    
    st.download_button(
        label="⬇️ Format Long",
        data=long_example,
        file_name="example_long.csv",
        mime="text/csv",
        help="Format avec colonnes transaction_id et item"
    )
    
    # Exemple format wide
    wide_example = """1 2 3 4 5
1 7 6 8 9
10
5 2 11
10 7 8 4 11"""
    
    st.download_button(
        label="⬇️ Format Wide",
        data=wide_example,
        file_name="example_wide.txt",
        mime="text/plain",
        help="Format avec items séparés par espaces"
    )
    
    st.divider()
    st.caption("Version 1.0 - EDA Project")

# ============================================================
# INITIALISATION DE L'ÉTAT DE SESSION
# ============================================================
if 'df' not in st.session_state:
    st.session_state.df = None
if 'data_type' not in st.session_state:
    st.session_state.data_type = None
if 'pool_P' not in st.session_state:
    st.session_state.pool_P = None
if 'sampler' not in st.session_state:
    st.session_state.sampler = None
if 'sample' not in st.session_state:
    st.session_state.sample = None

# ============================================================
# ÉTAPE 1 : CHARGEMENT DES DONNÉES
# ============================================================
st.header("1️⃣ Chargement des données")

# --- Sélection du format ---
file_format = st.selectbox(
    "Quel est le format de votre fichier ?",
    ('csv', 'json', 'parquet')
)

# --- Upload du fichier ---
uploaded_file = st.file_uploader(
    "Chargez votre fichier de données",
    type=[file_format]
)

if uploaded_file is not None:
    try:
        # Chargement du DataFrame selon le format choisi
        if file_format == 'csv':
            # Tenter de lire le fichier pour détecter le format
            try:
                # Essayer avec en-tête d'abord
                df_loaded = pd.read_csv(uploaded_file)
                
                # Si le fichier a l'air d'être au format "wide" (pas de colonne structurée)
                # on le recharge sans en-tête
                if df_loaded.shape[1] > 10 or any('Unnamed' in str(col) for col in df_loaded.columns):
                    uploaded_file.seek(0)  # Revenir au début du fichier
                    df_loaded = pd.read_csv(uploaded_file, header=None, sep=' ', skipinitialspace=True)
                    st.info("ℹ️ Format 'wide' détecté (items par ligne)")
                
            except:
                # Si ça échoue, essayer avec séparateur espace
                uploaded_file.seek(0)
                df_loaded = pd.read_csv(uploaded_file, header=None, sep=' ', skipinitialspace=True)
                
        elif file_format == 'json':
            df_loaded = pd.read_json(uploaded_file)
        elif file_format == 'parquet':
            df_loaded = pd.read_parquet(uploaded_file)
        else:
            st.error("Format non supporté.")
            df_loaded = None

        if df_loaded is not None:
            st.session_state.df = df_loaded
            st.success(f"✅ Fichier chargé avec succès ({len(df_loaded)} lignes, {len(df_loaded.columns)} colonnes)")
            
            # Afficher un aperçu différent selon le format probable
            if df_loaded.shape[1] <= 3:
                st.dataframe(df_loaded.head(10))
            else:
                # Pour format wide, afficher autrement
                st.write("**Aperçu des premières transactions :**")
                for i in range(min(5, len(df_loaded))):
                    items = [str(item) for item in df_loaded.iloc[i] if pd.notna(item)]
                    st.text(f"Transaction {i+1}: {' '.join(items)}")

            # Type de données
            data_type = st.radio(
                "Quel est le type de vos données ?",
                ('transactionnel', 'séquentiel'),
                horizontal=True
            )
            st.session_state.data_type = data_type
            
    except Exception as e:
        st.error(f"❌ Erreur lors du chargement du fichier : {e}")
        import traceback
        with st.expander("Voir les détails de l'erreur"):
            st.code(traceback.format_exc())

elif st.session_state.df is not None:
    st.info("📦 Données déjà chargées en mémoire.")
    st.dataframe(st.session_state.df.head())
    
    # Afficher le type de données si déjà défini
    if st.session_state.data_type:
        st.write(f"Type de données : **{st.session_state.data_type}**")
else:
    st.info("⬆️ Veuillez charger un fichier pour commencer l'analyse.")

# ============================================================
# ÉTAPE 2 : EXTRACTION DE MOTIFS (si dataset disponible)
# ============================================================

if st.session_state.df is not None and st.session_state.data_type == 'transactionnel':
    st.header("2️⃣ Extraction de motifs fréquents")
    
    # Détection du format des données
    st.subheader("Format des données")
    
    # Essayer de détecter le format automatiquement
    detected_format = 'auto'
    try:
        # Vérifier si c'est un format long (transaction_id, item)
        if st.session_state.df.shape[1] >= 2:
            # Format long probable
            col_info = f"Le fichier contient {st.session_state.df.shape[1]} colonnes. "
            if st.session_state.df.shape[1] == 2:
                col_info += "Format 'long' détecté (transaction_id, item)."
                detected_format = 'long'
            else:
                col_info += "Format ambigu. Veuillez sélectionner le format."
        else:
            col_info = "Format 'wide' probable (une transaction par ligne)."
            detected_format = 'wide'
        
        st.info(col_info)
    except:
        pass
    
    data_format = st.radio(
        "Sélectionnez le format de vos données :",
        options=['auto', 'long', 'wide'],
        index=0 if detected_format == 'auto' else (1 if detected_format == 'long' else 2),
        horizontal=True,
        help="**Long**: Format avec colonnes (transaction_id, item) - un item par ligne\n\n"
             "**Wide**: Format avec items séparés par espaces - une transaction par ligne\n\n"
             "**Auto**: Détection automatique du format"
    )
    
    # Exemple du format sélectionné
    with st.expander("ℹ️ Voir des exemples de formats"):
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Format Long (transaction_id, item)**")
            st.code("""transaction_id,item
1,eggs
1,milk
1,yogurt
2,eggs
2,bread
3,banana""")
        with col2:
            st.write("**Format Wide (items par ligne)**")
            st.code("""1 2 3 4 5
1 7 6 8 9
10
5 2 11""")

    col1, col2 = st.columns([2, 1])
    
    with col1:
        min_support = st.slider(
            "Choisissez le seuil minimum de support (min_support)",
            min_value=0.001, max_value=0.1, value=0.01, step=0.001,
            help="Plus la valeur est faible, plus vous obtiendrez de motifs (mais le calcul sera plus long)"
        )
    
    with col2:
        st.metric("Support sélectionné", f"{min_support:.3f}")

    if st.button("🚀 Lancer l'extraction", type="primary"):
        with st.spinner("Extraction en cours... Cela peut prendre quelques instants."):
            try:
                # Sauvegarde temporaire du dataset transactionnel
                tmp_path = "temp_transactions.csv"
                
                if data_format == 'long' or (data_format == 'auto' and st.session_state.df.shape[1] >= 2):
                    # Sauvegarder avec en-tête pour le format long
                    st.session_state.df.to_csv(tmp_path, index=False)
                else:
                    # Pour le format wide, sauvegarder sans en-tête
                    # Supposer que le DataFrame contient déjà les transactions au bon format
                    with open(tmp_path, 'w') as f:
                        for idx, row in st.session_state.df.iterrows():
                            # Prendre tous les items non-null de la ligne
                            items = [str(item) for item in row if pd.notna(item)]
                            if items:
                                f.write(' '.join(items) + '\n')

                # Chargement et binarisation via tools.py
                df_bin = tools.load_transactions(tmp_path, format_type=data_format)
                
                st.info(f"📊 {len(df_bin)} transactions chargées avec {len(df_bin.columns)} items uniques")

                # Extraction des itemsets fréquents
                frequent_itemsets = tools.extract_frequent_itemsets(df_bin, min_support=min_support)
                pool_P = tools.calc_all_metrics(frequent_itemsets, df_bin)

                # Sauvegarde du pool (avec conversion automatique des frozensets)
                output_path = tools.save_pool(pool_P, "pool_P_candidats.csv")

                # Nettoyage du fichier temporaire
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

                st.success(f"✅ Extraction terminée ! {len(pool_P)} motifs trouvés")
                st.write(f"📁 Résultats sauvegardés dans : `{output_path}`")

                # Affichage des premiers motifs (conversion pour l'affichage)
                st.subheader("Aperçu des motifs extraits")
                display_pool = pool_P.copy()
                display_pool['itemset_str'] = display_pool['itemset'].apply(
                    lambda x: ', '.join(sorted(list(x))) if isinstance(x, (set, frozenset)) else str(x)
                )
                
                display_cols = ['itemset_str', 'support', 'confidence', 'lift', 'coverage', 'length']
                available_cols = [col for col in display_cols if col in display_pool.columns or col == 'itemset_str']
                st.dataframe(display_pool[available_cols].head(15))

                # Statistiques rapides
                st.write("📊 **Statistiques globales :**")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Nombre de motifs", len(pool_P))
                with col2:
                    st.metric("Support moyen", f"{pool_P['support'].mean():.4f}")
                with col3:
                    st.metric("Longueur moyenne", f"{pool_P['length'].mean():.2f}")
                with col4:
                    st.metric("Longueur max", int(pool_P['length'].max()))

                # Sauvegarde en session (garder les frozensets pour le traitement)
                st.session_state.pool_P = pool_P
                
            except Exception as e:
                st.error(f"❌ Erreur lors de l'extraction : {e}")
                import traceback
                st.code(traceback.format_exc())

elif st.session_state.df is not None and st.session_state.data_type == 'séquentiel':
    st.header("2️⃣ Extraction de motifs séquentiels")
    st.warning("⚠️ L'extraction de motifs séquentiels n'est pas encore implémentée dans cette version.")

# ============================================================
# ÉTAPE 3 : INTERACTION ET FEEDBACK
# ============================================================

if st.session_state.pool_P is not None:
    st.header("3️⃣ Exploration interactive")
    
    st.write("Utilisez cette section pour explorer les motifs extraits de manière interactive et donner votre feedback.")

    col1, col2, col3 = st.columns(3)
    
    with col1:
        strategy = st.selectbox(
            "Stratégie de scoring",
            ['balanced', 'quality', 'diversity'],
            help="Balanced: équilibre entre support, lift et surprise\n"
                 "Quality: priorité à lift et confidence\n"
                 "Diversity: favorise les motifs différents"
        )
    
    with col2:
        k = st.slider("Nombre de motifs à afficher", 5, 30, 10)
    
    with col3:
        temperature = st.slider(
            "Température", 0.1, 2.0, 1.0, 0.1,
            help="Plus basse = plus déterministe\nPlus haute = plus de diversité"
        )

    if st.button("🎯 Générer un échantillon interactif", type="primary"):
        try:
            with st.spinner("Génération de l'échantillon..."):
                sampler = tools.InteractiveSampler(st.session_state.pool_P, strategy=strategy)
                sample = sampler.importance_sampling(k=k, temperature=temperature)

                st.session_state.sampler = sampler
                st.session_state.sample = sample

                st.success(f"✅ {len(sample)} motifs échantillonnés selon la stratégie '{strategy}'")
                
                # Affichage du tableau avec conversion pour Streamlit
                display_sample = sample.copy()
                display_sample['itemset_str'] = display_sample['itemset'].apply(
                    lambda x: ', '.join(sorted(list(x))) if isinstance(x, (set, frozenset)) else str(x)
                )
                
                display_cols = ['itemset_str', 'score', 'support', 'confidence', 'lift', 'coverage']
                available_cols = [col for col in display_cols if col in display_sample.columns or col == 'itemset_str']
                st.dataframe(display_sample[available_cols], use_container_width=True)
                
        except Exception as e:
            st.error(f"❌ Erreur lors de la génération : {e}")
            import traceback
            st.code(traceback.format_exc())

# ============================================================
# SECTION FEEDBACK
# ============================================================

if st.session_state.sample is not None and st.session_state.sampler is not None:
    st.subheader("💬 Donnez votre feedback sur les motifs")
    
    st.write("Explorez chaque motif et indiquez si vous le trouvez intéressant ou non. "
             "Vos feedbacks influenceront les prochains échantillons.")

    sample = st.session_state.sample
    sampler = st.session_state.sampler

    for idx, row in sample.iterrows():
        # Conversion de l'itemset pour l'affichage
        itemset_str = ', '.join(sorted(list(row['itemset']))) if isinstance(row['itemset'], (set, frozenset)) else str(row['itemset'])
        
        with st.expander(f"Motif #{idx+1}: {itemset_str}"):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.write(f"**Score** : {row['score']:.4f}")
                st.write(f"**Support** : {row['support']:.4f}")
                
                confidence = row.get('confidence', float('nan'))
                if pd.notna(confidence):
                    st.write(f"**Confidence** : {confidence:.4f}")
                
                lift = row.get('lift', float('nan'))
                if pd.notna(lift):
                    st.write(f"**Lift** : {lift:.4f}")
                
                if 'coverage' in row:
                    st.write(f"**Coverage** : {row['coverage']:.4f}")
            
            with col2:
                col_like, col_dislike = st.columns(2)
                with col_like:
                    if st.button("👍 Like", key=f"like_{idx}", use_container_width=True):
                        sampler.add_feedback(row['sample_id'], 'like')
                        st.success("Feedback enregistré !")
                        st.rerun()
                
                with col_dislike:
                    if st.button("👎 Dislike", key=f"dislike_{idx}", use_container_width=True):
                        sampler.add_feedback(row['sample_id'], 'dislike')
                        st.info("Feedback enregistré !")
                        st.rerun()

    # Affichage du résumé des feedbacks
    st.divider()
    feedback_summary = sampler.get_feedback_summary()
    st.info(f"📊 {feedback_summary}")
    
    if len(sampler.feedback_history) > 0:
        st.write("💡 **Astuce** : Générez un nouvel échantillon pour voir l'impact de vos feedbacks !")