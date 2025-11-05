import streamlit as st
from preprocess import load_dataset
# Assurez-vous que la fonction load_dataset (définie ci-dessus) 
# est dans le même fichier ou importée.

# --- Début de l'application Streamlit ---
st.title("Projet EDA - Outil d'exploration de motifs")

st.header("1. Chargement des données")

# Sélection du format par l'utilisateur
file_format = st.selectbox(
    "Quel est le format de votre fichier ?",
    ('csv', 'json', 'parquet')
)

# Upload du fichier
uploaded_file = st.file_uploader(
    "Chargez votre fichier de données", 
    type=[file_format]
)

# Variable pour stocker le dataframe en session
if 'df' not in st.session_state:
    st.session_state.df = None

if uploaded_file is not None:
    # Appel de notre fonction de chargement
    df_loaded = load_dataset(uploaded_file, file_format)
    
    if df_loaded is not None:
        st.session_state.df = df_loaded
        st.success(f"Fichier chargé avec succès ! ({len(st.session_state.df)} lignes)")
        st.dataframe(st.session_state.df.head())
        
        # Le type de données est connu par l'utilisateur, comme spécifié
        data_type = st.radio(
            "Quel est le type de vos données ?",
            ('transactionnel', 'séquentiel'),
            horizontal=True
        )
        st.session_state.data_type = data_type

# Afficher le dataframe s'il est en mémoire
elif st.session_state.df is not None:
     st.info("Affichage des données précédemment chargées.")
     st.dataframe(st.session_state.df.head())

else:
    st.info("Veuillez charger un fichier pour commencer l'analyse.")

# --- La suite de votre application (Etape 2, etc.) ---
# if st.session_state.df is not None:
#    st.header("2. Pré-traitement et Extraction...")
#    ...