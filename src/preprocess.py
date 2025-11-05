import pandas as pd
import streamlit as st
from io import StringIO, BytesIO
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def load_dataset(file_input: str | StringIO | BytesIO | st.runtime.uploaded_file_manager.UploadedFile, 
                 file_format: str) -> pd.DataFrame | None:
    """
    Charge un ensemble de données dans un DataFrame pandas à partir de diverses sources.

    Args:
        file_input: Un chemin de fichier (str) ou un objet fichier en mémoire
                     (comme un fichier uploadé par Streamlit).
        file_format: Le format du fichier (ex: 'csv', 'json', 'parquet').

    Returns:
        Un DataFrame pandas si le chargement réussit, sinon None.
    """
    log.info(f"Tentative de chargement de données au format : {file_format}")
    
    try:
        if file_format == 'csv':
            df = pd.read_csv(file_input)
            
        elif file_format == 'json':
            if hasattr(file_input, 'seek'):
                file_input.seek(0)
            
            try:
                df = pd.read_json(file_input)
            except ValueError as e:
                log.warning(f"Échec de la lecture JSON standard ({e}). Tentative avec lines=True...")
                if hasattr(file_input, 'seek'):
                    file_input.seek(0)
                df = pd.read_json(file_input, lines=True)
                
        elif file_format == 'parquet':
            df = pd.read_parquet(file_input)
            
        else:
            log.error(f"Format de fichier non supporté : {file_format}")
            st.error(f"Format de fichier non supporté : {file_format}. Veuillez utiliser 'csv', 'json', or 'parquet'.")
            return None
            
        log.info(f"Chargement réussi. DataFrame avec {len(df)} lignes.")
        return df

    except Exception as e:
        log.error(f"Erreur lors de la lecture du fichier {file_format} : {e}")
        st.error(f"Une erreur est survenue lors de la lecture du fichier : {e}")
        return None
