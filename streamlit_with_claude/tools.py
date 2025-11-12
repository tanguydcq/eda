import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from itertools import combinations
import warnings
warnings.filterwarnings('ignore')


# ============================================================
# 1. CHARGEMENT ET PRÉPARATION DES DONNÉES
# ============================================================

def load_transactions(file_input, file_format='csv', format_type='auto', data_type='transactional'):
    """
    Charge et prépare les données transactionnelles et séquentielles.
    
    Parameters:
    -----------
    file_input : str | StringIO | BytesIO | st.runtime.uploaded_file_manager.UploadedFile
        Chemin vers le fichier ou objet fichier en mémoire
    file_format : str, default='csv'
        Format du fichier ('csv', 'json', 'parquet')
    format_type : str, default='auto'
        Format des données:
        - 'long': Format (transaction_id, item [, timestamp]) avec en-tête
        - 'wide': Format une transaction par ligne, items séparés par espaces
        - 'sequential': Format séquentiel avec ordre temporel
        - 'auto': Détection automatique
    data_type : str, default='transactional'
        Type de données:
        - 'transactional': Données transactionnelles (ordre non important)
        - 'sequential': Données séquentielles (ordre important)
    
    Returns:
    --------
    df : DataFrame binaire pour Apriori (transactional) ou liste de séquences (sequential)
    """
    try:
        # Chargement selon le format de fichier
        if file_format == 'csv':
            # Essayer plusieurs méthodes de lecture pour les CSV
            try:
                df = pd.read_csv(file_input)
            except Exception as e1:
                print(f"DEBUG: Première tentative de lecture CSV échouée: {e1}")
                try:
                    # Réessayer avec des paramètres différents
                    if hasattr(file_input, 'seek'):
                        file_input.seek(0)
                    df = pd.read_csv(file_input, encoding='utf-8', sep=',')
                except Exception as e2:
                    print(f"DEBUG: Deuxième tentative échouée: {e2}")
                    try:
                        if hasattr(file_input, 'seek'):
                            file_input.seek(0)
                        df = pd.read_csv(file_input, encoding='latin-1')
                    except Exception as e3:
                        print(f"DEBUG: Troisième tentative échouée: {e3}")
                        try:
                            # Essayer sans header pour les CSV wide
                            if hasattr(file_input, 'seek'):
                                file_input.seek(0)
                            df = pd.read_csv(file_input, header=None)
                        except Exception as e4:
                            print(f"DEBUG: Lecture normale échouée, tentative format wide: {e4}")
                            # Pour le format wide avec longueurs différentes, lire ligne par ligne
                            if hasattr(file_input, 'seek'):
                                file_input.seek(0)
                            df = _read_csv_wide_format(file_input)
            
        elif file_format == 'json':
            if hasattr(file_input, 'seek'):
                file_input.seek(0)
            
            try:
                df = pd.read_json(file_input)
            except (ValueError, Exception) as e:
                error_msg = str(e)
                print(f"DEBUG: Échec de la lecture JSON standard ({error_msg})")
                
                # Vérifier si c'est l'erreur spécifique des longueurs d'arrays différentes
                if "All arrays must be of the same length" in error_msg:
                    print("DEBUG: Erreur de longueurs d'arrays détectée, utilisation du parser manuel")
                    if hasattr(file_input, 'seek'):
                        file_input.seek(0)
                    df = _read_json_manual(file_input)
                else:
                    # Tentative avec lines=True pour les autres erreurs
                    print("DEBUG: Tentative avec lines=True...")
                    try:
                        if hasattr(file_input, 'seek'):
                            file_input.seek(0)
                        df = pd.read_json(file_input, lines=True)
                    except Exception as e2:
                        print(f"DEBUG: Échec lecture JSON lines, tentative manuelle: {e2}")
                        # Lecture manuelle du JSON
                        if hasattr(file_input, 'seek'):
                            file_input.seek(0)
                        df = _read_json_manual(file_input)
                
        elif file_format == 'parquet':
            df = pd.read_parquet(file_input)
            
        else:
            raise ValueError(f"Format de fichier non supporté : {file_format}. Utilisez 'csv', 'json' ou 'parquet'.")
        
        # Validation du DataFrame chargé
        if df is None or df.empty:
            raise ValueError("Le fichier chargé est vide ou invalide")
        
        # Nettoyer les noms de colonnes
        df.columns = df.columns.str.strip()
        
        # Vérifier qu'on a au moins une colonne
        if len(df.columns) == 0:
            raise ValueError("Aucune colonne détectée dans le fichier")
        
        # Traitement selon le format des données
        transactions_list = _process_data_format(df, file_input, file_format, format_type, data_type)
        
        # Retour selon le type de données
        if data_type == 'sequential':
            # Pour les données séquentielles, retourner directement les séquences
            return transactions_list
        else:
            # Pour les données transactionnelles, encodage binaire avec TransactionEncoder
            te = TransactionEncoder()
            te_ary = te.fit(transactions_list).transform(transactions_list)
            df_binary = pd.DataFrame(te_ary, columns=te.columns_)
            return df_binary
        
    except Exception as e:
        raise ValueError(f"Erreur lors du chargement des transactions : {e}")


def _process_data_format(df, file_input, file_format, format_type, data_type):
    """
    Traite les données selon leur format pour extraire les transactions/séquences.
    """
    transactions_list = []
    
    # Debug : afficher les informations sur le DataFrame
    print(f"DEBUG: DataFrame shape: {df.shape}")
    print(f"DEBUG: Columns: {df.columns.tolist()}")
    print(f"DEBUG: Format type: {format_type}")
    print(f"DEBUG: Data type: {data_type}")
    print(f"DEBUG: First few rows:\n{df.head()}")
    
    if file_format == 'json':
        # Traitement spécifique pour JSON
        transactions_list = _process_json_transactions(df, data_type)
        
    else:  # CSV ou autres formats tabulaires
        # Détection automatique du format pour CSV
        if format_type == 'auto':
            format_type = _detect_csv_format(df, file_input, data_type)
            print(f"DEBUG: Format détecté automatiquement: {format_type}")
        
        if format_type == 'long':
            # Format: transaction_id, item [, timestamp]
            transactions_list = _process_long_format(df, data_type)
            
        elif format_type == 'wide':
            # Format: une transaction par ligne
            transactions_list = _process_wide_format(df, data_type)
            
        elif format_type == 'sequential':
            # Format séquentiel spécifique
            transactions_list = _process_sequential_format(df)
            
        elif format_type == 'matrix':
            # Format: matrice binaire (colonnes = items)
            transactions_list = _process_matrix_format(df)
    
    print(f"DEBUG: Nombre de transactions extraites: {len(transactions_list)}")
    if transactions_list:
        print(f"DEBUG: Première transaction: {transactions_list[0]}")
    
    return transactions_list


def _read_csv_wide_format(file_input):
    """Lit un CSV au format wide avec des longueurs de lignes différentes."""
    import csv
    import io
    
    rows = []
    max_cols = 0
    
    try:
        # Gérer les différents types d'input
        if isinstance(file_input, str):
            # Si c'est un chemin de fichier
            with open(file_input, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    # Nettoyer la ligne
                    clean_row = [item.strip() for item in row if item.strip()]
                    if clean_row:  # Seulement les lignes non vides
                        rows.append(clean_row)
                        max_cols = max(max_cols, len(clean_row))
        elif hasattr(file_input, 'read'):
            # Si c'est un objet fichier
            content = file_input.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            string_io = io.StringIO(content)
            reader = csv.reader(string_io)
            for row in reader:
                # Nettoyer la ligne
                clean_row = [item.strip() for item in row if item.strip()]
                if clean_row:  # Seulement les lignes non vides
                    rows.append(clean_row)
                    max_cols = max(max_cols, len(clean_row))
        else:
            # Fallback: traiter comme un iterable de lignes
            if hasattr(file_input, 'seek'):
                file_input.seek(0)
            reader = csv.reader(file_input)
            for row in reader:
                # Nettoyer la ligne
                clean_row = [item.strip() for item in row if item.strip()]
                if clean_row:  # Seulement les lignes non vides
                    rows.append(clean_row)
                    max_cols = max(max_cols, len(clean_row))
        
        # Créer un DataFrame en complétant avec des NaN
        padded_rows = []
        for row in rows:
            padded_row = row + [pd.NA] * (max_cols - len(row))
            padded_rows.append(padded_row)
        
        # Créer des noms de colonnes génériques
        column_names = [f'col_{i}' for i in range(max_cols)]
        df = pd.DataFrame(padded_rows, columns=column_names)
        
        return df
        
    except Exception as e:
        print(f"DEBUG: Erreur lors de la lecture CSV wide: {e}")
        # Fallback : créer un DataFrame avec une seule ligne
        return pd.DataFrame([['error']], columns=['col_0'])


def _read_json_manual(file_input):
    """Lit un fichier JSON manuellement pour gérer les cas problématiques."""
    import json
    
    try:
        # Lire le contenu
        if hasattr(file_input, 'read'):
            content = file_input.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
        else:
            with open(file_input, 'r', encoding='utf-8') as f:
                content = f.read()
        
        # Parser le JSON
        data = json.loads(content)
        
        # Traiter selon le type de données
        if isinstance(data, list):
            # Si c'est une liste de listes avec longueurs différentes
            if all(isinstance(item, list) for item in data):
                # Créer un DataFrame avec les transactions comme index
                transactions_data = []
                for i, transaction in enumerate(data):
                    transactions_data.append({'transaction_id': i, 'items': transaction})
                return pd.DataFrame(transactions_data)
            
            # Si c'est une liste d'objets
            elif all(isinstance(item, dict) for item in data):
                return pd.DataFrame(data)
            
            # Autres cas
            else:
                return pd.DataFrame({'data': data})
        
        elif isinstance(data, dict):
            # Si c'est un dictionnaire, le transformer en format long
            transactions_data = []
            for key, value in data.items():
                if isinstance(value, list):
                    transactions_data.append({'transaction_id': key, 'items': value})
                else:
                    transactions_data.append({'transaction_id': key, 'items': [value]})
            return pd.DataFrame(transactions_data)
        
        else:
            # Cas simple
            return pd.DataFrame({'data': [data]})
            
    except Exception as e:
        print(f"DEBUG: Erreur lors de la lecture JSON manuelle: {e}")
        return pd.DataFrame({'error': ['json_parse_error']})


def _detect_csv_format(df, file_input, data_type):
    """Détecte automatiquement le format des données CSV."""
    # Vérifier les noms de colonnes
    columns_lower = [col.lower().strip() for col in df.columns]
    
    # Détection pour données séquentielles
    if data_type == 'sequential':
        if any('timestamp' in col or 'time' in col or 'order' in col for col in columns_lower):
            return 'sequential'
        elif any('sequence' in col or 'seq' in col for col in columns_lower):
            return 'sequential'
    
    if any('transaction' in col or 'trans' in col for col in columns_lower) and \
       any('item' in col or 'product' in col for col in columns_lower):
        return 'long'
    
    # Si beaucoup de colonnes avec des valeurs binaires/numériques
    if len(df.columns) > 5 and df.dtypes.apply(lambda x: x in ['int64', 'float64', 'bool']).mean() > 0.7:
        return 'matrix'
    
    # Par défaut, format wide
    return 'wide'


def _process_json_transactions(df, data_type='transactional'):
    """Traite les données JSON pour extraire les transactions/séquences."""
    transactions_list = []
    
    print(f"DEBUG: Traitement JSON - colonnes: {df.columns.tolist()}")
    
    if 'sequences' in df.columns or 'sequence' in df.columns:
        # Format séquentiel: {"sequences": [["A","B","C"], ["D","E","F"]]}
        col_name = 'sequences' if 'sequences' in df.columns else 'sequence'
        for _, row in df.iterrows():
            if isinstance(row[col_name], list):
                if data_type == 'sequential':
                    # Préserver l'ordre pour les séquences
                    transactions_list.append([str(item) for item in row[col_name]])
                else:
                    # Pour transactionnel, l'ordre n'importe pas
                    transactions_list.append([str(item) for item in row[col_name]])
    
    elif 'transactions' in df.columns:
        # Format: {"transactions": [["A","B"], ["C","D"]]}
        for _, row in df.iterrows():
            if isinstance(row['transactions'], list):
                transactions_list.extend(row['transactions'])
    
    elif 'itemset' in df.columns or 'items' in df.columns:
        # Format: [{"items": ["A","B"]}, {"items": ["C","D"]}]
        col_name = 'itemset' if 'itemset' in df.columns else 'items'
        for _, row in df.iterrows():
            if isinstance(row[col_name], list):
                transactions_list.append([str(item) for item in row[col_name]])
    
    elif 'transaction_id' in df.columns and 'items' in df.columns:
        # Format manuel de _read_json_manual
        for _, row in df.iterrows():
            if isinstance(row['items'], list):
                transactions_list.append([str(item) for item in row['items']])
    
    elif 'data' in df.columns:
        # Format de fallback
        for _, row in df.iterrows():
            if isinstance(row['data'], list):
                transactions_list.append([str(item) for item in row['data']])
            else:
                transactions_list.append([str(row['data'])])
    
    else:
        # Essayer de traiter comme format tabulaire
        print("DEBUG: Tentative de traitement comme format tabulaire")
        transactions_list = _process_long_format(df, data_type)
    
    print(f"DEBUG: Transactions extraites du JSON: {len(transactions_list)}")
    return transactions_list


def _process_long_format(df, data_type='transactional'):
    """Traite le format long: transaction_id, item [, timestamp]."""
    # Identifier les colonnes
    columns_lower = [col.lower().strip() for col in df.columns]
    
    # Trouver la colonne transaction_id/user_id/session_id
    trans_col = None
    for i, col in enumerate(columns_lower):
        if any(keyword in col for keyword in ['transaction', 'trans', 'user', 'session', 'customer']):
            trans_col = df.columns[i]
            break
    if trans_col is None:
        trans_col = df.columns[0]  # Première colonne par défaut
    
    # Trouver la colonne item/event
    item_col = None
    for i, col in enumerate(columns_lower):
        if any(keyword in col for keyword in ['item', 'product', 'event', 'action', 'page']):
            item_col = df.columns[i]
            break
    if item_col is None:
        item_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
    
    # Trouver la colonne timestamp (pour données séquentielles)
    time_col = None
    if data_type == 'sequential' and len(df.columns) > 2:
        for i, col in enumerate(columns_lower):
            if any(keyword in col for keyword in ['timestamp', 'time', 'datetime', 'order', 'position']):
                time_col = df.columns[i]
                break
    
    # S'assurer qu'on n'a que deux colonnes principales
    if trans_col == item_col:
        # Si les colonnes sont identiques, prendre les deux premières
        trans_col = df.columns[0]
        item_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
    
    # Créer un DataFrame propre avec seulement les colonnes nécessaires
    df_work = df.copy()
    
    # Nettoyer les données
    df_clean = df_work[[trans_col, item_col]].copy()
    df_clean.columns = ['transaction_id', 'item']  # Renommer pour éviter les conflits
    
    # Supprimer les lignes avec des valeurs manquantes
    df_clean = df_clean.dropna()
    
    # Convertir les items en string et nettoyer
    df_clean['item'] = df_clean['item'].astype(str).str.strip()
    df_clean['transaction_id'] = df_clean['transaction_id'].astype(str).str.strip()
    
    # Supprimer les lignes vides après nettoyage
    df_clean = df_clean[
        (df_clean['item'] != '') & 
        (df_clean['transaction_id'] != '') &
        (df_clean['item'] != 'nan') &
        (df_clean['transaction_id'] != 'nan')
    ]
    
    # Gérer le cas séquentiel avec timestamp
    if data_type == 'sequential' and time_col and time_col in df.columns:
        df_clean['timestamp'] = df[time_col]
        df_clean = df_clean.dropna(subset=['timestamp'])
        df_clean = df_clean.sort_values(['transaction_id', 'timestamp'])
    
    # Grouper les items par transaction
    try:
        grouped = df_clean.groupby('transaction_id')['item'].apply(list)
        transactions_list = grouped.tolist()
    except Exception as e:
        # Fallback : traitement manuel si groupby échoue
        transactions_dict = {}
        for _, row in df_clean.iterrows():
            trans_id = row['transaction_id']
            item = row['item']
            if trans_id not in transactions_dict:
                transactions_dict[trans_id] = []
            transactions_dict[trans_id].append(item)
        
        transactions_list = list(transactions_dict.values())
    
    return transactions_list


def _process_sequential_format(df):
    """Traite le format séquentiel spécifique."""
    transactions_list = []
    
    # Chercher une colonne 'sequence' directement
    if 'sequence' in df.columns:
        for _, row in df.iterrows():
            seq = row['sequence']
            if isinstance(seq, str):
                # Parser une chaîne comme "A,B,C" ou "A B C"
                if ',' in seq:
                    items = [item.strip() for item in seq.split(',')]
                else:
                    items = seq.split()
                transactions_list.append(items)
            elif isinstance(seq, list):
                transactions_list.append([str(item) for item in seq])
    
    # Format avec user_id et événements ordonnés
    elif 'user_id' in df.columns and 'event' in df.columns:
        if 'timestamp' in df.columns:
            df_sorted = df.sort_values(['user_id', 'timestamp'])
        else:
            df_sorted = df.sort_values('user_id')
        
        transactions_list = df_sorted.groupby('user_id')['event'].apply(
            lambda x: [str(item) for item in x]
        ).tolist()
    
    return transactions_list


def _process_wide_format(df, data_type='transactional'):
    """Traite le format wide: une transaction par ligne, items dans les colonnes."""
    transactions_list = []
    
    for _, row in df.iterrows():
        transaction = []
        for col in df.columns:
            value = row[col]
            if pd.notna(value) and str(value).strip() not in ['', '0', 'False', 'nan']:
                transaction.append(str(value).strip())
        
        if transaction:
            # Pour les données séquentielles, préserver l'ordre des colonnes
            # Pour les données transactionnelles, l'ordre n'importe pas
            transactions_list.append(transaction)
    
    return transactions_list


def _process_matrix_format(df):
    """Traite le format matrice: colonnes = items, valeurs = présence."""
    transactions_list = []
    
    for _, row in df.iterrows():
        transaction = []
        for col in df.columns:
            value = row[col]
            try:
                # Vérifier si la valeur indique une présence
                if pd.notna(value) and float(value) > 0:
                    transaction.append(col)
            except (ValueError, TypeError):
                # Valeur non numérique
                if pd.notna(value) and str(value).lower() not in ['', '0', 'false', 'no', 'nan']:
                    transaction.append(col)
        
        if transaction:
            transactions_list.append(transaction)
    
    return transactions_list


# ============================================================
# 1.5. OUTILS POUR DONNÉES SÉQUENTIELLES
# ============================================================

def detect_data_type(file_input, file_format='csv'):
    """
    Détecte automatiquement si les données sont transactionnelles ou séquentielles.
    
    Returns:
    --------
    str : 'transactional' ou 'sequential'
    """
    try:
        # Charger un échantillon pour analyse
        if file_format == 'csv':
            df_sample = pd.read_csv(file_input, nrows=100)
        elif file_format == 'json':
            df_sample = pd.read_json(file_input)
        else:
            df_sample = pd.read_parquet(file_input)
        
        columns_lower = [col.lower().strip() for col in df_sample.columns]
        
        # Indicateurs de données séquentielles
        sequential_indicators = [
            'timestamp', 'time', 'datetime', 'order', 'position', 'step',
            'sequence', 'seq', 'event_order', 'session_start'
        ]
        
        # Indicateurs de données transactionnelles
        transactional_indicators = [
            'transaction_id', 'basket', 'cart', 'purchase', 'invoice'
        ]
        
        sequential_score = sum(1 for col in columns_lower 
                             if any(ind in col for ind in sequential_indicators))
        transactional_score = sum(1 for col in columns_lower 
                                if any(ind in col for ind in transactional_indicators))
        
        if sequential_score > transactional_score:
            return 'sequential'
        else:
            return 'transactional'
            
    except Exception:
        return 'transactional'  # Par défaut


def prepare_for_sequential_mining(sequences):
    """
    Prépare les séquences pour les algorithmes de motifs séquentiels (ex: PrefixSpan).
    
    Parameters:
    -----------
    sequences : list of lists
        Liste de séquences où chaque séquence est une liste d'items ordonnés
    
    Returns:
    --------
    sequences_prepared : list
        Séquences formatées pour l'extraction de motifs séquentiels
    """
    # Pour PrefixSpan, chaque séquence peut être une liste d'itemsets
    # Si chaque élément est un item simple, on peut les grouper en itemsets
    sequences_prepared = []
    
    for seq in sequences:
        # Convertir chaque item en itemset de taille 1
        sequence_as_itemsets = [[item] for item in seq]
        sequences_prepared.append(sequence_as_itemsets)
    
    return sequences_prepared


# ============================================================
# 2. EXTRACTION DES MOTIFS FRÉQUENTS
# ============================================================

def extract_frequent_itemsets(df, min_support=0.005):
    """Extrait les itemsets fréquents avec Apriori et calcule les métriques de base."""
    frequent_itemsets = apriori(df, min_support=min_support, use_colnames=True)
    frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(len)

    def calc_coverage(itemset):
        mask = df[list(itemset)].all(axis=1)
        return mask.sum() / len(df)

    frequent_itemsets['coverage'] = frequent_itemsets['itemsets'].apply(calc_coverage)
    return frequent_itemsets


def calc_all_metrics(frequent_itemsets, df):
    """Calcule support, confidence, lift, coverage pour tous les motifs."""
    supports_dict = dict(zip(frequent_itemsets['itemsets'], frequent_itemsets['support']))
    metrics_list = []

    for _, row in frequent_itemsets.iterrows():
        itemset = row['itemsets']
        support_xy = row['support']
        length = len(itemset)

        if length == 1:
            metrics_list.append({
                'itemset': itemset,
                'support': support_xy,
                'coverage': row['coverage'],
                'length': length,
                'confidence': np.nan,
                'lift': np.nan,
                'antecedent': None,
                'consequent': None
            })
        else:
            best_conf, best_lift = 0, 0
            best_ante, best_cons = None, None

            for i in range(1, length):
                for antecedent in combinations(itemset, i):
                    antecedent = frozenset(antecedent)
                    consequent = itemset - antecedent

                    if antecedent in supports_dict and consequent in supports_dict:
                        support_x = supports_dict[antecedent]
                        support_y = supports_dict[consequent]
                        confidence = support_xy / support_x
                        lift = support_xy / (support_x * support_y)

                        if confidence > best_conf:
                            best_conf, best_lift = confidence, lift
                            best_ante, best_cons = antecedent, consequent

            metrics_list.append({
                'itemset': itemset,
                'support': support_xy,
                'coverage': row['coverage'],
                'length': length,
                'confidence': best_conf if best_conf > 0 else np.nan,
                'lift': best_lift if best_lift > 0 else np.nan,
                'antecedent': best_ante,
                'consequent': best_cons
            })

    return pd.DataFrame(metrics_list)


# ============================================================
# 3. STRATÉGIES DE SCORING
# ============================================================

class ScoringStrategy:
    """Classe de base pour les stratégies de scoring."""

    def __init__(self, pool_P):
        self.pool_P = pool_P.copy()
        self._normalize_metrics()
        self._compute_redundancy()

    def _normalize_metrics(self):
        metrics = ['support', 'coverage', 'confidence', 'lift']
        for metric in metrics:
            if metric in self.pool_P.columns:
                values = self.pool_P[metric].fillna(0)
                if values.max() > values.min():
                    self.pool_P[f'{metric}_norm'] = (values - values.min()) / (values.max() - values.min())
                else:
                    self.pool_P[f'{metric}_norm'] = 0.5

    def _compute_redundancy(self):
        n = len(self.pool_P)
        itemsets_list = self.pool_P['itemset'].tolist()
        similarity_matrix = np.zeros((n, n))

        for i in range(n):
            for j in range(i + 1, n):
                set_i, set_j = itemsets_list[i], itemsets_list[j]
                intersection = len(set_i & set_j)
                union = len(set_i | set_j)
                similarity = intersection / union if union > 0 else 0
                similarity_matrix[i, j] = similarity
                similarity_matrix[j, i] = similarity

        self.pool_P['redundancy'] = similarity_matrix.mean(axis=1)

    def compute_score(self):
        raise NotImplementedError


class BalancedScoring(ScoringStrategy):
    """Scoring équilibré : support + lift + surprise - redondance."""
    def compute_score(self):
        self.pool_P['surprise'] = self.pool_P['confidence'].fillna(0) * (1 - self.pool_P['support_norm'])
        surprise_values = self.pool_P['surprise']
        surprise_range = surprise_values.max() - surprise_values.min()
        
        if surprise_range > 0:
            self.pool_P['surprise_norm'] = (surprise_values - surprise_values.min()) / surprise_range
        else:
            self.pool_P['surprise_norm'] = 0.5

        self.pool_P['score'] = (
            0.3 * self.pool_P['support_norm'] +
            0.3 * self.pool_P['lift_norm'].fillna(0) +
            0.3 * self.pool_P['surprise_norm'] -
            0.1 * self.pool_P['redundancy']
        )
        return self.pool_P


class QualityScoring(ScoringStrategy):
    """Scoring qualité : priorité à lift, confidence et longueur."""
    def compute_score(self):
        max_length = self.pool_P['length'].max()
        if max_length > 0:
            self.pool_P['length_norm'] = self.pool_P['length'] / max_length
        else:
            self.pool_P['length_norm'] = 0.5
            
        self.pool_P['score'] = (
            0.4 * self.pool_P['lift_norm'].fillna(0) +
            0.3 * self.pool_P['confidence_norm'].fillna(0) +
            0.2 * self.pool_P['length_norm'] -
            0.1 * self.pool_P['redundancy']
        )
        return self.pool_P


class DiversityScoring(ScoringStrategy):
    """Scoring diversité : favorise les motifs peu redondants."""
    def compute_score(self):
        self.pool_P['score'] = (
            0.25 * self.pool_P['support_norm'] +
            0.25 * self.pool_P['confidence_norm'].fillna(0) +
            0.25 * self.pool_P['lift_norm'].fillna(0) -
            0.25 * self.pool_P['redundancy']
        )
        return self.pool_P


# ============================================================
# 4. SAMPLER INTERACTIF AVEC FEEDBACK UTILISATEUR
# ============================================================

class InteractiveSampler:
    """Échantillonneur interactif avec feedback utilisateur."""

    def __init__(self, pool_P, strategy='balanced'):
        self.pool_P_original = pool_P.copy()
        self.strategy_name = strategy
        self.feedback_history = []
        self.user_weights = np.ones(len(pool_P))
        self._apply_scoring_strategy()

    def _apply_scoring_strategy(self):
        strategies = {
            'balanced': BalancedScoring,
            'quality': QualityScoring,
            'diversity': DiversityScoring
        }
        if self.strategy_name not in strategies:
            raise ValueError(f"Stratégie inconnue : {self.strategy_name}")
        strategy = strategies[self.strategy_name](self.pool_P_original)
        self.pool_P = strategy.compute_score()

    def importance_sampling(self, k=10, with_replacement=False, temperature=1.0):
        """Échantillonnage par importance pondéré."""
        final_scores = self.pool_P['score'] * self.user_weights
        final_scores = final_scores - final_scores.min() + 1e-10
        probabilities = np.power(final_scores, 1 / temperature)
        probabilities /= probabilities.sum()

        indices = np.random.choice(
            len(self.pool_P),
            size=min(k, len(self.pool_P)),
            replace=with_replacement,
            p=probabilities
        )

        sampled = self.pool_P.iloc[indices].copy()
        sampled['sampling_prob'] = probabilities[indices]
        sampled['sample_id'] = indices  # CORRECTION: utiliser les indices réels
        return sampled

    def add_feedback(self, sample_id, feedback):
        """Ajoute un feedback utilisateur ('like' ou 'dislike')."""
        if feedback not in ['like', 'dislike']:
            raise ValueError("Feedback doit être 'like' ou 'dislike'")
        feedback_value = 1.5 if feedback == 'like' else 0.5

        self.feedback_history.append({'sample_id': sample_id, 'feedback': feedback})
        target_itemset = self.pool_P.iloc[sample_id]['itemset']

        for idx, row in self.pool_P.iterrows():
            itemset = row['itemset']
            intersection = len(target_itemset & itemset)
            union = len(target_itemset | itemset)
            similarity = intersection / union if union > 0 else 0
            
            if similarity > 0.5:
                self.user_weights[idx] *= (1 + (feedback_value - 1) * similarity)
        
        # Renormalisation des poids
        weight_sum = self.user_weights.sum()
        if weight_sum > 0:
            self.user_weights = self.user_weights / weight_sum * len(self.user_weights)

    def get_feedback_summary(self):
        if not self.feedback_history:
            return "Aucun feedback enregistré"
        df_feedback = pd.DataFrame(self.feedback_history)
        likes = (df_feedback['feedback'] == 'like').sum()
        dislikes = (df_feedback['feedback'] == 'dislike').sum()
        return f"Feedbacks : {likes} likes, {dislikes} dislikes"
    
# ============================================================
# 5. ÉVALUATION & MÉTRIQUES (STEP 4)
# ============================================================

class PatternEvaluator:
    """Évalue les performances et la qualité des motifs sélectionnés."""
    
    def __init__(self, sampler, original_pool):
        """
        Args:
            sampler: instance de InteractiveSampler (après session)
            original_pool: DataFrame du pool complet (pool_P)
        """
        self.sampler = sampler
        self.pool_P = original_pool
        self.transactions = None  
    
    def _jaccard(self, a, b):
        inter = len(a & b)
        union = len(a | b)
        return inter / union if union > 0 else 0

    def acceptance_rate(self):
        """Proportion de motifs aimés ('like') sur le total des feedbacks."""
        if not self.sampler.feedback_history:
            return np.nan
        df_fb = pd.DataFrame(self.sampler.feedback_history)
        likes = (df_fb['feedback'] == 'like').sum()
        total = len(df_fb)
        return likes / total if total > 0 else np.nan

    def diversity(self, sample_df):
        """Diversité moyenne (1 - similarité de Jaccard moyenne)."""
        itemsets = sample_df['itemset'].tolist()
        if len(itemsets) < 2:
            return 1.0
        sims = []
        for i in range(len(itemsets)):
            for j in range(i + 1, len(itemsets)):
                sims.append(self._jaccard(set(itemsets[i]), set(itemsets[j])))
        return 1 - np.mean(sims)

    def coverage(self, sample_df, df_binary):
        """Proportion de transactions couvertes par au moins un motif."""
        n_tx = len(df_binary)
        covered = np.zeros(n_tx, dtype=bool)
        for itemset in sample_df['itemset']:
            mask = df_binary[list(itemset)].all(axis=1)
            covered |= mask
        return covered.mean()

    def stability(self, strategy='balanced', k=20, n_runs=4):
        """Mesure la stabilité (overlap moyen entre plusieurs échantillons)."""
        overlaps = []
        for seed in range(n_runs):
            temp_sampler = InteractiveSampler(self.pool_P, strategy=strategy)
            s1 = set(map(frozenset, temp_sampler.importance_sampling(k=k)['itemset']))
            s2 = set(map(frozenset, temp_sampler.importance_sampling(k=k)['itemset']))
            if len(s1 | s2) > 0:
                overlaps.append(len(s1 & s2) / len(s1 | s2))
        return np.mean(overlaps)

    def latency(self, k=100):
        """Temps moyen d’échantillonnage."""
        import time
        start = time.time()
        _ = self.sampler.importance_sampling(k=k)
        return time.time() - start

    def evaluate(self, df_binary):
        """Évalue toutes les métriques et renvoie un résumé."""
        sample_df = self.sampler.importance_sampling(k=30)
        results = {
            'acceptance_rate': self.acceptance_rate(),
            'diversity': self.diversity(sample_df),
            'coverage': self.coverage(sample_df, df_binary),
            'stability_mean_overlap': self.stability(),
            'sampling_time_sec_k100': self.latency(k=100)
        }
        return pd.DataFrame(results.items(), columns=['metric', 'value'])
    
# ============================================================
# 6. ÉCHANTILLONNAGE EN SORTIE DE MOTIFS
# ============================================================

class OutputPatternSampler:
    """
    Échantillonneur de motifs en sortie : génère un échantillon de motifs
    sans fouille exhaustive, en se basant sur une mesure d'intérêt.
    """

    def __init__(self, df_binary, measure='support', n_samples=1000, max_length=3, random_state=None):
        """
        Args:
            df_binary : DataFrame binaire (transactions x items)
            measure : mesure d'intérêt ('support', 'lift', 'length')
            n_samples : nombre de motifs à échantillonner
            max_length : taille max des itemsets
            random_state : graine aléatoire
        """
        self.df = df_binary
        self.measure = measure
        self.n_samples = n_samples
        self.max_length = max_length
        self.rng = np.random.default_rng(random_state)
        self.item_names = list(df_binary.columns)

    def _calc_support(self, itemset):
        """Support = fréquence d'apparition du motif."""
        mask = self.df[list(itemset)].all(axis=1)
        return mask.mean()

    def _calc_lift(self, itemset):
        """Approximation du lift : support(itemset) / prod(supports singletons)."""
        if len(itemset) < 2:
            return 1.0
        support_xy = self._calc_support(itemset)
        support_prod = np.prod([self._calc_support([i]) for i in itemset])
        return support_xy / support_prod if support_prod > 0 else np.nan

    def _sample_itemset(self):
        """Génère un itemset aléatoire (1 à max_length)."""
        length = self.rng.integers(1, self.max_length + 1)
        return frozenset(self.rng.choice(self.item_names, size=length, replace=False))

    def generate_sample(self):
        """Génère un échantillon de motifs selon la mesure d'intérêt."""
        motifs = []
        for _ in range(self.n_samples):
            itemset = self._sample_itemset()
            support = self._calc_support(itemset)
            lift = self._calc_lift(itemset)
            length = len(itemset)

            motifs.append({
                'itemset': itemset,
                'support': support,
                'lift': lift,
                'length': length
            })

        df_motifs = pd.DataFrame(motifs)

        # Pondération selon la mesure choisie
        if self.measure == 'support':
            probs = df_motifs['support'] / df_motifs['support'].sum()
        elif self.measure == 'lift':
            df_motifs['lift'] = df_motifs['lift'].replace([np.inf, np.nan], 0)
            probs = df_motifs['lift'] / (df_motifs['lift'].sum() + 1e-10)
        elif self.measure == 'length':
            probs = df_motifs['length'] / df_motifs['length'].sum()
        else:
            raise ValueError("Mesure inconnue : choisissez 'support', 'lift' ou 'length'")

        # Échantillonner selon la distribution
        sampled_indices = self.rng.choice(
            len(df_motifs), size=min(200, len(df_motifs)), replace=False, p=probs / probs.sum()
        )
        sampled = df_motifs.iloc[sampled_indices].copy()
        sampled['sampling_prob'] = probs.iloc[sampled_indices]

        return sampled

    def plot_distribution(self, df_sampled):
        """Affiche la distribution de la mesure d'intérêt dans l'échantillon."""
        import matplotlib.pyplot as plt
        plt.figure(figsize=(7, 4))
        plt.hist(df_sampled[self.measure], bins=20)
        plt.title(f"Distribution des motifs selon {self.measure}")
        plt.xlabel(self.measure)
        plt.ylabel("Fréquence")
        plt.grid(alpha=0.3)
        plt.show()


# ============================================================
# 7. OUTILS D'INTÉGRATION STREAMLIT
# ============================================================

def prepare_pool_from_csv(csv_path):
    """Charge un pool P à partir d'un CSV exporté et reconvertit les itemsets."""
    pool = pd.read_csv(csv_path)
    # Convertir les chaînes en frozensets
    if 'itemset' in pool.columns:
        pool['itemset'] = pool['itemset'].apply(
            lambda x: frozenset(eval(x)) if isinstance(x, str) else x
        )
    return pool


def save_pool(pool, output_path="pool_P_candidats.csv"):
    """Sauvegarde le pool de motifs en convertissant les frozensets en strings."""
    pool_to_save = pool.copy()
    
    # Convertir les colonnes contenant des frozensets en strings
    for col in ['itemset', 'antecedent', 'consequent']:
        if col in pool_to_save.columns:
            pool_to_save[col] = pool_to_save[col].apply(
                lambda x: str(sorted(list(x))) if isinstance(x, (set, frozenset)) and x is not None else str(x)
            )
    
    pool_to_save.to_csv(output_path, index=False)
    return output_path