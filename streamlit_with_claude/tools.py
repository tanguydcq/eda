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

def load_transactions(csv_path, format_type='auto'):
    """
    Charge et prépare les transactions à partir d'un fichier CSV.
    
    Parameters:
    -----------
    csv_path : str
        Chemin vers le fichier CSV
    format_type : str, default='auto'
        Format des données:
        - 'long': Format (transaction_id, item) avec en-tête
        - 'wide': Format une transaction par ligne, items séparés par espaces
        - 'auto': Détection automatique
    
    Returns:
    --------
    df : DataFrame binaire pour l'algorithme Apriori
    """
    # Lecture du fichier
    with open(csv_path, 'r') as f:
        first_line = f.readline().strip()
    
    # Détection automatique du format
    if format_type == 'auto':
        # Si la première ligne contient des virgules et ressemble à un en-tête
        if ',' in first_line and any(char.isalpha() for char in first_line):
            format_type = 'long'
        else:
            format_type = 'wide'
    
    if format_type == 'long':
        # Format: transaction_id,item
        transactions = pd.read_csv(csv_path, header=0)
        
        # Vérifier si les colonnes existent
        if transactions.shape[1] < 2:
            raise ValueError("Le format 'long' nécessite au moins 2 colonnes (transaction_id, item)")
        
        # Prendre les deux premières colonnes
        transactions = transactions.iloc[:, [0, 1]]
        transactions.columns = ['transaction_id', 'item']
        
        # Convertir en format liste de listes
        transactions['item'] = transactions['item'].astype(str)
        transactions_list = transactions.groupby('transaction_id')['item'].apply(list).tolist()
        
    else:  # format_type == 'wide'
        # Format: items séparés par espaces, une transaction par ligne
        transactions_list = []
        with open(csv_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:  # Ignorer les lignes vides
                    items = line.split()
                    if items:  # S'assurer qu'il y a des items
                        transactions_list.append(items)
    
    # Encodage binaire avec TransactionEncoder
    te = TransactionEncoder()
    te_ary = te.fit(transactions_list).transform(transactions_list)
    df = pd.DataFrame(te_ary, columns=te.columns_)
    
    return df


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
# 5. OUTILS D'INTÉGRATION STREAMLIT
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