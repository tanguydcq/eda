import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from itertools import combinations
import warnings
import time

warnings.filterwarnings('ignore')

# ============================================================
# 1. CHARGEMENT ET PRÉPARATION DES DONNÉES 
# ============================================================

def load_transactions(source, format_type='auto', data_type='transactionnel'):
    """
    Charge et prépare les transactions ou séquences.
    
    Args:
        source: DataFrame ou chemin de fichier.
        format_type: 'auto', 'long' (row-based), ou 'wide' (column-based).
        data_type: 'transactionnel' (return matrice binaire) ou 'séquentiel' (return liste ordonnée).
    """
    df_raw = None

    # CAS 1 : C'est un DataFrame (Cas Streamlit)
    if isinstance(source, pd.DataFrame):
        df_raw = source.copy()
    
    # CAS 2 : C'est un chemin de fichier (Cas test local)
    elif isinstance(source, str):
        if source.endswith('.csv'):
            try:
                df_raw = pd.read_csv(source)
            except:
                df_raw = pd.read_csv(source, header=None, sep=None, engine='python')
        else:
            with open(source, 'r') as f:
                lines = f.readlines()
            transactions_list = [line.strip().split() for line in lines]
            return _process_output(transactions_list, data_type)

    if df_raw is None:
        raise ValueError("Source invalide : Attendu DataFrame ou chemin fichier.")

    # --- TRAITEMENT DU DATAFRAME ---
    transactions_list = []

    # Détection auto simple
    if format_type == 'auto':
        # Si 2 ou 3 colonnes et beaucoup de répétitions dans la 1ère => Long format
        if df_raw.shape[1] in [2, 3] and df_raw.iloc[:,0].nunique() < len(df_raw):
            format_type = 'long'
        else:
            format_type = 'wide'

    if format_type == 'long':
        # Gestion Format Long : ID, [Temps], Item
        # On suppose : col 0 = ID Transaction
        
        # S'il y a 3 colonnes, on suppose : ID, Timestamp, Item
        if df_raw.shape[1] >= 3:
            df_raw.columns = ['id', 'time', 'item']
            # Tri indispensable pour le séquentiel
            if data_type == 'séquentiel':
                df_raw = df_raw.sort_values(by=['id', 'time'])
        else:
            # 2 colonnes : ID, Item
            df_raw.columns = ['id', 'item']
        
        df_raw['item'] = df_raw['item'].astype(str)
        transactions_list = df_raw.groupby('id')['item'].apply(list).tolist()
    
    else: # format 'wide'
        # On itère ligne par ligne
        for _, row in df_raw.iterrows():
            transaction = []
            for val in row:
                if pd.notna(val) and val != '':
                    if isinstance(val, (list, np.ndarray)):
                        transaction.extend([str(x) for x in val])
                    else:
                        transaction.append(str(val))
            
            if transaction:
                transactions_list.append(transaction)

    return _process_output(transactions_list, data_type)


def _process_output(transactions_list, data_type):
    """Route vers la binarisation ou le format séquentiel brut."""
    if data_type == 'transactionnel':
        return _binarize(transactions_list)
    else:
        # Pour le séquentiel, on retourne un DataFrame avec une colonne 'sequence'
        # Cela permet de garder l'ordre des items [a, b, a, c]
        return pd.DataFrame({'sequence': transactions_list})


def _binarize(transactions_list):
    """Helper interne pour l'encodage binaire (TransactionEncoder)"""
    te = TransactionEncoder()
    try:
        te_ary = te.fit(transactions_list).transform(transactions_list)
        return pd.DataFrame(te_ary, columns=te.columns_)
    except Exception as e:
        print(f"Erreur binarisation: {e}")
        return pd.DataFrame()


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
        sampled['sample_id'] = indices  
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
# 5. ÉVALUATION & MÉTRIQUES 
# ============================================================

class BasePatternEvaluator:
    """
    CLASSE DE BASE : Évalue la QUALITÉ générique d'un ÉCHANTILLON de motifs.
    """
    
    def __init__(self, df_binary):
        self.df_binary = df_binary
    
    def _jaccard(self, a, b):
        inter = len(a & b)
        union = len(a | b)
        return inter / union if union > 0 else 0

    def diversity(self, sample_df):
        """Diversité moyenne (1 - similarité de Jaccard moyenne)."""
        itemsets = sample_df['itemset'].tolist()
        if len(itemsets) < 2: return 1.0
        sims = []
        for i in range(len(itemsets)):
            for j in range(i + 1, len(itemsets)):
                sims.append(self._jaccard(set(itemsets[i]), set(itemsets[j])))
        return 1 - np.mean(sims)

    def coverage(self, sample_df):
        """Proportion de transactions couvertes par au moins un motif."""
        n_tx = len(self.df_binary)
        covered = np.zeros(n_tx, dtype=bool)
        # Gestion compatibilité set/frozenset/list
        itemsets_list = [list(itemset) for itemset in sample_df['itemset']]
        
        for itemset in itemsets_list:
            # Filtrer les items qui n'existent pas dans le df (cas edge)
            valid_cols = [col for col in itemset if col in self.df_binary.columns]
            if not valid_cols: continue
            mask = self.df_binary[valid_cols].all(axis=1)
            covered |= mask
        return covered.mean()

    def evaluate_sample_quality(self, sample_df):
        """Calcule les métriques de qualité génériques."""
        if sample_df is None or len(sample_df) == 0:
            return {'diversity': np.nan, 'coverage': np.nan}
            
        return {
            'diversity': self.diversity(sample_df),
            'coverage': self.coverage(sample_df)
        }

class InteractiveEvaluator(BasePatternEvaluator):
    """
    Évalue le pipeline INTERACTIF (Exhaustif).
    """
    def __init__(self, df_binary, sampler, original_pool):
        super().__init__(df_binary)
        self.sampler = sampler
        self.pool_P = original_pool

    def acceptance_rate(self):
        """Taux d'acceptation (spécifique au feedback)."""
        fb_history = self.sampler.feedback_history
        if not fb_history:
            return np.nan
        likes = sum(1 for f in fb_history if f['feedback'] == 'like')
        return likes / len(fb_history)

    def latency(self, k=100):
        """Temps de ré-échantillonnage."""
        start = time.time()
        _ = self.sampler.importance_sampling(k=k)
        return time.time() - start

    def stability(self, strategy='balanced', k=20, n_runs=4):
        """Stabilité des échantillons générés."""
        overlaps = []
        for seed in range(n_runs):
            # Note: InteractiveSampler n'a pas de seed explicite dans __init__, 
            # on compte sur l'aléatoire numpy global ou on instancie à nouveau
            np.random.seed(seed)
            s1_sampler = InteractiveSampler(self.pool_P, strategy=strategy)
            s1 = set(map(frozenset, s1_sampler.importance_sampling(k=k)['itemset']))
            
            np.random.seed(seed + 100)
            s2_sampler = InteractiveSampler(self.pool_P, strategy=strategy)
            s2 = set(map(frozenset, s2_sampler.importance_sampling(k=k)['itemset']))
            
            if len(s1 | s2) > 0:
                overlaps.append(len(s1 & s2) / len(s1 | s2))
        return np.mean(overlaps) if overlaps else np.nan

    def evaluate_all(self, sample_df):
        """Exécute toutes les évaluations."""
        results = self.evaluate_sample_quality(sample_df)
        results['acceptance_rate'] = self.acceptance_rate()
        results['stability'] = self.stability()
        results['latency (k=100)'] = self.latency()
        return pd.DataFrame(results.items(), columns=['Métrique', 'Valeur'])


class OutputSamplingEvaluator(BasePatternEvaluator):
    """
    Évalue le pipeline OUTPUT SAMPLING.
    """
    def __init__(self, df_binary, sampler_class):
        super().__init__(df_binary)
        self.sampler_class = sampler_class 

    def latency(self, n_samples=1000, max_length=3):
        """Temps de génération totale."""
        start = time.time()
        temp_sampler = self.sampler_class(self.df_binary, n_samples=n_samples, max_length=max_length)
        _ = temp_sampler.generate_sample()
        return time.time() - start

    def stability(self, k=20, n_runs=4):
        """Stabilité de l'échantillonnage en sortie."""
        overlaps = []
        for seed in range(n_runs):
            temp_sampler_1 = self.sampler_class(self.df_binary, random_state=seed)
            s1 = set(map(frozenset, temp_sampler_1.generate_sample()['itemset']))
            
            temp_sampler_2 = self.sampler_class(self.df_binary, random_state=seed + 100)
            s2 = set(map(frozenset, temp_sampler_2.generate_sample()['itemset']))
            
            if len(s1 | s2) > 0:
                overlaps.append(len(s1 & s2) / len(s1 | s2))
        return np.mean(overlaps) if overlaps else np.nan

    def evaluate_all(self, sample_df):
        """Exécute toutes les évaluations."""
        results = self.evaluate_sample_quality(sample_df)
        results['stability'] = self.stability()
        results['latency_total'] = self.latency()
        return pd.DataFrame(results.items(), columns=['Métrique', 'Valeur'])
    
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