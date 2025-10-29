import json
import csv
from typing import List, Dict, Union


def preprocess_data(data_path: str) -> List[List[str]]:
    """
    Charge et prétraite un dataset transactionnel.
    
    Supporte :
    - Données catégorielles et numériques
    - Données pondérées ou non
    - Formats CSV et JSON
    
    Returns:
        List[List[str]]: Liste de transactions (items convertis en strings)
    """
    if data_path.endswith('.csv'):
        return load_csv_data(data_path)
    elif data_path.endswith('.json'):
        return load_json_data(data_path)
    else:
        raise ValueError("Format non supporté. Utilisez CSV ou JSON.")


def load_csv_data(data_path: str) -> List[List[str]]:
    """
    Charge un fichier CSV transactionnel.
    
    Formats supportés :
    1. transaction_id, item [, weight] - Format classique
    2. Une transaction par ligne, items séparés par virgules
    3. Matrice binaire (colonnes = items, lignes = transactions)
    """
    try:
        with open(data_path, 'r', encoding='utf-8') as file:            
            reader = csv.reader(file)
            header = next(reader)
            
            # Format 1: transaction_id, item [, weight]
            if 'transaction_id' in [h.lower().strip() for h in header] and 'item' in [h.lower().strip() for h in header]:
                return _load_transactional_format(reader, header)
            
            # Format 2: Matrice binaire ou une transaction par ligne
            elif len(header) > 2:
                file.seek(0)
                return _load_matrix_format(reader)
            
            # Format 3: Simple, essayer de deviner
            else:
                file.seek(0)
                return _load_simple_format(reader)
                
    except Exception as e:
        raise ValueError(f"Erreur lors du chargement du CSV : {e}")


def _load_transactional_format(reader, header: List[str]) -> List[List[str]]:
    """Charge le format transaction_id, item [, weight]"""
    # Trouver les indices des colonnes
    header_lower = [h.lower().strip() for h in header]
    
    try:
        trans_idx = header_lower.index('transaction_id')
    except ValueError:
        trans_idx = header_lower.index('trans_id') if 'trans_id' in header_lower else 0
    
    try:
        item_idx = header_lower.index('item')
    except ValueError:
        item_idx = header_lower.index('product') if 'product' in header_lower else 1
    
    # Grouper par transaction
    transactions_dict = {}
    for row in reader:
        if len(row) > max(trans_idx, item_idx):
            trans_id = row[trans_idx].strip()
            item = str(row[item_idx]).strip()
            
            if item and item.lower() not in ['', 'nan', 'null']:
                if trans_id not in transactions_dict:
                    transactions_dict[trans_id] = []
                transactions_dict[trans_id].append(item)
    
    transactions = list(transactions_dict.values())
    print(f"Chargé {len(transactions)} transactions (format transactionnel)")
    return transactions


def _load_matrix_format(reader) -> List[List[str]]:
    """Charge le format matrice (colonnes = items, valeurs = présence/poids)"""
    header = next(reader)
    transactions = []
    
    for row in reader:
        transaction = []
        for i, value in enumerate(row):
            if i < len(header) and value and value.strip():
                # Considérer comme présent si > 0 ou non vide
                try:
                    if float(value) > 0:
                        transaction.append(header[i].strip())
                except ValueError:
                    if value.strip().lower() not in ['0', 'false', 'no', '', 'nan']:
                        transaction.append(header[i].strip())
        
        if transaction:
            transactions.append(transaction)
    
    print(f"Chargé {len(transactions)} transactions (format matrice)")
    return transactions


def _load_simple_format(reader) -> List[List[str]]:
    """Charge un format simple : une transaction par ligne, items séparés"""
    transactions = []
    
    for row in reader:
        if row:
            # Joindre toute la ligne et séparer par virgules
            line = ','.join(row)
            items = [item.strip() for item in line.split(',') if item.strip()]
            if items:
                transactions.append(items)
    
    print(f"Chargé {len(transactions)} transactions (format simple)")
    return transactions


def load_json_data(data_path: str) -> List[List[str]]:
    """
    Charge un fichier JSON avec support des données pondérées.
    
    Formats supportés :
    - Liste de listes : [["A","B"], ["C","D"]]
    - Dictionnaire : {"user1": ["A","B"], "user2": ["C","D"]}
    - Format pondéré : [{"items": ["A","B"], "weights": [0.5, 0.8]}]
    - Format objets : [{"transaction_id": 1, "items": ["A","B"]}]
    """
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        transactions = []
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, list):
                    # Liste simple : ["A", "B", "C"]
                    transactions.append([str(x) for x in item if x])
                    
                elif isinstance(item, dict):
                    # Format objet avec différentes clés possibles
                    trans = _extract_transaction_from_dict(item)
                    if trans:
                        transactions.append(trans)
                        
                else:
                    # Item simple
                    transactions.append([str(item)])
            
        elif isinstance(data, dict):
            # Dictionnaire de transactions
            for key, value in data.items():
                if isinstance(value, list):
                    transactions.append([str(x) for x in value if x])
                elif isinstance(value, dict):
                    trans = _extract_transaction_from_dict(value)
                    if trans:
                        transactions.append(trans)
                else:
                    transactions.append([str(value)])
        
        else:
            raise ValueError("Format JSON non reconnu")
        
        # Filtrer les transactions vides
        transactions = [t for t in transactions if t]
        
        print(f"Chargé {len(transactions)} transactions depuis {data_path}")
        return transactions
        
    except Exception as e:
        raise ValueError(f"Erreur lors du chargement du JSON : {e}")


def _extract_transaction_from_dict(item_dict: Dict) -> List[str]:
    """Extrait une transaction d'un dictionnaire."""
    # Chercher les clés possibles pour les items
    possible_keys = ['items', 'products', 'events', 'sequence', 'data']
    
    for key in possible_keys:
        if key in item_dict:
            items = item_dict[key]
            if isinstance(items, list):
                return [str(x) for x in items if x]
            else:
                return [str(items)]
    
    # Si pas de clé spécifique, prendre toutes les valeurs
    values = []
    for key, value in item_dict.items():
        if key not in ['id', 'transaction_id', 'user_id', 'weight', 'weights']:
            if isinstance(value, list):
                values.extend([str(x) for x in value if x])
            else:
                values.append(str(value))
    
    return values


def get_data_summary(transactions: List[List[str]]) -> dict:
    """
    Retourne un résumé des données.
    """
    if not transactions:
        return {"error": "Aucune transaction"}
    
    total_transactions = len(transactions)
    all_items = [item for trans in transactions for item in trans]
    unique_items = len(set(all_items))
    avg_length = len(all_items) / total_transactions

    
    return {
        "total_transactions": total_transactions,
        "unique_items": unique_items,
        "avg_transaction_length": round(avg_length, 2),
        "sample_transactions": transactions[:3]
    }


def normalize_items(transactions: List[List[str]]) -> List[List[str]]:
    """
    Normalise les items pour gérer les données numériques et catégorielles.
    """
    normalized = []
    
    for transaction in transactions:
        normalized_trans = []
        for item in transaction:
            # Nettoyer l'item
            clean_item = str(item).strip()
            
            # Gérer les données numériques (les convertir en catégories si nécessaire)
            try:
                num_value = float(clean_item)
                # Si c'est un entier, le garder tel quel
                if num_value.is_integer():
                    clean_item = str(int(num_value))
                else:
                    # Pour les floats, arrondir à 2 décimales
                    clean_item = f"{num_value:.2f}"
            except ValueError:
                # Pas un nombre, garder tel quel mais normaliser
                clean_item = clean_item.lower().replace(' ', '_')
            
            if clean_item and clean_item not in ['nan', 'null', 'none', '']:
                normalized_trans.append(clean_item)
        
        if normalized_trans:
            normalized.append(normalized_trans)
    
    return normalized


if __name__ == "__main__":

    transactions = preprocess_data('data/bitcoin.csv')
    transactions = normalize_items(transactions)
    summary = get_data_summary(transactions)
    print()
    print("=== Résumé des données ===")
    print()
    for key, value in summary.items():
        print(f"{key}: {value}")