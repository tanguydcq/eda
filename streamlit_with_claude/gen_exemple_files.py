# Script pour créer des fichiers d'exemple pour tester l'application

# ============================================================
# 1. FORMAT LONG (transaction_id, item)
# ============================================================

long_format_data = """transaction_id,item
1,eggs
1,milk
1,yogurt
1,cereal
1,lettuce
1,butter
2,eggs
2,onion
2,butter
2,rice
2,bread
2,milk
3,banana
4,lettuce
4,milk
5,banana
5,onion
5,rice
5,cereal
5,pasta
6,yogurt
6,milk
6,apple
6,rice
6,tea
7,cheese
7,banana"""

with open('example_long_format.csv', 'w') as f:
    f.write(long_format_data)

print("✅ Fichier 'example_long_format.csv' créé")

# ============================================================
# 2. FORMAT WIDE (items par ligne, séparés par espaces)
# ============================================================

wide_format_data = """1 2 3 4 5 6
1 7 6 8 9 2
10
5 2
10 7 8 4 11
3 2 12 8 13
14 10 13
6
1 15 16 3
11
1 17 6 18 15
10 6 9 4 18
4
17
11 15 12
15 10 3
6 12 7 4 16 5
3 7 4 13
4
13
3 6 10 13
19 17
11 14 3 16 4 15
7 3 8 17 15
14 5
6 9 1 14
12 8 6 17 5 18"""

with open('example_wide_format.txt', 'w') as f:
    f.write(wide_format_data)

print("✅ Fichier 'example_wide_format.txt' créé")

# ============================================================
# 3. FORMAT WIDE avec noms d'items (plus lisible)
# ============================================================

wide_format_named = """bread milk eggs butter cheese
bread butter coffee
milk yogurt
eggs bacon toast
bread milk butter jam
coffee croissant
milk cereal banana
bread cheese ham lettuce
eggs milk bread
butter jam toast bread
milk yogurt apple banana
bread cheese
coffee croissant butter
milk eggs bread butter
cereal milk banana
bread ham cheese lettuce tomato
eggs bacon coffee
milk bread
butter jam
bread milk eggs"""

with open('example_wide_named.txt', 'w') as f:
    f.write(wide_format_named)

print("✅ Fichier 'example_wide_named.txt' créé")

# ============================================================
# 4. FORMAT LONG étendu (plus de données)
# ============================================================

import pandas as pd

# Générer un dataset plus large
transactions = []
for tid in range(1, 101):
    # Chaque transaction a entre 2 et 8 items
    import random
    items = ['bread', 'milk', 'eggs', 'butter', 'cheese', 'yogurt', 
             'apple', 'banana', 'orange', 'lettuce', 'tomato', 'onion',
             'rice', 'pasta', 'cereal', 'coffee', 'tea', 'juice']
    
    n_items = random.randint(2, 8)
    selected_items = random.sample(items, n_items)
    
    for item in selected_items:
        transactions.append({'transaction_id': tid, 'item': item})

df = pd.DataFrame(transactions)
df.to_csv('example_large_dataset.csv', index=False)

print("✅ Fichier 'example_large_dataset.csv' créé (100 transactions)")

print("\n" + "="*60)
print("RÉSUMÉ DES FICHIERS CRÉÉS :")
print("="*60)
print("1. example_long_format.csv      - Format long basique")
print("2. example_wide_format.txt      - Format wide avec IDs numériques")
print("3. example_wide_named.txt       - Format wide avec noms d'items")
print("4. example_large_dataset.csv    - Format long avec 100 transactions")
print("="*60)