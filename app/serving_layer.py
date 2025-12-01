import json
import glob

# Récupération de tous les fichiers JSON produits par la Batch Layer
# Spark crée souvent plusieurs fichiers de sortie, d'où l'utilisation de glob
batch_files = glob.glob("/app/batch_output/*.json")
batch_data = []

# Lecture de chaque fichier JSON et chargement des lignes dans une liste Python
for file in batch_files:
    with open(file) as f:
        for line in f:
            batch_data.append(json.loads(line))

# Simulation de la vue streaming.
# Dans un système réel, ces valeurs viendraient d'une base NoSQL, Redis, etc.
# Ces valeurs représentent le total des transactions reçues DEPUIS la dernière exécution du batch.
streaming_data = {
    "Ali": 80,
    "Mounir": 200,
    "Sara": 10 
}


# Dictionnaire qui va contenir la vue finale (Serving View)
final = {}

# Fusion Batch + Streaming :
# Pour chaque client, on additionne :
# - le total historique (batch_total)
# - le total temps réel (streaming_total)
for entry in batch_data:
    # Les colonnes de la Batch View sont : {"customer": "...", "sum(amount)": ...}
    customer = entry["customer"]
    batch_total = entry["total_amount"]
    
    # Récupère le delta du streaming, ou 0 si aucune donnée streaming pour ce client
    streaming_total = streaming_data.get(customer, 0)
    
    # Calcul de la vue finale
    final[customer] = batch_total + streaming_total

# Affichage de la vue finale
print("=== Final Serving View ===")
print(final)