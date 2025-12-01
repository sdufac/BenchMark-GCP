import argparse
from google.cloud import datastore

def clean_datastore():
    print("--- [CLEAN] NETTOYAGE DU DATASTORE ---")
    
    try:
        client = datastore.Client()
    except Exception as e:
        print(f"Erreur de connexion au Datastore: {e}")
        return

    kinds = ['Post', 'User']
    
    for kind in kinds:
        print(f"Recherche des entités '{kind}'...", end=" ", flush=True)
        query = client.query(kind=kind)
        query.keys_only()
        
        keys = list(query.fetch())
        total_keys = len(keys)
        
        if total_keys == 0:
            print("Aucune entité trouvée.")
            continue

        print(f"{total_keys} entités trouvées. Suppression en cours...")

        chunk_size = 400
        for i in range(0, total_keys, chunk_size):
            chunk = keys[i:i + chunk_size]
            client.delete_multi(chunk)
            print(".", end="", flush=True)
        
        print(" Terminé.")
    
    print("[CLEAN] Base de données vidée avec succès.\n")

if __name__ == "__main__":
    clean_datastore()
