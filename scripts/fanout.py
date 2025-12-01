import os
import subprocess
import csv
import time
import pandas as pd

# PATHS
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
HOME_DIR = os.path.dirname(PROJECT_ROOT)

TARGET_HOST = "https://tpbigdata-473713.ew.r.appspot.com"
OUTPUT_FILE = os.path.join(PROJECT_ROOT, 'csv', 'fanout.csv')

LOCUST_FILE = os.path.join(SCRIPT_DIR, 'locustfile.py')
CLEAN_SCRIPT = os.path.join(SCRIPT_DIR, 'clean.py')
SEED_SCRIPT = os.path.join(SCRIPT_DIR, 'seed.py')

# PARAMS
FOLLOW_STEPS = [10, 50, 100]
LOCUST_USERS = 50
DB_TOTAL_USERS = 1000
POSTS_PER_USER = 100
TOTAL_POSTS_TO_SEED = DB_TOTAL_USERS * POSTS_PER_USER
RUNS_PER_STEP = 3

def run_external_script(script_path, args=None):
    if not os.path.exists(script_path):
        print(f"ERREUR : Le script {script_path} est introuvable.")
        exit(1)
        
    cmd = ["python3", script_path]
    if args:
        cmd.extend(args)
        
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERREUR lors de l'exécution de {os.path.basename(script_path)}: {e}")
        exit(1)

def clean_database():
    run_external_script(CLEAN_SCRIPT)

def seed_database(follow_count):
    print(f"--- SEEDING: {follow_count} follows/user | 100 posts/user (Total Posts: {TOTAL_POSTS_TO_SEED}) ---")
    
    args = [
        "--users", str(DB_TOTAL_USERS),
        "--posts", str(TOTAL_POSTS_TO_SEED),
        "--follows-min", str(follow_count),
        "--follows-max", str(follow_count),
        "--prefix", "user"
    ]
    run_external_script(SEED_SCRIPT, args)

def run_benchmark():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    print(f"--- LANCEMENT DU BENCHMARK (Variable: Fanout/Follows) ---")
    print(f"Fichier de sortie : {OUTPUT_FILE}")

    with open(OUTPUT_FILE, 'w', newline='') as csvfile:
        fieldnames = ['PARAM', 'AVG_TIME', 'RUN', 'FAILED']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for follow_count in FOLLOW_STEPS:
            print(f"\n=============================================")
            print(f" ÉTAPE : {follow_count} followees par utilisateur")
            print(f"=============================================")
            
            clean_database()
            seed_database(follow_count)
            
            for run in range(1, RUNS_PER_STEP + 1):
                print(f"  -> Run {run}/{RUNS_PER_STEP} (Charge: {LOCUST_USERS} users)...", end=" ", flush=True)
                
                temp_csv_prefix = os.path.join(SCRIPT_DIR, "temp_locust_fanout")
                
                cmd = [
                    "locust",
                    "-f", LOCUST_FILE,
                    "--headless",
                    "-u", str(LOCUST_USERS),
                    "-r", str(LOCUST_USERS), 
                    "--run-time", "10s",
                    "--host", TARGET_HOST,
                    "--csv", temp_csv_prefix
                ]
                
                try:
                    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    
                    stats_file = f"{temp_csv_prefix}_stats.csv"
                    avg_time = 0
                    failed = 1
                    
                    if os.path.exists(stats_file):
                        try:
                            df = pd.read_csv(stats_file)
                            total_row = df[df['Name'] == 'Aggregated']
                            if total_row.empty and not df.empty:
                                total_row = df.iloc[[-1]]

                            if not total_row.empty:
                                avg_time = int(total_row.iloc[0]['Average Response Time'])
                                fail_count = int(total_row.iloc[0]['Failure Count'])
                                failed = 1 if fail_count > 0 else 0
                        except: pass
                        
                    writer.writerow({'PARAM': follow_count, 'AVG_TIME': f"{avg_time}ms", 'RUN': run, 'FAILED': failed})
                    csvfile.flush()
                    print(f" Result: {avg_time}ms | Failed: {failed}")
                    
                    for ext in ['_stats.csv', '_failures.csv', '_stats_history.csv', '_exceptions.csv']:
                        f_path = f"{temp_csv_prefix}{ext}"
                        if os.path.exists(f_path): os.remove(f_path)
                            
                except Exception as e:
                    print(f" Erreur: {e}")
                    writer.writerow({'PARAM': follow_count, 'AVG_TIME': "0ms", 'RUN': run, 'FAILED': 1})

                time.sleep(2)

# DEPENDANCES
if __name__ == "__main__":
    try:
        import pandas
        import locust
    except ImportError:
        subprocess.run(["pip", "install", "pandas", "locust", "google-cloud-datastore"], check=True)

    run_benchmark()
    print(f"\nTerminé ! Résultats dans : {OUTPUT_FILE}")
