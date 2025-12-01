#!/usr/bin/env python3
from __future__ import annotations
import argparse
import random
from datetime import datetime, timedelta
from google.cloud import datastore

def parse_args():
    p = argparse.ArgumentParser(description="Seed Datastore for Tiny Instagram")
    p.add_argument('--users', type=int, default=5)
    p.add_argument('--posts', type=int, default=30)
    p.add_argument('--follows-min', type=int, default=1)
    p.add_argument('--follows-max', type=int, default=3)
    p.add_argument('--prefix', type=str, default='user')
    p.add_argument('--dry-run', action='store_true')
    return p.parse_args()

def ensure_users(client: datastore.Client, names: list[str], dry: bool):
    created = 0
    batch = []
    
    for name in names:
        key = client.key('User', name)
        entity = datastore.Entity(key)
        entity['follows'] = []
        batch.append(entity)
        
        if len(batch) >= 400:
            if not dry:
                client.put_multi(batch)
            created += len(batch)
            batch = []
            print(".", end="", flush=True)

    if batch:
        if not dry:
            client.put_multi(batch)
        created += len(batch)
        
    print(f" ({created} users traités)")
    return created

def assign_follows(client: datastore.Client, names: list[str], fmin: int, fmax: int, dry: bool):
    
    batch = []
    count = 0
    
    for name in names:
        key = client.key('User', name)
        entity = datastore.Entity(key)
        
        others = [u for u in names if u != name]
        if others:
            target_count = random.randint(min(fmin, len(others)), min(fmax, len(others)))
            selection = random.sample(others, target_count)
            entity['follows'] = sorted(selection)
        else:
            entity['follows'] = []
            
        batch.append(entity)

        if len(batch) >= 400:
            if not dry:
                client.put_multi(batch)
            batch = []
            print(".", end="", flush=True)
            
    if batch and not dry:
        client.put_multi(batch)

def create_posts(client: datastore.Client, names: list[str], total_posts: int, dry: bool):
    if not names or total_posts <= 0:
        return 0
    
    created = 0
    base_time = datetime.utcnow()
    batch = []

    print(f"Génération de {total_posts} posts en mode batch...")

    for i in range(total_posts):
        author = random.choice(names)
        key = client.key('Post')
        post = datastore.Entity(key)
        
        post['author'] = author
        post['content'] = f"Seed post {i+1} by {author}"
        post['created'] = base_time - timedelta(seconds=i)
        
        batch.append(post)

        if len(batch) >= 400:
            if not dry:
                try:
                    client.put_multi(batch)
                except Exception as e:
                    print(f"x", end="", flush=True)
                    pass
            created += len(batch)
            batch = []
            if created % 5000 == 0:
                print(f" {created}", end="", flush=True)
            else:
                print(".", end="", flush=True)

    if batch:
        if not dry:
            client.put_multi(batch)
        created += len(batch)
        
    return created

def main():
    args = parse_args()
    client = datastore.Client()

    user_names = [f"{args.prefix}{i}" for i in range(1, args.users + 1)]

    print(f"[Seed] Configuration: {args.users} users, {args.posts} posts total.")

    print("[Seed] Création Users + Follows...")
    assign_follows(client, user_names, args.follows_min, args.follows_max, args.dry_run)
    print("\n[Seed] Users terminés.")

    print("[Seed] Création des Posts...")
    created_posts = create_posts(client, user_names, args.posts, args.dry_run)
    print(f"\n[Seed] {created_posts} posts créés.")

    print("[Seed] Terminé.")

if __name__ == '__main__':
    main()
