#utils/checkpoint.py
import json
import os

from configs.settings import MANIFEST_PATH

def load_manifest():
    if not os.path.exists(MANIFEST_PATH):
        return {}
    
    with open(MANIFEST_PATH, 'r') as f:
        return json.load(f)
    
def save_manifest(manifest):

    os.makedirs(
    "data/metadata", 
    exist_ok=True
    )

    with open(MANIFEST_PATH, 'w') as f:
        json.dump(manifest, f, indent=4)

def is_processed(file_name):
    manifest = load_manifest()
    return manifest.get(file_name, False)

def mark_processed(file_name):
    manifest = load_manifest()
    manifest[file_name] = True
    save_manifest(manifest)
