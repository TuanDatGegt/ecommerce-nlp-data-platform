#pipeline/bronze/state/checkpoint.py
import json
import tempfile
import os

from pipeline.bronze.storage.minio_client import MinioClient

from configs.settings import MINIO_BUCKET_NAME, CHECKPOINT_OBJECT

storage = MinioClient()

def load_manifest():
    temp_dir = tempfile.mkdtemp()
    local_manifest = os.path.join(temp_dir, "bronze_manifest.json")
    try:
        storage.download_file(bucket_name=MINIO_BUCKET_NAME, object_name=CHECKPOINT_OBJECT, file_path=local_manifest)

        with open(local_manifest, "r") as f:
            manifest = json.load(f)

    except Exception:
        manifest={}

    finally:
        if os.path.exists(local_manifest):
            os.remove(local_manifest)

    return manifest


    
def save_manifest(manifest):

    temp_dir = tempfile.mkdtemp()
    local_manifest = os.path.join(temp_dir, "bronze_manifest.json")

    with open(local_manifest, 'w') as f:
        json.dump(manifest, f, indent=4)

    storage.upload_file(bucket_name=MINIO_BUCKET_NAME, object_name=CHECKPOINT_OBJECT, file_path=local_manifest)

    os.remove(local_manifest)





def is_processed(file_name):
    manifest = load_manifest()
    return manifest.get(file_name, False)

def mark_processed(file_name):
    manifest = load_manifest()
    manifest[file_name] = {"processed": True}
    save_manifest(manifest)


def reset_checkpoint(file_name):
    manifest = load_manifest()
    if file_name in manifest:
        manifest[file_name]
    save_manifest(manifest)


def list_processed_files():
    manifest = load_manifest()
    return list(manifest.keys())
