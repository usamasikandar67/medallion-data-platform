#!/usr/bin/env python3
import os
import sys
import base64
import requests

WORKSPACE_ID = "7474645859205897"
DEFAULT_HOST = f"https://adb-{WORKSPACE_ID}.azuredatabricks.net"

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", DEFAULT_HOST)
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def upload_file_to_dbfs(local_path, dbfs_path):
    """
    Uploads a local file to DBFS using the Databricks DBFS PUT API.
    """
    if not DATABRICKS_TOKEN:
        print("Error: DATABRICKS_TOKEN environment variable is missing.")
        print("Please configure DATABRICKS_TOKEN in your environment or .env file.")
        return False

    url = f"{DATABRICKS_HOST.rstrip('/')}/api/2.0/dbfs/put"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        if not os.path.exists(local_path):
            print(f"Error: Local file {local_path} not found.")
            return False

        with open(local_path, 'rb') as f:
            content = f.read()

        # File size limit for PUT API is 1MB. Larger files require multiple blocks streams,
        # but for config/scripts/metrics, PUT API is perfect.
        if len(content) > 1024 * 1024:
            print(f"Warning: File {local_path} exceeds 1MB. Large files should use multipart block stream API.")

        encoded_content = base64.b64encode(content).decode('utf-8')

        payload = {
            "path": dbfs_path,
            "contents": encoded_content,
            "overwrite": True
        }

        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            print(f"Success: Mapped {local_path} -> dbfs:{dbfs_path}")
            return True
        else:
            print(f"Error: Failed to upload. status code {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"Exception during upload execution: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 scripts/databricks_sync.py <local_path> <dbfs_path>")
        print("Example: python3 scripts/databricks_sync.py data/dashboard_stats.json /FileStore/dashboard_stats.json")
        sys.exit(1)

    local = sys.argv[1]
    dbfs = sys.argv[2]
    upload_file_to_dbfs(local, dbfs)
