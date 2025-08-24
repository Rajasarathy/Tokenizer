import logging
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from io import StringIO, BytesIO
import os
import azure.functions as func

app = func.FunctionApp()

# --- Config from env / local.settings.json ---
AZURE_STORAGE_CONNECTION_STRING = os.getenv("c7fe8f_STORAGE")
CONTAINER_NAME = "csv"
INPUT_FOLDER = "all"
OUTPUT_FOLDER = "secured"
TOKEN_API_URL = os.getenv("TOKEN_API_URL")


# --- Helper: batch tokenization ---
def batch_tokenize(all_values):
    if not all_values:
        return []
    payload = {"data": [[v] for v in all_values]}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(TOKEN_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        results = response.json().get("results", [])
        return [t[0] for t in results]
    except Exception as e:
        logging.error(f"❌ Tokenization API error: {e}")
        return [None] * len(all_values)


# --- Trigger: process each uploaded CSV ---
@app.blob_trigger(arg_name="myblob", path=f"{CONTAINER_NAME}/{INPUT_FOLDER}/{{name}}",
                  connection="c7fe8f_STORAGE")
def blob_trigger(myblob: func.InputStream):
    logging.info(f"📂 Processing {myblob.name}")

    # Read CSV
    content = myblob.read().decode("utf-8")
    df = pd.read_csv(StringIO(content))

    if "Credit_Card_Number" not in df.columns:
        logging.warning("⚠️ Missing Credit_Card_Number column, skipping file.")
        return

    # Tokenize
    cards = df["Credit_Card_Number"].tolist()
    tokens = batch_tokenize(cards)
    df["CREDIT_CARD_NUMBER"] = tokens
    df.drop(columns=["Credit_Card_Number"], inplace=True)

    # Upload to secured/ folder
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    out_buf = BytesIO()
    df.to_csv(out_buf, index=False)
    out_buf.seek(0)

    filename = os.path.basename(myblob.name)
    secured_name = f"{OUTPUT_FOLDER}/{filename}"

    container_client.get_blob_client(secured_name).upload_blob(out_buf, overwrite=True)
    logging.info(f"✅ Tokenized file uploaded to: {CONTAINER_NAME}/{secured_name}")
