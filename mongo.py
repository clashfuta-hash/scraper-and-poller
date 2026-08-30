"""
One-off diagnostic: tests Mongo Atlas connectivity with cert validation
relaxed, to isolate "TLS interception" vs "OpenSSL/driver negotiation"
as the cause of the SSL handshake failures.

Run from the project folder (same folder as .env):
    python test_mongo.py
"""
from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
import pymongo

load_dotenv()

uri = os.environ.get("MONGO_URI")
if not uri:
    print("MONGO_URI not found in .env — check it's in the same folder as this script.")
    sys.exit(1)

print("Connecting with tlsAllowInvalidCertificates=True ...")
client = pymongo.MongoClient(
    uri,
    tls=True,
    tlsAllowInvalidCertificates=True,
    serverSelectionTimeoutMS=10000,
)

try:
    result = client.admin.command("ping")
    print("SUCCESS:", result)
except Exception as exc:
    print("FAILED:", exc)
finally:
    client.close()