from astrapy import DataAPIClient

# Initialize the client
client = DataAPIClient()
db = client.get_database(
  "https://c2c5f879-9b37-4011-83c8-3b87b292013d-us-east-2.apps.astra.datastax.com",
  token="AstraCS:hgAdZLhMSLDWaPpozmlEfNpG:aff53a117e9d93a4d9c8980367a0f6d1c144272f8c4028fd100141c2d6ee456b"
)

print(f"Connected to Astra DB: {db.list_collection_names()}")