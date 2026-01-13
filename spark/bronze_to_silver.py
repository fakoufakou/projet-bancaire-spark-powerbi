from pyspark.sql import functions as F
from pyspark.sql.types import *
from spark_session import create_spark_session

# Creation session , definition des chemins
spark = create_spark_session()
RAW_PATH = "/home/jelassi/projet_bancaire/data_raw/"
CLEAN_PATH = "/home/jelassi/projet_bancaire/data_clean/"

# Functions

def clean_columns(df):
    """
    Nettoie les noms de colonnes : minuscules, underscore, sans espaces
    """
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower().strip().replace(" ", "_"))
    return df

# RGPD functions
def mask_name(col):
    return F.concat(F.substring(col, 1, 1), F.lit("***"))

def mask_email(col):
    return F.concat(F.lit("***@"), F.split(col, "@").getItem(1))

def hash_col(col):
    return F.sha2(F.col(col).cast("string"), 256)

def mask_iban(col):
    return F.concat(F.lit("****"), F.substring(col, -4, 4))

def mask_card(col):
    return F.concat(F.lit("**** **** **** "), F.substring(col, -4, 4))


# Clients

print("📌 Chargement des clients...")
clients = spark.read.csv(f"{RAW_PATH}clients_lundi.csv", header=True, inferSchema=True)
clients = clean_columns(clients)

# Conversion dates
clients = clients.withColumn("date_naissance", F.to_date("date_naissance")) \
                 .withColumn("date_creation_client", F.to_date("date_creation_client")) \
                 .withColumn("date_derniere_mise_a_jour", F.to_date("date_derniere_mise_a_jour"))

#  RGPD
if "nom" in clients.columns:
    clients = clients.withColumn("nom", mask_name(F.col("nom")))

if "prenom" in clients.columns:
    clients = clients.withColumn("prenom", mask_name(F.col("prenom")))

if "email" in clients.columns:
    clients = clients.withColumn("email", mask_email(F.col("email")))

if "iban" in clients.columns:
    clients = clients.withColumn("iban", mask_iban(F.col("iban")))

if "adresse" in clients.columns:
    clients = clients.withColumn("adresse", F.lit("ANONYMIZED"))

if "client_id" in clients.columns:
    clients = clients.withColumn("client_id", hash_col("client_id"))

clients.write.mode("overwrite").parquet(f"{CLEAN_PATH}clients_silver.parquet")
print("✔️ Clients → Silver OK")

# Comptes

print("📌 Chargement des comptes...")
comptes = spark.read.csv(f"{RAW_PATH}comptes_lundi.csv", header=True, inferSchema=True)
comptes = clean_columns(comptes)

if "devise" in comptes.columns:
    comptes = comptes.withColumnRenamed("devise", "devise_compte")

comptes = comptes.withColumn("date_ouverture", F.to_date("date_ouverture")) \
                 .withColumn("date_derniere_mise_a_jour", F.to_date("date_derniere_mise_a_jour"))

# RGPD 
if "iban" in comptes.columns:
    comptes = comptes.withColumn("iban", mask_iban(F.col("iban")))

if "compte_id" in comptes.columns:
    comptes = comptes.withColumn("compte_id", hash_col("compte_id"))

if "client_id" in comptes.columns:
    comptes = comptes.withColumn("client_id", hash_col("client_id"))

comptes.write.mode("overwrite").parquet(f"{CLEAN_PATH}comptes_silver.parquet")
print("✔️ Comptes → Silver OK")

# TRANSACTIONS

print("📌 Chargement des transactions...")
transactions = spark.read.csv(f"{RAW_PATH}transactions_lundi.csv", header=True, inferSchema=True)
transactions = clean_columns(transactions)

if "devise" in transactions.columns:
    transactions = transactions.withColumnRenamed("devise", "devise_tx")

transactions = transactions.withColumn("date_transaction", F.to_timestamp("date_transaction"))

# RGPD 
if "client_id" in transactions.columns:
    transactions = transactions.withColumn("client_id", hash_col("client_id"))

if "compte_id" in transactions.columns:
    transactions = transactions.withColumn("compte_id", hash_col("compte_id"))

if "carte_id" in transactions.columns:
    transactions = transactions.withColumn("carte_id", hash_col("carte_id"))

transactions.write.mode("overwrite").parquet(f"{CLEAN_PATH}transactions_silver.parquet")
print("✔️ Transactions → Silver OK")

# CARTES

print("📌 Chargement des cartes...")
cartes = spark.read.csv(f"{RAW_PATH}cartes_lundi.csv", header=True, inferSchema=True)
cartes = clean_columns(cartes)

cartes = cartes.withColumn("date_creation_carte", F.to_date("date_creation_carte"))

# RGPD 
if "numero_carte" in cartes.columns:
    cartes = cartes.withColumn("numero_carte", mask_card(F.col("numero_carte")))

if "client_id" in cartes.columns:
    cartes = cartes.withColumn("client_id", hash_col("client_id"))

cartes.write.mode("overwrite").parquet(f"{CLEAN_PATH}cartes_silver.parquet")
print("✔️ Cartes → Silver OK")


# Fraude

print("📌 Chargement des fraudes...")
fraude = spark.read.csv(f"{RAW_PATH}fraude_lundi.csv", header=True, inferSchema=True)
fraude = clean_columns(fraude)

fraude = fraude.withColumn("date_fraude", F.to_timestamp("date_fraude"))

# ---------- RGPD ----------
if "client_id" in fraude.columns:
    fraude = fraude.withColumn("client_id", hash_col("client_id"))

if "carte_id" in fraude.columns:
    fraude = fraude.withColumn("carte_id", hash_col("carte_id"))

fraude.write.mode("overwrite").parquet(f"{CLEAN_PATH}fraude_silver.parquet")
print("✔️ Fraude → Silver OK")

print("\n🎉 Pipeline Bronze → Silver terminé avec RGPD ! 🔒")

