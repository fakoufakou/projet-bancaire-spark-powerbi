from pyspark.sql import functions as F
from spark_session import create_spark_session

# 1 SESSION SPARK + PATHS
spark = create_spark_session("Projet_Bancaire_Gold")

SILVER_PATH = "/home/jelassi/projet_bancaire/data_clean/"
GOLD_PATH = "/home/jelassi/projet_bancaire/data_gold/"

print("📌 Chargement des tables Silver...")

clients = spark.read.parquet(f"{SILVER_PATH}clients_silver.parquet")
comptes = spark.read.parquet(f"{SILVER_PATH}comptes_silver.parquet")
transactions = spark.read.parquet(f"{SILVER_PATH}transactions_silver.parquet")
cartes = spark.read.parquet(f"{SILVER_PATH}cartes_silver.parquet")
fraude = spark.read.parquet(f"{SILVER_PATH}fraude_silver.parquet")

print("✔️ Silver chargé\n")

# 2 DIM DATE

print("📌 Construction DIM_DATE...")

dim_date = (
    transactions
    .select(F.to_date("date_transaction").alias("date"))
    .dropna()
    .distinct()
    .withColumn("annee", F.year("date"))
    .withColumn("mois", F.month("date"))
    .withColumn("jour", F.dayofmonth("date"))
    .withColumn("trimestre", F.quarter("date"))
    .withColumn("mois_texte", F.date_format("date", "MMMM"))
)

dim_date.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_date.parquet")
print("✔️ dim_date OK")

# 3 AGRÉGATIONS (TX + FRAUDE) 
print("📌 Calcul des agrégations TX + FRAUDE par compte...")

tx_by_account = (
    transactions.groupBy("account_id")
    .agg(
        F.count("*").alias("nb_transactions"),
        F.sum(F.when(F.col("type_transaction") == "Crédit", F.col("montant")).otherwise(0)).alias("montant_total_credit"),
        F.sum(F.when(F.col("type_transaction") == "Débit", F.col("montant")).otherwise(0)).alias("montant_total_debit")
    )
)

fraude_by_account = (
    fraude.groupBy("account_id")
    .agg(
        F.count("*").alias("nb_fraudes"),
        F.max("score_fraude").alias("score_fraude_max")
    )
)

comptes_gold = (
    comptes
    .join(tx_by_account, on="account_id", how="left")
    .join(fraude_by_account, on="account_id", how="left")
)

comptes_gold = comptes_gold.fillna({
    "nb_transactions": 0,
    "montant_total_credit": 0.0,
    "montant_total_debit": 0.0,
    "nb_fraudes": 0,
    "score_fraude_max": 0
})

# 4 DIM_CLIENT

print("📌 Construction DIM_CLIENT...")

dim_client = clients.select(
    "client_id",
    "nom",
    "prenom",
    "segment_client",
    "nationalite",
    "date_naissance",
    "pays"
).withColumn(
    "age", F.year(F.current_date()) - F.year("date_naissance")
)

dim_client.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_client.parquet")
print("✔️ dim_client OK")


# 5 DIM_COMPTE

print("📌 Construction DIM_COMPTE...")

dim_compte = comptes_gold.select(
    "account_id",
    "client_id",
    "type_compte",
    "statut_compte",
    "solde",
    "devise_compte",
    "date_ouverture",
    "nb_transactions",
    "montant_total_credit",
    "montant_total_debit",
    "nb_fraudes",
    "score_fraude_max"
)

dim_compte.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_compte.parquet")
print("✔️ dim_compte OK")

# 6. DIM_CARTE (corrigé selon tes données réelles)
print("📌 Construction DIM_CARTE...")

dim_carte = cartes.select(
    "card_id",
    "account_id",
    "type_carte",
    "date_creation_carte",
    "date_expiration",
    "statut_carte",
    "limite_mensuelle"
)

dim_carte.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_carte.parquet")
print("✔️ dim_carte OK")

# 7 FACT_TRANSACTIONS 
print("📌 Construction FACT_TRANSACTIONS...")

fact_transactions = (
    transactions
    .join(dim_compte.select("account_id", "client_id"), on="account_id", how="left")
    .withColumn("date_id", F.to_date("date_transaction"))
)

fact_transactions = fact_transactions.select(
    "transaction_id",
    "date_id",
    "client_id",
    "account_id",
    "montant",
    "type_transaction",
    "devise_tx",
    "canal",
    "categorie",
    "date_transaction"
)

fact_transactions.write.mode("overwrite").parquet(f"{GOLD_PATH}fact_transactions.parquet")
print("✔️ fact_transactions OK")

# 8 FACT_FRAUDE (même logique, avec date_id)

print("📌 Construction FACT_FRAUDE...")

fact_fraude = (
    fraude
    .join(dim_compte.select("account_id", "client_id"), on="account_id", how="left")
    .withColumn("date_id", F.to_date("date_fraude"))
)

fact_fraude = fact_fraude.select(
    "fraude_id",
    "date_id",
    "client_id",
    "account_id",
    "type_fraude",
    "score_fraude",
    "statut_fraude",
    "date_fraude",
    "transaction_id",
    "commentaire"  
)

fact_fraude.write.mode("overwrite").parquet(f"{GOLD_PATH}fact_fraude.parquet")
print("✔️ fact_fraude OK")
print("\n🎉 Pipeline Silver → Gold ")
