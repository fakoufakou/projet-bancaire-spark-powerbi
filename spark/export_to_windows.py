from pyspark.sql import SparkSession
from pathlib import Path

spark = SparkSession.builder.appName("ExportGoldToWindows").getOrCreate()

GOLD_WSL = "/home/jelassi/projet_bancaire/data_gold/"
GOLD_WIN = "/mnt/c/_WSL_Dataexport/gold_files/"

Path(GOLD_WIN).mkdir(parents=True, exist_ok=True)

tables = [
    "dim_client",
    "dim_compte",
    "dim_carte",
    "dim_date",
    "fact_transactions",
    "fact_fraude",
]

for t in tables:
    print(f" Export {t}")

    df = spark.read.parquet(f"{GOLD_WSL}{t}.parquet")

    df.write.mode("overwrite").parquet(f"{GOLD_WIN}{t}.parquet")

    print(f"✔️ {t} exporté → {GOLD_WIN}{t}.parquet")

print("\n🎉 Export terminé")
