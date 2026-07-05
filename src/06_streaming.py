# ============================================================
# 06_streaming.py  -  Bonus : Structured Streaming
# ============================================================
# Simulation d'un flux d'accidents en temps réel.
#
# Principe :
#   1. On découpe la silver en mini-batches (1 fichier = 1 micro-batch)
#   2. Spark Structured Streaming lit le dossier stream_input/ en continu
#   3. Deux agrégats continus sont calculés à chaque batch :
#      - Accidents par département
#      - Tués par condition atmosphérique
#
# Pour observer : laisser tourner et voir les résultats se mettre
# à jour dans le terminal à chaque nouveau fichier détecté.
# ============================================================

import time
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, LongType, DateType, FloatType
)

# ─────────────────────────────────────────────────────────────
# SESSION SPARK
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ONISR_Streaming_Jour4")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✅ Session Spark Streaming démarrée — Spark UI : http://localhost:4040")

# ─────────────────────────────────────────────────────────────
# CHEMINS
# ─────────────────────────────────────────────────────────────
SILVER       = "data/silver"
STREAM_INPUT = "data/stream_input"
CHECKPOINT1  = "data/stream_checkpoint/dep"
CHECKPOINT2  = "data/stream_checkpoint/atm"

# ─────────────────────────────────────────────────────────────
# 1. PRÉPARATION DU FLUX SIMULÉ
# ─────────────────────────────────────────────────────────────
# On relit la silver et on la découpe en 10 mini-fichiers CSV
# qui seront déposés 1 par 1 pour simuler un flux temps réel

print("\n── Préparation du flux simulé ──────────────────────────────")

# Nettoyer les dossiers de streaming
for path in [STREAM_INPUT, CHECKPOINT1, CHECKPOINT2]:
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)

# Relire la silver caractéristiques + usagers jointes
silver_carac = spark.read.parquet(f"{SILVER}/caracteristiques")
silver_usag  = spark.read.parquet(f"{SILVER}/usagers")

df_joint = (
    silver_carac.select("Num_Acc", "dep", "atm", "date_accident", "heure")
    .join(silver_usag.select("Num_Acc", "grav"), on="Num_Acc", how="inner")
)

# Découper en 10 partitions et écrire en CSV dans stream_input/
# On garde uniquement les colonnes nécessaires
(df_joint
 .repartition(10)
 .write
 .mode("overwrite")
 .option("header", "true")
 .csv(f"{STREAM_INPUT}/source"))

# Lister les fichiers générés
fichiers = sorted([
    f for f in os.listdir(f"{STREAM_INPUT}/source")
    if f.endswith(".csv")
])
print(f"  {len(fichiers)} fichiers CSV générés dans {STREAM_INPUT}/source/")

# ─────────────────────────────────────────────────────────────
# 2. SCHÉMA DU FLUX
# ─────────────────────────────────────────────────────────────
# Structured Streaming requiert un schéma explicite (pas d'inferSchema)

schema_flux = StructType([
    StructField("Num_Acc",       LongType(),    True),
    StructField("dep",           StringType(),  True),
    StructField("atm",           IntegerType(), True),
    StructField("date_accident", DateType(),    True),
    StructField("heure",         IntegerType(), True),
    StructField("grav",          IntegerType(), True),
])

# ─────────────────────────────────────────────────────────────
# 3. LECTURE DU FLUX (readStream)
# ─────────────────────────────────────────────────────────────
print("\n── Démarrage des requêtes streaming ────────────────────────")

# Spark surveille ce dossier et traite chaque nouveau fichier
df_stream = (
    spark.readStream
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)   # 1 fichier par micro-batch
    .schema(schema_flux)
    .csv(f"{STREAM_INPUT}/source")
)

# ─────────────────────────────────────────────────────────────
# 4. AGRÉGAT 1 — Accidents par département (continu)
# ─────────────────────────────────────────────────────────────

query1 = (
    df_stream
    .groupBy("dep")
    .agg(
        F.count("*").alias("nb_accidents"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues")
    )
    .orderBy(F.col("nb_accidents").desc())
    .writeStream
    .outputMode("complete")          # recalcule tout à chaque batch
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .option("checkpointLocation", CHECKPOINT1)
    .queryName("accidents_par_dep")
    .trigger(processingTime="5 seconds")
    .start()
)

# ─────────────────────────────────────────────────────────────
# 5. AGRÉGAT 2 — Tués par condition atmosphérique (continu)
# ─────────────────────────────────────────────────────────────

query2 = (
    df_stream
    .groupBy("atm")
    .agg(
        F.count("*").alias("nb_usagers"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues"),
        F.round(
            F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)) /
            F.when(F.count("*") > 0, F.count("*")).otherwise(None) * 100, 2
        ).alias("pct_tues")
    )
    .orderBy(F.col("pct_tues").desc())
    .writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .option("checkpointLocation", CHECKPOINT2)
    .queryName("tues_par_atm")
    .trigger(processingTime="5 seconds")
    .start()
)

print("  ✅ 2 requêtes streaming actives :")
print("     - accidents_par_dep  : accidents et tués par département")
print("     - tues_par_atm       : % de tués par condition atmosphérique")
print("\n  Les résultats se mettent à jour toutes les 5 secondes.")
print("  Observe les totaux augmenter à chaque micro-batch.")
print("\n  Spark UI : http://localhost:4040 → onglet 'Structured Streaming'")

# ─────────────────────────────────────────────────────────────
# 6. INJECTION DU FLUX SIMULÉ
# ─────────────────────────────────────────────────────────────
# On déplace les fichiers 1 par 1 dans le dossier surveillé,
# avec un délai de 5s entre chaque pour simuler un flux réel

print("\n── Injection du flux (1 fichier toutes les 5s) ─────────────")

watch_dir = f"{STREAM_INPUT}/watch"
os.makedirs(watch_dir, exist_ok=True)

# Reconfigurer readStream pour surveiller watch_dir
# (on recrée les queries sur le bon dossier)
query1.stop()
query2.stop()

# Nettoyer checkpoints
for path in [CHECKPOINT1, CHECKPOINT2]:
    shutil.rmtree(path)
    os.makedirs(path)

df_stream2 = (
    spark.readStream
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .schema(schema_flux)
    .csv(watch_dir)
)

query1 = (
    df_stream2
    .groupBy("dep")
    .agg(
        F.count("*").alias("nb_accidents"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues")
    )
    .orderBy(F.col("nb_accidents").desc())
    .writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .option("checkpointLocation", CHECKPOINT1)
    .queryName("accidents_par_dep")
    .trigger(processingTime="5 seconds")
    .start()
)

query2 = (
    df_stream2
    .groupBy("atm")
    .agg(
        F.count("*").alias("nb_usagers"),
        F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues"),
        F.round(
            F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)) /
            F.when(F.count("*") > 0, F.count("*")).otherwise(None) * 100, 2
        ).alias("pct_tues")
    )
    .orderBy(F.col("pct_tues").desc())
    .writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .option("checkpointLocation", CHECKPOINT2)
    .queryName("tues_par_atm")
    .trigger(processingTime="5 seconds")
    .start()
)

# Injection des fichiers 1 par 1
for i, fichier in enumerate(fichiers):
    src = f"{STREAM_INPUT}/source/{fichier}"
    dst = f"{watch_dir}/{fichier}"
    shutil.copy(src, dst)
    print(f"  Batch {i+1:02d}/{len(fichiers)} injecté : {fichier}")
    time.sleep(6)   # 6s > trigger 5s pour que chaque fichier soit traité

# Attendre le dernier batch
time.sleep(8)

# ─────────────────────────────────────────────────────────────
# FIN
# ─────────────────────────────────────────────────────────────
print("\n✅ Streaming terminé — tous les batches ont été traités.")
print("   Vérifie la Spark UI → 'Structured Streaming' pour les stats.")
input("   Appuie sur Entrée pour arrêter...\n")

query1.stop()
query2.stop()
spark.stop()
