# ============================================================
# pipeline.py  -  Orchestrateur principal
# Projet Jour 4 - Pipeline Spark ONISR (Accidents corporels)
# Équipe : [Sandrine YAO; Fride Audrey MOBOU; Destiné BEHANZIN]
# ============================================================
# Architecture : bronze -> silver (Parquet) -> gold (analyses)
#
# Fichiers du pipeline :
#   01_ingestion.py      -> lecture CSV bruts
#   02_nettoyage.py      -> nettoyage + écriture silver
#   03_analyses.py       -> 3 analyses silver -> gold
#   04_optimisation.py   -> broadcast join mesuré
#   05_exploration_aqe.py -> benchmark AQE x partitions
# ============================================================

from pyspark.sql import SparkSession
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from ingestion       import load_bronze
from nettoyage       import nettoyer_et_ecrire_silver
from analyses        import lire_silver, analyse1_gravite_par_atm, \
                            analyse2_accidents_par_type_route, \
                            analyse3_classement_departements
from optimisation    import mesurer_broadcast_join
from exploration_aqe import benchmark_aqe

# ─────────────────────────────────────────────────────────────
# CHEMINS
# ─────────────────────────────────────────────────────────────
BRONZE = "data/bronze"
SILVER = "data/silver"
GOLD   = "data/gold"

# ─────────────────────────────────────────────────────────────
# SESSION SPARK
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ONISR_Pipeline_Jour4")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✅ Session Spark démarrée — Spark UI : http://localhost:4040")

# ─────────────────────────────────────────────────────────────
# EXÉCUTION DU PIPELINE
# ─────────────────────────────────────────────────────────────

# Étape 1 — Ingestion bronze
df_carac, df_lieux, df_veh, df_usag = load_bronze(spark, BRONZE)

# Étape 2+3 — Nettoyage + écriture silver
nettoyer_et_ecrire_silver(df_carac, df_lieux, df_veh, df_usag, SILVER)

# Étape 4 — Relecture silver
silver_carac, silver_usag, silver_lieux, silver_veh = lire_silver(spark, SILVER)

# Étape 5 — 3 analyses
analyse1_gravite_par_atm(silver_carac, silver_usag, GOLD)
analyse2_accidents_par_type_route(silver_carac, silver_usag, silver_lieux, GOLD)
analyse3_classement_departements(silver_carac, silver_usag, GOLD)

# Étape 6 — Optimisation broadcast
mesurer_broadcast_join(silver_carac, silver_lieux)

# Étape 7 — Exploration AQE
benchmark_aqe(spark, silver_carac, silver_usag)

# ─────────────────────────────────────────────────────────────
print("\n✅ Pipeline terminé.")
print(f"   Résultats dans : {GOLD}")
print("   Spark UI : http://localhost:4040")
input("   Appuie sur Entrée pour arrêter la session Spark...\n")

spark.stop()
