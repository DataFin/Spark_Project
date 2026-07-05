# ============================================================
# 05_exploration_aqe.py  -  Exploration AQE x shuffle.partitions
# ============================================================
# Piste choisie : AQE et nombre de partitions de shuffle
# Question : L'AQE améliore-t-il les performances sur notre volume ?
#            Quel nombre de partitions est optimal ?
# ============================================================

import time
from pyspark.sql import functions as F


def benchmark_aqe(spark, silver_carac, silver_usag):
    """
    Protocole :
    - Requête fixe : join usagers x caracteristiques + groupBy(dep, atm)
    - Variable 1 : AQE True/False
    - Variable 2 : shuffle.partitions in {4, 8, 16, 32}
    - Mesure : temps d'exécution en secondes
    """
    print("\n── Étape 6 : Exploration — AQE et shuffle.partitions ──────")

    resultats = []

    for aqe_enabled in [False, True]:
        for nb_parts in [4, 8, 16, 32]:
            spark.conf.set("spark.sql.adaptive.enabled",   str(aqe_enabled).lower())
            spark.conf.set("spark.sql.shuffle.partitions", str(nb_parts))

            t0 = time.time()
            (silver_usag
             .join(silver_carac.select("Num_Acc", "dep", "atm"), on="Num_Acc")
             .groupBy("dep", "atm")
             .agg(F.count("*").alias("nb"), F.avg("grav").alias("grav_moy"))
             .count())
            elapsed = round(time.time() - t0, 2)

            resultats.append((aqe_enabled, nb_parts, elapsed))
            print(f"  AQE={str(aqe_enabled):<5}  partitions={nb_parts:>2}  -> {elapsed}s")

    # Remettre les paramètres par défaut
    spark.conf.set("spark.sql.adaptive.enabled",   "true")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    # Tableau récapitulatif
    print("\n  Tableau récapitulatif AQE :")
    print(f"  {'AQE':<6} {'Partitions':>10} {'Temps (s)':>10}")
    print("  " + "-" * 30)
    for aqe, parts, t in resultats:
        print(f"  {str(aqe):<6} {parts:>10} {t:>10}")

    # Meilleur résultat
    meilleur = min(resultats, key=lambda x: x[2])
    print(f"\n  Meilleur : AQE={meilleur[0]}, partitions={meilleur[1]} -> {meilleur[2]}s")

    return resultats
