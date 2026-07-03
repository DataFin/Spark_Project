# ============================================================
# 04_optimisation.py  -  Broadcast Join mesuré (avant/après)
# ============================================================

import time
from pyspark.sql import functions as F


def mesurer_broadcast_join(silver_carac, silver_lieux):
    """
    Optimisation : Broadcast Join
    Compare SortMergeJoin (défaut) vs BroadcastHashJoin sur la jointure
    caracteristiques x lieux.
    """
    print("\n── Étape 5 : Optimisation — Broadcast Join (avant/après) ──")

    # ── SANS broadcast (SortMergeJoin par défaut) ────────────
    t0 = time.time()
    (silver_carac.select("Num_Acc", "dep")
     .join(silver_lieux.select("Num_Acc", "catr"), on="Num_Acc")
     .groupBy("dep", "catr").count()
     .count())
    t_sans = round(time.time() - t0, 2)
    print(f"  Sans broadcast : {t_sans}s")

    # ── AVEC broadcast (BroadcastHashJoin) ───────────────────
    t0 = time.time()
    (silver_carac.select("Num_Acc", "dep")
     .join(F.broadcast(silver_lieux.select("Num_Acc", "catr")), on="Num_Acc")
     .groupBy("dep", "catr").count()
     .count())
    t_avec = round(time.time() - t0, 2)
    print(f"  Avec broadcast : {t_avec}s")
    print(f"  Gain           : {round((t_sans - t_avec) / t_sans * 100, 1)}%")

    # ── Plan d'exécution (BroadcastHashJoin attendu) ─────────
    print("\n  Plan AVEC broadcast (BroadcastHashJoin attendu) :")
    (silver_carac.select("Num_Acc", "dep")
     .join(F.broadcast(silver_lieux.select("Num_Acc", "catr")), on="Num_Acc")
     .explain())

    return t_sans, t_avec
