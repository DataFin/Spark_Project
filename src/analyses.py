# ============================================================
# 03_analyses.py  -  3 analyses silver -> gold
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def lire_silver(spark, silver_path):
    """Relit la couche silver depuis Parquet."""
    print("\n── Étape 4 : Relecture silver ──────────────────────────────")
    silver_carac = spark.read.parquet(f"{silver_path}/caracteristiques")
    silver_usag  = spark.read.parquet(f"{silver_path}/usagers")
    silver_lieux = spark.read.parquet(f"{silver_path}/lieux")
    silver_veh   = spark.read.parquet(f"{silver_path}/vehicules")
    print("  ✅ Silver rechargée depuis Parquet")
    return silver_carac, silver_usag, silver_lieux, silver_veh


def analyse1_gravite_par_atm(silver_carac, silver_usag, gold_path):
    """
    Analyse 1 — Agrégation
    Question : Les conditions météo influencent-elles la gravité des accidents ?
    """
    print("\n── Analyse 1 : Gravité par condition atmosphérique ────────")

    df = (
        silver_carac.select("Num_Acc", "atm")
        .join(silver_usag.select("Num_Acc", "grav"), on="Num_Acc", how="inner")
        .groupBy("atm")
        .agg(
            F.count("*").alias("nb_usagers"),
            F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues"),
            F.round(
                F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)) /
                F.when(F.count("*") > 0, F.count("*")).otherwise(None) * 100,
                2
            ).alias("pct_tues")
        )
        .orderBy(F.col("pct_tues").desc())
    )

    df.show(10)
    df.write.mode("overwrite").parquet(f"{gold_path}/analyse1_gravite_par_atm")
    print("  ✅ Analyse 1 écrite.")
    return df


def analyse2_accidents_par_type_route(silver_carac, silver_usag, silver_lieux, gold_path):
    """
    Analyse 2 — Jointure
    Question : Quel type de route concentre le plus de tués ?
    """
    print("\n── Analyse 2 : Profil accidents par type de route ─────────")

    # Broadcast de lieux (1 ligne par accident = petit DataFrame)
    df_lieux_b = F.broadcast(silver_lieux.select("Num_Acc", "catr", "surf", "vma"))

    df = (
        silver_carac.select("Num_Acc", "atm", "col", "dep")
        .join(df_lieux_b, on="Num_Acc", how="inner")
        .join(silver_usag.select("Num_Acc", "grav"), on="Num_Acc", how="inner")
        .groupBy("catr")
        .agg(
            F.count("*").alias("nb_usagers"),
            F.round(F.avg("vma"), 1).alias("vitesse_max_moy"),
            F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues"),
            F.round(
                F.avg(F.when(F.col("surf") == 2, 1).otherwise(0)) * 100, 2
            ).alias("pct_route_mouillee")
        )
        .orderBy(F.col("nb_tues").desc())
    )

    df.show()
    df.write.mode("overwrite").parquet(f"{gold_path}/analyse2_accidents_par_type_route")
    print("  ✅ Analyse 2 écrite.")
    return df


def analyse3_classement_departements(silver_carac, silver_usag, gold_path):
    """
    Analyse 3 — Window function
    Question : Quels départements concentrent le plus de tués ?
    """
    print("\n── Analyse 3 : Classement des départements ─────────────────")

    df_dep = (
        silver_carac.select("Num_Acc", "dep")
        .join(silver_usag.select("Num_Acc", "grav"), on="Num_Acc", how="inner")
        .groupBy("dep")
        .agg(
            F.count("*").alias("nb_usagers"),
            F.sum(F.when(F.col("grav") == 2, 1).otherwise(0)).alias("nb_tues"),
            F.sum(F.when(F.col("grav") == 3, 1).otherwise(0)).alias("nb_blesses_hosp"),
        )
    )

    w_tues = Window.orderBy(F.col("nb_tues").desc())

    df = (
        df_dep
        .withColumn("rang_tues", F.rank().over(w_tues))
        .withColumn("pct_cumul_tues", F.round(
            F.sum("nb_tues").over(w_tues.rowsBetween(Window.unboundedPreceding, Window.currentRow))
            / F.sum("nb_tues").over(Window.partitionBy()) * 100,
            2
        ))
        .orderBy("rang_tues")
    )

    df.show(20)
    df.write.mode("overwrite").parquet(f"{gold_path}/analyse3_classement_departements")
    print("  ✅ Analyse 3 écrite.")
    return df
