# ============================================================
# 02_nettoyage.py  -  Nettoyage bronze -> silver (Parquet)
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, FloatType


def nettoyer_et_ecrire_silver(df_carac, df_lieux, df_veh, df_usag, silver_path):
    """Nettoie les 4 DataFrames et écrit la couche silver en Parquet."""
    print("\n── Étape 2 : Nettoyage (silver) ────────────────────────────")

    # ── Caractéristiques ─────────────────────────────────────
    nb_brut_carac = df_carac.count()

    df_carac_clean = (
        df_carac
        # Extraire l'heure depuis "HH:MM"
        .withColumn("heure", F.split(F.col("hrmn"), ":").getItem(0).cast(IntegerType()))
        # Filtrer les heures invalides
        .filter(F.col("heure").between(0, 23) | F.col("heure").isNull())
        # Supprimer lignes sans identifiant
        .filter(F.col("Num_Acc").isNotNull())
        # Construire une date propre
        .withColumn(
            "date_accident",
            F.to_date(
                F.concat_ws("-",
                    F.col("an").cast(StringType()),
                    F.lpad(F.col("mois").cast(StringType()), 2, "0"),
                    F.lpad(F.col("jour").cast(StringType()), 2, "0")),
                "yyyy-MM-dd"
            )
        )
        # Convertir lat/long (virgule -> point, format français)
        .withColumn("lat",  F.regexp_replace(F.col("lat"),  ",", ".").cast(FloatType()))
        .withColumn("long", F.regexp_replace(F.col("long"), ",", ".").cast(FloatType()))
        .dropDuplicates(["Num_Acc"])
    )

    nb_silver_carac = df_carac_clean.count()
    print(f"  caracteristiques : {nb_brut_carac} -> {nb_silver_carac} "
          f"({nb_brut_carac - nb_silver_carac} lignes écartées, "
          f"{round((nb_brut_carac - nb_silver_carac) / nb_brut_carac * 100, 2)}%)")

    # ── Usagers ──────────────────────────────────────────────
    nb_brut_usag = df_usag.count()

    df_usag_clean = (
        df_usag
        .filter(F.col("Num_Acc").isNotNull())
        .filter(F.col("grav").isin(1, 2, 3, 4))  # 1=indemne 2=tué 3=blessé hosp. 4=blessé léger
        .dropDuplicates()
    )

    nb_silver_usag = df_usag_clean.count()
    print(f"  usagers         : {nb_brut_usag} -> {nb_silver_usag} "
          f"({nb_brut_usag - nb_silver_usag} lignes écartées, "
          f"{round((nb_brut_usag - nb_silver_usag) / nb_brut_usag * 100, 2)}%)")

    # ── Lieux & Véhicules ────────────────────────────────────
    df_lieux_clean = df_lieux.filter(F.col("Num_Acc").isNotNull()).dropDuplicates(["Num_Acc"])
    df_veh_clean   = df_veh.filter(F.col("Num_Acc").isNotNull()).dropDuplicates()
    print("  lieux & vehicules : nettoyage minimal OK")

    # ── Écriture silver (Parquet) ─────────────────────────────
    print("\n── Étape 3 : Écriture couche silver (Parquet) ──────────────")

    # Partitionné par département (faible cardinalité ~100 valeurs)
    (df_carac_clean
     .write.mode("overwrite")
     .partitionBy("dep")
     .parquet(f"{silver_path}/caracteristiques"))

    df_usag_clean.write.mode("overwrite").parquet(f"{silver_path}/usagers")
    df_lieux_clean.write.mode("overwrite").parquet(f"{silver_path}/lieux")
    df_veh_clean.write.mode("overwrite").parquet(f"{silver_path}/vehicules")

    print("  ✅ Couche silver écrite.")
