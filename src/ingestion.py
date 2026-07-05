# ============================================================
# 01_ingestion.py  -  Lecture des CSV bruts (couche bronze)
# ============================================================

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, FloatType, LongType
)

# ── Schémas explicites ───────────────────────────────────────

schema_carac = StructType([
    StructField("Accident_Id", LongType(),    True),
    StructField("jour",        IntegerType(), True),
    StructField("mois",        IntegerType(), True),
    StructField("an",          IntegerType(), True),
    StructField("hrmn",        StringType(),  True),
    StructField("lum",         IntegerType(), True),
    StructField("dep",         StringType(),  True),
    StructField("com",         StringType(),  True),
    StructField("agg",         IntegerType(), True),
    StructField("int",         IntegerType(), True),
    StructField("atm",         IntegerType(), True),
    StructField("col",         IntegerType(), True),
    StructField("adr",         StringType(),  True),
    StructField("lat",         StringType(),  True),
    StructField("long",        StringType(),  True),
])

schema_lieux = StructType([
    StructField("Num_Acc",  LongType(),    True),
    StructField("catr",     IntegerType(), True),
    StructField("voie",     StringType(),  True),
    StructField("v1",       IntegerType(), True),
    StructField("v2",       StringType(),  True),
    StructField("circ",     IntegerType(), True),
    StructField("nbv",      IntegerType(), True),
    StructField("vosp",     IntegerType(), True),
    StructField("prof",     IntegerType(), True),
    StructField("pr",       StringType(),  True),
    StructField("pr1",      StringType(),  True),
    StructField("plan",     IntegerType(), True),
    StructField("lartpc",   IntegerType(), True),
    StructField("larrout",  IntegerType(), True),
    StructField("surf",     IntegerType(), True),
    StructField("infra",    IntegerType(), True),
    StructField("situ",     IntegerType(), True),
    StructField("vma",      IntegerType(), True),
])

schema_vehicules = StructType([
    StructField("Num_Acc",      LongType(),    True),
    StructField("id_vehicule",  StringType(),  True),
    StructField("num_veh",      StringType(),  True),
    StructField("senc",         IntegerType(), True),
    StructField("catv",         IntegerType(), True),
    StructField("obs",          IntegerType(), True),
    StructField("obsm",         IntegerType(), True),
    StructField("choc",         IntegerType(), True),
    StructField("manv",         IntegerType(), True),
    StructField("motor",        IntegerType(), True),
    StructField("occutc",       IntegerType(), True),
])

schema_usagers = StructType([
    StructField("Num_Acc",      LongType(),    True),
    StructField("id_usager",    StringType(),  True),
    StructField("id_vehicule",  StringType(),  True),
    StructField("num_veh",      StringType(),  True),
    StructField("place",        IntegerType(), True),
    StructField("catu",         IntegerType(), True),
    StructField("grav",         IntegerType(), True),
    StructField("sexe",         IntegerType(), True),
    StructField("an_nais",      IntegerType(), True),
    StructField("trajet",       IntegerType(), True),
    StructField("secu1",        IntegerType(), True),
    StructField("secu2",        IntegerType(), True),
    StructField("secu3",        IntegerType(), True),
    StructField("locp",         IntegerType(), True),
    StructField("actp",         IntegerType(), True),
    StructField("etatp",        IntegerType(), True),
])


def read_csv(spark, path, schema, sep=";"):
    """Lecture CSV brut avec schéma explicite."""
    return (
        spark.read
        .option("header", "true")
        .option("sep", sep)
        .option("encoding", "iso-8859-1")
        .option("nullValue", "")
        .schema(schema)
        .csv(path)
    )


def load_bronze(spark, bronze_path):
    """Charge les 4 fichiers CSV ONISR depuis la couche bronze."""
    print("\n── Étape 1 : Ingestion bronze ──────────────────────────────")

    df_carac = read_csv(spark, f"{bronze_path}/caracteristiques.csv", schema_carac)
    df_carac = df_carac.withColumnRenamed("Accident_Id", "Num_Acc")

    df_lieux = read_csv(spark, f"{bronze_path}/lieux.csv",      schema_lieux)
    df_veh   = read_csv(spark, f"{bronze_path}/vehicules.csv",  schema_vehicules)
    df_usag  = read_csv(spark, f"{bronze_path}/usagers.csv",    schema_usagers)

    for nom, df in [("caracteristiques", df_carac), ("lieux", df_lieux),
                    ("vehicules", df_veh), ("usagers", df_usag)]:
        print(f"  {nom} : {df.count()} lignes")

    print("   Ingestion bronze OK")
    return df_carac, df_lieux, df_veh, df_usag
