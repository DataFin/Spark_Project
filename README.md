# Projet Spark - Pipeline ONISR (Accidents corporels)
**Formation Apache Spark — Jour 4**

---

## Arborescence

```
Spark_Project/
├── data/
│   ├── bronze/          ← CSV bruts ONISR (à déposer ici)
│   │   ├── caracteristiques.csv
│   │   ├── lieux.csv
│   │   ├── vehicules.csv
│   │   └── usagers.csv
│   ├── silver/          ← Parquet nettoyé (généré)
│   └── gold/            ← Résultats des analyses (généré)
├── src/
│   └── pipeline.py      ← Pipeline complet (ETL + analyses + exploration)
├── notebooks/           ← Exploration interactive (optionnel)
├── outputs/
│   └── spark_ui_captures/  ← Copies d'écran Spark UI
├── requirements.txt
├── setup_env.ps1
└── README.md
```

---

## 1. Mise en place de l'environnement

```powershell
# Si erreur d'exécution de script PS
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Initialisation complète (virtualenv + packages + dossiers)
.\setup_env.ps1
```

Prérequis : **Python 3.10+** et **JDK 11 ou 17** avec `JAVA_HOME` configuré.

---

## 2. Téléchargement des données ONISR

Lien officiel : https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2022/

Télécharger **une seule année** (ex. 2022) → 4 fichiers CSV :
- `caracteristiques-YYYY.csv`
- `lieux-YYYY.csv`
- `vehicules-YYYY.csv`
- `usagers-YYYY.csv`

**Renommer** en `caracteristiques.csv`, `lieux.csv`, `vehicules.csv`, `usagers.csv` et déposer dans `data/bronze/`.

---

## 3. Lancer le pipeline

```powershell
# Activer l'environnement si pas encore fait
.venv\Scripts\Activate.ps1

# Lancer le pipeline complet
python src\pipeline.py
```

Le pipeline :
1. **Bronze → Silver** : lecture CSV avec schéma explicite, nettoyage, écriture Parquet
2. **Silver → Gold** : 3 analyses + optimisation mesurée
3. **Exploration** : benchmark AQE × shuffle.partitions

La **Spark UI** est accessible sur http://localhost:4040 pendant toute la durée du script.

---

## Ce qui est couvert (grille sur 20)

| Critère | Où dans le code |
|---|---|
| Ingestion + schéma explicite | `pipeline.py` lignes 45-100 |
| Nettoyage + Parquet silver | `pipeline.py` lignes 115-165 |
| Analyse 1 — agrégation | `pipeline.py` : `df_analyse1` |
| Analyse 2 — jointure (broadcast) | `pipeline.py` : `df_analyse2` |
| Analyse 3 — window function | `pipeline.py` : `df_analyse3` |
| Optimisation mesurée (broadcast) | `pipeline.py` : Étape 5 |
| Exploration AQE | `pipeline.py` : Étape 6 |
| Lecture Spark UI | captures dans `outputs/spark_ui_captures/` |
