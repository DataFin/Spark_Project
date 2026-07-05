# Projet Spark — Pipeline ONISR (Accidents corporels)
**Formation Apache Spark — Jour 4 | HETIC MD4**
**Équipe : Audrey MOBOU, Sandrine YAO, Destiné BEHANZIN**

---

## Arborescence

```
Spark_Project/
├── data/
│   ├── bronze/          ← CSV bruts ONISR (à déposer ici, non versionné)
│   │   ├── caracteristiques.csv
│   │   ├── lieux.csv
│   │   ├── vehicules.csv
│   │   └── usagers.csv
│   ├── silver/          ← Parquet nettoyé (généré par le pipeline)
│   └── gold/            ← Résultats des analyses (généré par le pipeline)
├── src/
│   ├── ingestion.py     ← Lecture des 4 CSV ONISR avec schémas explicites
│   ├── nettoyage.py     ← Nettoyage bronze → silver (Parquet)
│   ├── analyses.py      ← 3 analyses + analyse temporelle bonus
│   ├── optimisation.py  ← Broadcast join mesuré avant/après
│   ├── exploration_aqe.py ← Benchmark AQE × shuffle.partitions
│   ├── streaming.py  ← Bonus : Structured Streaming
│   ├── pipeline.py      ← Orchestrateur principal
│   └── __init__.py
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

**Prérequis :**
- Python 3.10+
- JDK 11 ou 17 avec `JAVA_HOME` configuré
- winutils.exe dans `C:\hadoop\bin` (Windows uniquement)

```powershell
# Configurer HADOOP_HOME à chaque nouvelle session PowerShell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:Path;C:\hadoop\bin"
```

---

## 2. Téléchargement des données ONISR

Lien officiel : https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2022/

Télécharger l'année **2022** → 4 fichiers CSV :
- `caracteristiques-2022.csv`
- `lieux-2022.csv`
- `vehicules-2022.csv`
- `usagers-2022.csv`

Renommer (enlever `-2022`) et déposer dans `data/bronze/` :

```powershell
Move-Item "$env:USERPROFILE\Downloads\caracteristiques-2022.csv" "data\bronze\caracteristiques.csv"
Move-Item "$env:USERPROFILE\Downloads\lieux-2022.csv"            "data\bronze\lieux.csv"
Move-Item "$env:USERPROFILE\Downloads\vehicules-2022.csv"        "data\bronze\vehicules.csv"
Move-Item "$env:USERPROFILE\Downloads\usagers-2022.csv"          "data\bronze\usagers.csv"
```

---

## 3. Lancer le pipeline

```powershell
# Activer l'environnement
.venv\Scripts\Activate.ps1

# Lancer le pipeline complet
python src\pipeline.py

# Lancer le bonus streaming (après pipeline.py)
python src\06_streaming.py
```

** Ordre obligatoire :** lancer `pipeline.py` avant `streaming.py` — le streaming utilise la couche silver générée par le pipeline.

Le pipeline exécute dans l'ordre :
1. **Bronze → Silver** : lecture CSV avec schéma explicite, nettoyage, écriture Parquet
2. **Silver → Gold** : 3 analyses + analyse temporelle bonus
3. **Optimisation** : broadcast join mesuré avant/après
4. **Exploration** : benchmark AQE × shuffle.partitions

La **Spark UI** est accessible sur `http://localhost:4040` pendant toute la durée du script.

---

## 4. Ce qui est couvert (grille sur 20)

| Critère | Fichier | Description |
|---|---|---|
| Ingestion + schéma explicite | `src/ingestion.py` | StructType sur 4 fichiers, encodage iso-8859-1 |
| Nettoyage + Parquet silver | `src/nettoyage.py` | Doublons, nulls, lat/long, partitionné par dep |
| Analyse 1 — agrégation | `src/analyses.py` | Gravité par condition atmosphérique |
| Analyse 2 — jointure | `src/analyses.py` | Profil accidents par type de route (broadcast) |
| Analyse 3 — window function | `src/analyses.py` | Classement des départements avec % cumulé |
| Analyse bonus — temporelle | `src/analyses.py` | Accidents par heure, jour et croisement heure×jour |
| Optimisation mesurée | `src/optimisation.py` | Broadcast join : 0.97s → 0.91s (gain 6.2%) |
| Exploration AQE | `src/exploration_aqe.py` | 8 combinaisons AQE × partitions mesurées |
| Lecture Spark UI | `rapport-modele.md` | DAG, stages, shuffles commentés + captures |
| Bonus streaming | `src/streaming.py` | 2 agrégats continus sur flux simulé |

---

## 5. Résultats principaux

- **55 302 accidents** traités, **126 662 usagers**
- Le **brouillard** est la condition météo la plus mortelle (5.92% de tués)
- Les **routes départementales** concentrent 56% des tués
- Les **Bouches-du-Rhône** arrivent en tête (117 tués)
- **17h** est l'heure la plus accidentogène, **2h** la plus mortelle
- Le **dimanche** est le jour le plus meurtrier (3.76% de tués)
- Gain broadcast join : **6.2%** | Meilleur AQE : **True, 8 partitions (0.78s)**
