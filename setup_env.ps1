# ============================================================
# setup_env.ps1  -  Initialisation de l'environnement Spark
# Projet ONISR - Pipeline PySpark (Jour 4)
# ============================================================
# Usage : .\setup_env.ps1  (depuis C:\Rendu_Spark_projet\Spark_Project)
# ============================================================

Write-Host "🔧 Vérification de Python..." -ForegroundColor Cyan
python --version
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Python introuvable. Installe Python 3.10+ et relance." -ForegroundColor Red
    exit 1
}

Write-Host "🔧 Vérification de Java (requis pour Spark)..." -ForegroundColor Cyan
java -version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Java introuvable. Installe JDK 11 ou 17 et configure JAVA_HOME." -ForegroundColor Red
    exit 1
}

# ─── Création du virtualenv ──────────────────────────────
if (-Not (Test-Path ".venv")) {
    Write-Host "📦 Création du virtualenv .venv..." -ForegroundColor Cyan
    python -m venv .venv
} else {
    Write-Host "✅ .venv déjà présent." -ForegroundColor Green
}

# ─── Activation ──────────────────────────────────────────
Write-Host "⚡ Activation du virtualenv..." -ForegroundColor Cyan
.venv\Scripts\Activate.ps1

# ─── Installation des dépendances ────────────────────────
Write-Host "📥 Installation des packages (requirements.txt)..." -ForegroundColor Cyan
pip install --upgrade pip --quiet
pip install -r requirements.txt

# ─── Création des dossiers du projet ─────────────────────
Write-Host "📁 Création de l'arborescence du projet..." -ForegroundColor Cyan
$dirs = @(
    "data\bronze",
    "data\silver",
    "data\gold",
    "src",
    "notebooks",
    "outputs\spark_ui_captures"
)
foreach ($dir in $dirs) {
    if (-Not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
        Write-Host "  + $dir" -ForegroundColor Gray
    }
}

# ─── Vérification finale ─────────────────────────────────
Write-Host ""
Write-Host "✅ Environnement prêt !" -ForegroundColor Green
Write-Host ""
Write-Host "─── Prochaines étapes ───────────────────────────────────────" -ForegroundColor Yellow
Write-Host "1. Télécharge les CSV ONISR (lien dans data/sources-open-data.md)"
Write-Host "   → Place les 4 fichiers dans : data\bronze\"
Write-Host "   → caracteristiques.csv | lieux.csv | vehicules.csv | usagers.csv"
Write-Host ""
Write-Host "2. Lance le pipeline :"
Write-Host "   python src\pipeline.py"
Write-Host ""
Write-Host "3. Ou ouvre le notebook :"
Write-Host "   jupyter notebook notebooks\exploration.ipynb"
Write-Host "──────────────────────────────────────────────────────────────" -ForegroundColor Yellow
