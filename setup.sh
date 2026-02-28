#!/bin/bash
# ═══════════════════════════════════════════════════════════════
#  LogiStream – Script de préparation (à lancer UNE SEULE FOIS)
#  avant docker-compose up
# ═══════════════════════════════════════════════════════════════

set -e
echo "══════════════════════════════════════════════"
echo "   LogiStream Control Tower – Setup"
echo "══════════════════════════════════════════════"

# Vérifier Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 non trouvé. Installez Python 3.10+"
    exit 1
fi

# Créer le dossier data et ml
mkdir -p data ml

# Installer les dépendances Python localement
echo ""
echo "📦 Installation des dépendances..."
pip install pandas numpy scikit-learn xgboost --quiet

# Générer les données synthétiques
echo ""
echo "🗃️  Génération des données synthétiques..."
python3 scripts/generate_data.py

# Entraîner le modèle ML
echo ""
echo "🤖 Entraînement du modèle ML..."
python3 scripts/train_model.py

# Copier le modèle pour les services
echo ""
echo "📋 Préparation de la configuration..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "⚠️  Fichier .env créé depuis .env.example"
    echo "    → Éditez .env et ajoutez votre GROQ_API_KEY (gratuit sur https://console.groq.com)"
fi

echo ""
echo "══════════════════════════════════════════════"
echo "✅ Setup terminé !"
echo ""
echo "Prochaine étape :"
echo "  docker-compose up --build"
echo ""
echo "Puis ouvrez :"
echo "  🌐 Dashboard :  http://localhost:8501"
echo "  🔧 Agent API :  http://localhost:8000/docs"
echo "  📨 Kafka UI  :  http://localhost:8080"
echo "══════════════════════════════════════════════"
