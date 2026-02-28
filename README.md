# 🚚 LogiStream Control Tower
### SI Logistique Event-Driven & Agent IA

> Tour de contrôle temps réel pour la gestion logistique B2B.  
> Architecture event-driven (Kafka) + Agent IA (LLM + Tool Use) + Dashboard Streamlit.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SOURCES DE DONNÉES                               │
│  [Kaggle CSV]  planned_shipments    [LaDe CSV]  delivery_events     │
└──────────┬──────────────────────────────┬───────────────────────────┘
           │                              │
           ▼                              ▼
┌──────────────────┐            ┌──────────────────────┐
│ Ingesteur Kaggle │            │   Replay LaDe        │
│ (initialise TMS) │            │ (simule temps réel)  │
└────────┬─────────┘            └──────────┬───────────┘
         │                                 │
         ▼                                 ▼
┌═══════════════════════════════════════════════════════╗
║              BUS D'ÉVÉNEMENTS KAFKA                   ║
║  planned_shipments │ delivery_events │ alerts │ errors║
╚═══════════════════════════╤═══════════════════════════╝
                            │
                            ▼
              ┌─────────────────────────┐
              │  Service Enrichissement  │
              │  (corrélation + retards) │
              └───────────┬─────────────┘
                          │
                          ▼
              ┌─────────────────────────┐
              │    PostgreSQL           │
              │  (Store d'état Golden   │
              │   Record par colis)     │
              └────────┬────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
┌──────────────────┐    ┌───────────────────────┐
│   Agent IA       │    │  Dashboard Streamlit   │
│  FastAPI + Tools │    │  (Carte + KPIs + Chat) │
│  QueryStatus     │    └───────────────────────┘
│  PredictDelay    │
└──────────────────┘
```

## 📁 Structure du projet

```
logistream/
├── docker-compose.yml          # Orchestration complète
├── .env.example                # Template configuration
├── requirements.txt            # Dépendances Python globales
├── setup.sh                    # Script de préparation
│
├── data/                       # Données générées (auto-créé)
│   ├── kaggle_planned.csv      # 3000 colis planifiés
│   └── lade_events.csv         # ~12000 événements terrain
│
├── ml/                         # Modèle ML (auto-créé)
│   ├── delay_model.pkl         # Modèle XGBoost entraîné
│   ├── feature_names.json      # Features du modèle
│   └── metrics.json            # Métriques d'évaluation
│
├── scripts/
│   ├── generate_data.py        # Génère les données synthétiques
│   ├── train_model.py          # Entraîne le modèle ML
│   └── init_db.sql             # Schéma PostgreSQL
│
└── services/
    ├── ingestion_kaggle/       # Charge Kaggle → Kafka planned_shipments
    │   ├── main.py
    │   ├── Dockerfile
    │   └── requirements.txt
    ├── replay_lade/            # Rejoue LaDe → Kafka delivery_events
    │   ├── main.py
    │   └── Dockerfile
    ├── enrichissement/         # Corrélation + alertes (stateful)
    │   ├── main.py
    │   └── Dockerfile
    ├── agent_ia/               # FastAPI + Agent LLM + Tools
    │   ├── main.py
    │   ├── Dockerfile
    │   └── requirements.txt
    └── ui/                     # Dashboard Streamlit
        ├── app.py
        ├── Dockerfile
        └── requirements.txt
```

---

## 🚀 Lancer la démo en 3 étapes

### Pré-requis
- Docker Desktop (≥ 4.x) avec **au moins 4 Go RAM alloués**
- Python 3.10+ (pour le setup local)
- Une clé API Groq **gratuite** : [console.groq.com](https://console.groq.com)

---

### Étape 1 – Préparer les données et le modèle

```bash
# Cloner / entrer dans le dossier
cd logistream

# Rendre le script exécutable et le lancer
chmod +x setup.sh
./setup.sh
```

Ce script génère automatiquement :
- `data/kaggle_planned.csv` — 3000 colis planifiés
- `data/lade_events.csv` — ~12 000 événements terrain
- `ml/delay_model.pkl` — Modèle XGBoost entraîné

---

### Étape 2 – Configurer la clé API LLM

```bash
# Éditez le fichier .env (créé par setup.sh)
nano .env

# Ajoutez votre clé Groq (gratuite)
GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxxxxxx
```

> **Sans clé API**, l'agent fonctionne en **mode heuristique** (sans LLM),
> toutes les autres fonctionnalités restent opérationnelles.

---

### Étape 3 – Lancer tout le système

```bash
docker-compose up --build
```

Attendez environ **60-90 secondes** que tous les services démarrent.

---

## 🌐 Accéder aux interfaces

| Interface | URL | Description |
|-----------|-----|-------------|
| 🎛️ **Dashboard principal** | http://localhost:8501 | Streamlit Control Tower |
| 🤖 **API Agent IA** | http://localhost:8000/docs | FastAPI Swagger |
| 📨 **Kafka UI** | http://localhost:8080 | Monitoring des topics |

---

## 📊 Fonctionnalités démontrées

### 🗺️ Page 1 – Carte & Hubs
- Carte interactive des 7 hubs français (Paris, Lyon, Marseille, Bordeaux, Lille, Nantes, Strasbourg)
- Code couleur : 🟢 Vert (on-time) / 🟠 Orange (risque moyen) / 🔴 Rouge (risque élevé)
- Tableau statistiques par hub : on-time %, délai moyen, colis à risque

### 📊 Page 2 – Dashboard & Alertes
- KPIs temps réel : total colis, livrés, en transit, en retard, on-time %
- Graphiques : délai par route, distribution des risques
- Fil d'alertes temps réel (mise à jour automatique)
- Tableau des colis à risque avec filtres

### 💬 Page 3 – Agent IA
- Chat en langage naturel avec l'assistant
- Affichage du raisonnement de l'agent (étape par étape)
- Appels d'outils visibles : `QueryStatus` + `PredictDelay`
- Questions suggérées pour démarrer rapidement

### 📋 Page 4 – Journal événements
- Historique complet des événements LaDe traités
- Timeline par heure et type d'événement
- Dead Letter Queue (erreurs / colis inconnus)

---

## 🤖 Exemples de questions pour l'Agent IA

```
"Quels hubs présentent un risque élevé en ce moment ?"
→ Appelle QueryStatus(hub_stats) → Identifie Hub Lyon et Hub Paris → Recommande des actions

"Quelle est la probabilité de retard sur la route Paris-Marseille avec de la pluie ?"
→ Appelle PredictDelay(route=R_PAR_MAR, weather=rain) → Donne le niveau de risque prédit

"Quels clients B2B sont impactés par des retards aujourd'hui ?"
→ Appelle QueryStatus(delayed_parcels) → Liste les clients avec SLA en danger

"Résume-moi les KPIs de cette semaine"
→ Appelle QueryStatus(kpi_summary) + QueryStatus(hub_stats) → Synthèse complète
```

---

## ⚙️ Variables d'environnement

| Variable | Défaut | Description |
|----------|--------|-------------|
| `LLM_PROVIDER` | `groq` | Provider LLM : `groq`, `openai`, `mock` |
| `GROQ_API_KEY` | *(vide)* | Clé API Groq (gratuit) |
| `OPENAI_API_KEY` | *(vide)* | Clé API OpenAI (alternatif) |
| `TIME_ACCELERATION` | `360` | Facteur d'accélération simulation (360 = 1h→10s) |
| `DELAY_THRESHOLD_MINUTES` | `30` | Seuil de retard pour déclencher une alerte |

---

## 🔧 Commandes utiles

```bash
# Voir les logs d'un service spécifique
docker-compose logs -f enrichissement
docker-compose logs -f replay-lade
docker-compose logs -f agent-ia

# Arrêter et nettoyer
docker-compose down -v   # supprime aussi les volumes (DB)

# Relancer juste le replay (rejouer les événements)
docker-compose restart replay-lade

# Accéder à la base de données
docker exec -it logistream_postgres psql -U logistream -d logistream
```

---

## 📐 Choix d'architecture – Justification

### Pourquoi Event-Driven ?

| Critère | Batch classique | Event-Driven (ce projet) |
|---------|----------------|--------------------------|
| Latence alertes | Heures | < 1 seconde |
| Découplage services | Non | ✅ Oui (Kafka topics) |
| Scalabilité | Limitée | ✅ Horizontale |
| Perturbation du TMS legacy | Possible | ✅ Non (lecture seule) |
| Corrélation temps réel | Impossible | ✅ Service enrichissement |

### Contrat d'interface – DeliveryEvent (JSON canonique)

```json
{
  "event_id":        "EV-FR123456-PIK-1704067200000",
  "parcel_id":       "FR123456",
  "event_type":      "pickup",
  "event_timestamp": "2024-01-08T08:30:00",
  "hub_id":          "HUB_PARIS",
  "latitude":        48.8566,
  "longitude":       2.3522,
  "replayed_at":     "2024-01-08T08:30:00"
}
```

### Gestion des événements désordonnés
- Le service d'enrichissement utilise `COALESCE` pour ne pas écraser les timestamps déjà connus
- Un `delivered` arrivant avant `pickup` est accepté mais le pickup_ts réel reste null
- Les doublons sont gérés par `ON CONFLICT DO UPDATE` (idempotence)

---

## 📋 Livrables

- [x] Architecture technique event-driven (ce README + docker-compose)
- [x] Code source complet (6 services Python)
- [x] Pipeline d'intégration Kafka opérationnel
- [x] Script Replay temps réel (simulation temporelle)
- [x] Service d'enrichissement stateful (corrélation + alertes)
- [x] Modèle ML XGBoost (prédiction risque Low/Medium/High)
- [x] Agent IA avec Tool Use (QueryStatus + PredictDelay)
- [x] Dashboard Streamlit (carte + KPIs + alertes + chat)
- [x] Dead Letter Queue (gestion erreurs / colis inconnus)
- [x] README avec lancement en une commande
