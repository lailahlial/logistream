"""
Agent IA Control Tower – FastAPI + Tool Use (Function Calling)

Endpoints :
  POST /chat     – Question en langage naturel → réponse métier
  GET  /health   – Health check
"""

import os, json, pickle, logging, time, re
from typing import Any
from decimal import Decimal
from datetime import datetime, date
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import numpy as np
from groq import Groq
from openai import OpenAI


def json_safe(obj):
    """Convert Decimal, datetime, date to JSON-safe types."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [AGENT-IA] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logistream:logistream_secret@postgres:5432/logistream")
ML_MODEL_PATH = os.getenv("ML_MODEL_PATH", "/app/ml/delay_model.pkl")
LLM_PROVIDER  = os.getenv("LLM_PROVIDER", "groq")   # groq | openai | mistral | mock
GROQ_API_KEY  = os.getenv("GROQ_API_KEY", "")
OPENAI_API_KEY= os.getenv("OPENAI_API_KEY", "")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:latest")

# ── FastAPI ──────────────────────────────────────────────────────
app = FastAPI(title="LogiStream Agent IA", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── DB ───────────────────────────────────────────────────────────
def get_db():
    for i in range(10):
        try:
            return psycopg2.connect(DATABASE_URL)
        except Exception as e:
            log.warning(f"PG indisponible ({i+1}/10) : {e}")
            time.sleep(3)
    raise RuntimeError("Impossible de se connecter à PostgreSQL")

# ── Chargement modèle ML ─────────────────────────────────────────
ml_artifacts = None

def load_model():
    global ml_artifacts
    if os.path.exists(ML_MODEL_PATH):
        with open(ML_MODEL_PATH, "rb") as f:
            ml_artifacts = pickle.load(f)
        log.info(f"✅ Modèle ML chargé depuis {ML_MODEL_PATH}")
    else:
        log.warning(f"⚠️ Modèle ML non trouvé ({ML_MODEL_PATH}). PredictDelay utilisera des heuristiques.")

# ────────────────────────────────────────────────────────────────
# TOOLS
# ────────────────────────────────────────────────────────────────

TOOLS_SCHEMA = [
    {
        "name": "QueryStatus",
        "description": (
            "Interroge le store d'état PostgreSQL pour obtenir des informations "
            "sur les colis, les hubs, les routes et les alertes. "
            "Utilisez cet outil pour répondre à des questions sur l'état actuel des livraisons."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query_type": {
                    "type": "string",
                    "enum": [
                        "delayed_parcels",
                        "at_risk_parcels",
                        "hub_stats",
                        "route_stats",
                        "recent_alerts",
                        "client_parcels",
                        "kpi_summary",
                        "parcels_by_status",
                    ],
                    "description": "Type de requête à exécuter"
                },
                "filters": {
                    "type": "object",
                    "description": "Filtres optionnels (hub_id, client_id, severity, limit)",
                    "properties": {
                        "hub_id":    {"type": "string"},
                        "client_id": {"type": "string"},
                        "severity":  {"type": "string"},
                        "limit":     {"type": "integer", "default": 20},
                        "status":    {"type": "string"},
                    }
                }
            },
            "required": ["query_type"]
        }
    },
    {
        "name": "PredictDelay",
        "description": (
            "Prédit la probabilité et le niveau de risque de retard pour un colis ou une route. "
            "Utilisez cet outil pour des questions prospectives : 'Quels colis risquent d'être en retard ?'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "route_id":          {"type": "string", "description": "Identifiant de la route"},
                "weather_condition": {"type": "string", "enum": ["clear","rain","fog","snow","storm"]},
                "congestion_level":  {"type": "number", "description": "Niveau de congestion (0-1)"},
                "hub_id":            {"type": "string"},
                "distance_km":       {"type": "number"},
                "weight_kg":         {"type": "number"},
                "hour_of_day":       {"type": "integer", "description": "Heure de départ (0-23)"},
            },
            "required": ["route_id"]
        }
    }
]

# ── Implémentation QueryStatus ───────────────────────────────────
QUERIES = {
    "delayed_parcels": """
        SELECT p.parcel_id, p.client_id, p.hub_id, p.route_id,
               p.status, p.delay_minutes, p.risk_level,
               p.planned_delivery, p.actual_delivery
        FROM parcel_state p
        WHERE p.delay_minutes > 15
          AND ({hub_filter}) AND ({client_filter})
        ORDER BY p.delay_minutes DESC
        LIMIT {limit}
    """,
    "at_risk_parcels": """
        SELECT p.parcel_id, p.client_id, p.hub_id, p.route_id,
               p.status, p.risk_level, p.risk_probability, p.delay_minutes
        FROM parcel_state p
        WHERE p.risk_level IN ('medium','high')
          AND p.status NOT IN ('delivered')
          AND ({hub_filter}) AND ({client_filter})
        ORDER BY p.risk_probability DESC, p.delay_minutes DESC
        LIMIT {limit}
    """,
    "hub_stats": """
        SELECT hub_id,
               COUNT(*)                                           AS total_parcels,
               COUNT(*) FILTER (WHERE status = 'delivered')       AS delivered,
               COUNT(*) FILTER (WHERE delay_minutes > 15)         AS delayed,
               COUNT(*) FILTER (WHERE risk_level = 'high')        AS high_risk,
               ROUND(AVG(delay_minutes)::numeric, 1)              AS avg_delay_min,
               ROUND(
                 100.0 * COUNT(*) FILTER (WHERE delay_minutes <= 15)
                 / NULLIF(COUNT(*),0), 1
               )                                                  AS on_time_pct
        FROM parcel_state
        GROUP BY hub_id
        ORDER BY high_risk DESC, avg_delay_min DESC
        LIMIT {limit}
    """,
    "route_stats": """
        SELECT route_id,
               COUNT(*)                                              AS total,
               ROUND(AVG(delay_minutes)::numeric,1)                 AS avg_delay_min,
               COUNT(*) FILTER (WHERE risk_level = 'high')          AS high_risk_count,
               ROUND(
                 100.0 * COUNT(*) FILTER (WHERE delay_minutes <= 15)
                 / NULLIF(COUNT(*),0), 1
               )                                                     AS on_time_pct
        FROM parcel_state
        GROUP BY route_id
        ORDER BY avg_delay_min DESC
        LIMIT {limit}
    """,
    "recent_alerts": """
        SELECT a.alert_id, a.parcel_id, a.alert_type,
               a.severity, a.message, a.created_at, a.acknowledged,
               p.hub_id, p.client_id, p.route_id
        FROM alerts a
        LEFT JOIN parcel_state p ON p.parcel_id = a.parcel_id
        WHERE a.acknowledged = FALSE
          AND ({severity_filter})
        ORDER BY a.created_at DESC
        LIMIT {limit}
    """,
    "client_parcels": """
        SELECT parcel_id, hub_id, route_id, status,
               risk_level, delay_minutes, planned_delivery
        FROM parcel_state
        WHERE ({client_filter})
        ORDER BY delay_minutes DESC
        LIMIT {limit}
    """,
    "kpi_summary": """
        SELECT
            COUNT(*)                                                     AS total_parcels,
            COUNT(*) FILTER (WHERE status = 'delivered')                 AS delivered,
            COUNT(*) FILTER (WHERE status NOT IN ('delivered','planned')) AS in_transit,
            COUNT(*) FILTER (WHERE delay_minutes > 15)                   AS delayed,
            COUNT(*) FILTER (WHERE risk_level = 'high')                  AS high_risk,
            ROUND(AVG(delay_minutes)::numeric,1)                         AS avg_delay_min,
            ROUND(
              100.0 * COUNT(*) FILTER (WHERE delay_minutes <= 15)
              / NULLIF(COUNT(*),0), 1
            )                                                             AS on_time_pct
        FROM parcel_state
    """,
    "parcels_by_status": """
        SELECT status, risk_level, COUNT(*) AS count
        FROM parcel_state
        GROUP BY status, risk_level
        ORDER BY count DESC
        LIMIT {limit}
    """,
}

def execute_query_status(query_type: str, filters: dict) -> dict:
    limit     = int(filters.get("limit", 20))
    hub_id    = filters.get("hub_id")
    client_id = filters.get("client_id")
    severity  = filters.get("severity")

    hub_filter      = f"hub_id = '{hub_id}'"      if hub_id    else "1=1"
    client_filter   = f"client_id = '{client_id}'"if client_id else "1=1"
    severity_filter = f"severity = '{severity}'"   if severity  else "1=1"

    sql_template = QUERIES.get(query_type)
    if not sql_template:
        return {"error": f"query_type inconnu : {query_type}"}

    sql = sql_template.format(
        hub_filter=hub_filter,
        client_filter=client_filter,
        severity_filter=severity_filter,
        limit=limit,
    )

    try:
        conn = get_db()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        conn.close()
        # Sanitize Decimal/datetime values for JSON serialization
        clean_rows = []
        for r in rows:
            clean = {}
            for k, v in dict(r).items():
                if isinstance(v, Decimal):
                    clean[k] = float(v)
                elif isinstance(v, (datetime, date)):
                    clean[k] = v.isoformat()
                else:
                    clean[k] = v
            clean_rows.append(clean)
        return {"query_type": query_type, "count": len(clean_rows), "data": clean_rows}
    except Exception as e:
        log.error(f"QueryStatus error : {e}")
        return {"error": str(e), "query_type": query_type}

# ── Implémentation PredictDelay ──────────────────────────────────
WEATHER_ENC = {"clear": 0, "rain": 1, "fog": 2, "snow": 3, "storm": 4}
ROUTE_RISK  = {
    "R_PAR_MAR": 0.6,  "R_PAR_LYO": 0.4,  "R_PAR_BOR": 0.45,
    "R_PAR_LIL": 0.3,  "R_LYO_MAR": 0.5,  "R_PAR_NAN": 0.35,
    "R_PAR_STR": 0.4,
}
ROUTE_AVG_DURATION = {
    "R_PAR_LYO": 4.5,  "R_PAR_MAR": 7.5, "R_PAR_BOR": 6.0,
    "R_PAR_LIL": 2.5,  "R_LYO_MAR": 3.5, "R_PAR_NAN": 4.0,
    "R_PAR_STR": 5.0,
}
ROUTE_DISTANCE = {
    "R_PAR_LYO": 465,  "R_PAR_MAR": 775, "R_PAR_BOR": 585,
    "R_PAR_LIL": 225,  "R_LYO_MAR": 315, "R_PAR_NAN": 385,
    "R_PAR_STR": 490,
}

def execute_predict_delay(params: dict) -> dict:
    route_id    = params.get("route_id", "R_PAR_LYO")
    weather     = params.get("weather_condition", "clear")
    congestion  = float(params.get("congestion_level", 0.3))
    distance    = float(params.get("distance_km", ROUTE_DISTANCE.get(route_id, 400)))
    weight      = float(params.get("weight_kg", 5))
    hour        = int(params.get("hour_of_day", datetime.now().hour))

    if ml_artifacts and "model" in ml_artifacts:
        try:
            model    = ml_artifacts["model"]
            encoders = ml_artifacts.get("encoders", {})

            route_enc   = list(encoders["route"].classes_).index(route_id)   if route_id in encoders["route"].classes_ else 0
            weather_enc = list(encoders["weather"].classes_).index(weather)  if weather in encoders["weather"].classes_ else 0

            # Use proper day_of_week and is_weekend from current datetime
            now = datetime.now()
            day_of_week = now.weekday()          # 0=Monday .. 6=Sunday
            is_weekend  = int(day_of_week >= 5)  # Saturday=5, Sunday=6

            # Use actual route avg duration instead of rough estimate
            planned_duration_h = ROUTE_AVG_DURATION.get(route_id, distance / 80.0)

            # Derived features (must match train_model.py feature order)
            weather_congestion = weather_enc * congestion
            distance_per_hour  = distance / max(planned_duration_h, 0.1)
            high_congestion    = int(congestion > 0.7)
            severe_weather     = int(weather in ("snow", "storm"))
            heavy_parcel       = int(weight > 30)
            # One-hot weather features
            weather_rain  = int(weather == "rain")
            weather_fog   = int(weather == "fog")
            weather_snow  = int(weather == "snow")
            weather_storm = int(weather == "storm")
            congestion_sq = congestion ** 2
            weather_risk_map = {"clear": 0.0, "rain": 0.3, "fog": 0.2, "snow": 0.8, "storm": 1.5}
            weather_risk_score = weather_risk_map.get(weather, 0.0)
            composite_risk = weather_risk_score * (1 + congestion)

        
            features = np.array([[
                route_enc, weather_enc, 0, 0,
                congestion, distance, weight,
                hour, day_of_week, is_weekend, planned_duration_h,
                weather_congestion, distance_per_hour, high_congestion,
                severe_weather, heavy_parcel,
                weather_rain, weather_fog, weather_snow, weather_storm,
                congestion_sq, weather_risk_score, composite_risk,
            ]])
            proba = model.predict_proba(features)[0]
            pred  = int(np.argmax(proba))
            risk_map = {0: "low", 1: "medium", 2: "high"}
            return {
                "route_id":         route_id,
                "risk_level":       risk_map[pred],
                "probability_low":  round(float(proba[0]), 3),
                "probability_medium":round(float(proba[1]),3),
                "probability_high": round(float(proba[2]), 3),
                "source":           "ml_model",
            }
        except Exception as e:
            log.warning(f"ML predict échoué, fallback heuristique : {e}")

    # ── Fallback heuristique ──
    base_risk   = ROUTE_RISK.get(route_id, 0.4)
    weather_factor = {"clear": 1.0, "rain": 1.3, "fog": 1.2, "snow": 1.8, "storm": 2.5}.get(weather, 1.0)
    risk_score  = min(1.0, base_risk * weather_factor * (1 + congestion * 0.5))

    if risk_score < 0.35:   risk_level = "low"
    elif risk_score < 0.65: risk_level = "medium"
    else:                   risk_level = "high"

    return {
        "route_id":          route_id,
        "risk_level":        risk_level,
        "risk_score":        round(risk_score, 3),
        "weather_condition": weather,
        "congestion_level":  congestion,
        "source":            "heuristic",
    }

# ────────────────────────────────────────────────────────────────
# LLM PROVIDERS
# ────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Tu es l'Agent IA de la Tour de Contrôle LogiStream.
Tu aides les opérateurs logistiques à surveiller les livraisons et à prévenir les retards.

Pour répondre, tu dois TOUJOURS utiliser tes outils disponibles :
- QueryStatus : pour interroger l'état actuel des colis/hubs/alertes
- PredictDelay : pour prédire les risques futurs

Règles :
1. Appelle TOUJOURS au moins un outil avant de répondre.
2. Synthétise les résultats en langage métier clair, en français.
3. Propose des actions correctives concrètes (reroutage, notification client, renfort capacité).
4. Format : commence par un résumé, puis les données clés, puis les recommandations.

/no_think"""

def call_llm_ollama(messages: list, tools: list) -> dict:
    """Call local Ollama via OpenAI-compatible API."""
    client = OpenAI(
        base_url=f"{OLLAMA_BASE_URL}/v1",
        api_key="ollama",  # Ollama doesn't need a real key
    )

    tools_openai = [{"type": "function", "function": {
        "name": t["name"],
        "description": t["description"],
        "parameters": t["input_schema"],
    }} for t in tools]

    completion = client.chat.completions.create(
        model=OLLAMA_MODEL,
        messages=messages,
        tools=tools_openai,
        tool_choice="auto",
        max_tokens=4096,
        temperature=0.7,
    )
    # Convert OpenAI SDK response to dict format
    msg = completion.choices[0].message
    # Strip qwen3 <think>...</think> blocks from content
    content = msg.content or ""
    content = re.sub(r"<think>[\s\S]*?</think>", "", content).strip()
    result = {"choices": [{"message": {
        "role": msg.role,
        "content": content if content else None,
    }}]}
    if msg.tool_calls:
        result["choices"][0]["message"]["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {
                    "name": tc.function.name,
                    "arguments": tc.function.arguments,
                }
            }
            for tc in msg.tool_calls
        ]
    return result

def mock_llm_response(question: str, tool_results: list) -> str:
    """Fallback si aucune API configurée."""
    results_str = "\n".join([json.dumps(r, ensure_ascii=False, indent=2, default=json_safe) for r in tool_results])
    return (
        f"📊 **Analyse LogiStream** (mode demo)\n\n"
        f"Question : {question}\n\n"
        f"Résultats des outils :\n```\n{results_str}\n```\n\n"
        f"⚠️ *Configurez Ollama avec le modèle {OLLAMA_MODEL} pour activer l'analyse IA complète.*"
    )

# ────────────────────────────────────────────────────────────────
# ORCHESTRATION AGENT
# ────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str
    history: list = []

class ChatResponse(BaseModel):
    answer:      str
    tool_calls:  list
    reasoning:   list
    status:      str

async def run_agent(question: str, history: list) -> ChatResponse:
    reasoning  = []
    tool_calls = []

    # ── Étape 1 : Appel LLM initial ──
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history[-6:]:
        messages.append(h)
    messages.append({"role": "user", "content": question})

    reasoning.append(f"🤔 Analyse de la question : « {question} »")

    # Si mode mock → déduction directe + appel tools heuristique
    if LLM_PROVIDER == "mock":
        reasoning.append("⚙️ Mode heuristique (LLM_PROVIDER=mock)")
        reasoning.append("🔧 Appel automatique QueryStatus(kpi_summary)")
        kpi = execute_query_status("kpi_summary", {})
        tool_calls.append({"tool": "QueryStatus", "args": {"query_type": "kpi_summary"}, "result": kpi})

        reasoning.append("🔧 Appel automatique QueryStatus(recent_alerts)")
        alerts = execute_query_status("recent_alerts", {"limit": 5})
        tool_calls.append({"tool": "QueryStatus", "args": {"query_type": "recent_alerts"}, "result": alerts})

        answer = mock_llm_response(question, [kpi, alerts])
        return ChatResponse(answer=answer, tool_calls=tool_calls, reasoning=reasoning, status="mock")

    # ── Appel LLM avec tools ──
    try:
        reasoning.append(f"🧠 Envoi au LLM ({LLM_PROVIDER})...")
        response = call_llm_ollama(messages, TOOLS_SCHEMA)

        msg = response["choices"][0]["message"]

        # ── Traitement des tool calls ──
        while msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                fn_name = tc["function"]["name"]
                fn_args = json.loads(tc["function"]["arguments"])

                reasoning.append(f"🔧 Appel outil : {fn_name}({json.dumps(fn_args, ensure_ascii=False)})")

                if fn_name == "QueryStatus":
                    result = execute_query_status(
                        fn_args.get("query_type", "kpi_summary"),
                        fn_args.get("filters", {})
                    )
                elif fn_name == "PredictDelay":
                    result = execute_predict_delay(fn_args)
                else:
                    result = {"error": f"Outil inconnu : {fn_name}"}

                tool_calls.append({"tool": fn_name, "args": fn_args, "result": result})
                reasoning.append(f"📥 Résultat : {json.dumps(result, ensure_ascii=False, default=json_safe)[:200]}...")

                # Ajouter le résultat au contexte
                messages.append({"role": "assistant", "content": None, "tool_calls": [tc]})
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": json.dumps(result, ensure_ascii=False, default=json_safe),
                })

            # Nouvel appel LLM avec les résultats
            reasoning.append("🧠 Synthèse de la réponse...")
            response = call_llm_ollama(messages, TOOLS_SCHEMA)
            msg = response["choices"][0]["message"]

        answer = msg.get("content") or "Je n'ai pas pu générer de réponse."
        return ChatResponse(answer=answer, tool_calls=tool_calls, reasoning=reasoning, status="ok")

    except Exception as e:
        log.error(f"Erreur LLM : {e}", exc_info=True)
        reasoning.append(f"❌ Erreur LLM : {e}. Utilisation du mode heuristique.")
        kpi = execute_query_status("kpi_summary", {})
        tool_calls.append({"tool": "QueryStatus", "args": {"query_type": "kpi_summary"}, "result": kpi})
        return ChatResponse(
            answer=mock_llm_response(question, [kpi]),
            tool_calls=tool_calls,
            reasoning=reasoning,
            status="error_fallback"
        )

# ────────────────────────────────────────────────────────────────
# ENDPOINTS
# ────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    load_model()
    log.info("🚀 Agent IA démarré")

@app.get("/health")
def health():
    return {"status": "ok", "llm_provider": LLM_PROVIDER,
            "ml_loaded": ml_artifacts is not None}

@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    if not req.question.strip():
        raise HTTPException(400, "La question ne peut pas être vide")
    return await run_agent(req.question, req.history)

@app.get("/kpi")
def get_kpi():
    return execute_query_status("kpi_summary", {})

@app.get("/alerts")
def get_alerts(severity: str = None, limit: int = 50):
    filters = {"limit": limit}
    if severity:
        filters["severity"] = severity
    return execute_query_status("recent_alerts", filters)

@app.get("/hubs")
def get_hubs():
    return execute_query_status("hub_stats", {"limit": 20})
