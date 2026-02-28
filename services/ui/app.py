import os, time, json, math
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────
DATABASE_URL  = os.getenv("DATABASE_URL",  "postgresql://logistream:logistream_secret@postgres:5432/logistream")
AGENT_API_URL = os.getenv("AGENT_API_URL", "http://agent-ia:8000")
REFRESH_SEC   = 10

st.set_page_config(
    page_title="LogiStream Control Tower",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-title { font-size: 2rem; font-weight: 700; color: #1a365d; }
    .kpi-card   { background: #f7fafc; border-radius: 10px; padding: 1rem;
                  border-left: 4px solid #3182ce; text-align: center; }
    .kpi-value  { font-size: 2.5rem; font-weight: 700; color: #2b6cb0; }
    .kpi-label  { font-size: 0.9rem; color: #718096; }
    .alert-critical { background: #742a2a; border-left: 4px solid #e53e3e; border-radius: 6px; padding: 8px; margin: 4px 0; color: #fff; }
    .alert-high   { background: #fed7d7; border-left: 4px solid #e53e3e; border-radius: 6px; padding: 8px; margin: 4px 0; color: #742a2a; }
    .alert-medium { background: #fefcbf; border-left: 4px solid #f6ad55; border-radius: 6px; padding: 8px; margin: 4px 0; color: #744210; }
    .alert-low    { background: #c6f6d5; border-left: 4px solid #68d391; border-radius: 6px; padding: 8px; margin: 4px 0; color: #22543d; }
    .chat-user    { background: #ebf8ff; border-radius: 10px; padding: 10px; margin: 5px 0; }
    .chat-agent   { background: #f0fff4; border-radius: 10px; padding: 10px; margin: 5px 0; }
    .reasoning    { font-size: 0.8rem; color: #718096; font-style: italic; }
    .status-ok    { color: #38a169; font-weight: bold; }
    .status-warn  { color: #d69e2e; font-weight: bold; }
    .status-error { color: #e53e3e; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ── DB Helper ────────────────────────────────────────────────────
@st.cache_resource
def get_db_connection():
    for _ in range(10):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            return conn
        except:
            time.sleep(2)
    return None

def _get_active_connection():
    """Get an active DB connection, reconnecting if stale."""
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        if conn.closed:
            get_db_connection.clear()
            return get_db_connection()
        # Test the connection is alive
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        get_db_connection.clear()
        return get_db_connection()

def query_db(sql: str, params=None) -> pd.DataFrame:
    conn = _get_active_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            columns = [desc.name if hasattr(desc, "name") else desc[0] for desc in (cur.description or [])]
            rows = cur.fetchall()
        if not rows:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame([dict(r) for r in rows], columns=columns)
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        # Try to reconnect once
        try:
            get_db_connection.clear()
            conn = get_db_connection()
            if conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql, params)
                    columns = [desc.name if hasattr(desc, "name") else desc[0] for desc in (cur.description or [])]
                    rows = cur.fetchall()
                if not rows:
                    return pd.DataFrame(columns=columns)
                return pd.DataFrame([dict(r) for r in rows], columns=columns)
        except:
            pass
        return pd.DataFrame()

# ── API Agent ────────────────────────────────────────────────────
def call_agent(question: str, history: list) -> dict:
    try:
        r = requests.post(
            f"{AGENT_API_URL}/chat",
            json={"question": question, "history": history},
            timeout=120
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {
            "answer": f"❌ Erreur de connexion à l'Agent IA : {e}",
            "tool_calls": [],
            "reasoning": [f"Erreur : {e}"],
            "status": "error"
        }

# ── Sidebar ──────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/delivery-truck.png", width=60)
    st.markdown("## 🚚 LogiStream")
    st.markdown("**Control Tower v1.0**")
    st.divider()

    page = st.radio("Navigation", [
        "🗺️ Carte & Hubs",
        "📊 Dashboard & Alertes",
        "💬 Agent IA",
        "📋 Journal événements"
    ])
    st.divider()

    # Status système
    try:
        health = requests.get(f"{AGENT_API_URL}/health", timeout=3).json()
        st.markdown('<span class="status-ok">● Agent IA : OK</span>', unsafe_allow_html=True)
        st.caption(f"LLM: {health.get('llm_provider','?')} | ML: {'✅' if health.get('ml_loaded') else '⚠️'}")
    except:
        st.markdown('<span class="status-error">● Agent IA : Hors ligne</span>', unsafe_allow_html=True)

    st.divider()
    auto_refresh = st.toggle("Auto-refresh (10s)", value=False)
    if auto_refresh:
        time.sleep(REFRESH_SEC)
        st.rerun()

# ══════════════════════════════════════════════════════════════
# PAGE 1 : CARTE & HUBS
# ══════════════════════════════════════════════════════════════
if page == "🗺️ Carte & Hubs":
    st.markdown('<p class="main-title">🗺️ Carte France — Livraisons en Temps Réel</p>', unsafe_allow_html=True)

    col_refresh_map = st.columns([4, 1])
    with col_refresh_map[1]:
        if st.button("🔄 Rafraîchir carte"):
            st.rerun()

    # ── Hubs français (hardcoded) ──
    HUB_COORDS = {
        "HUB_PARIS":      (48.8566,  2.3522, "Paris CDG"),
        "HUB_LYON":       (45.7640,  4.8357, "Lyon Sud"),
        "HUB_MARSEILLE":  (43.2965,  5.3698, "Marseille Fos"),
        "HUB_BORDEAUX":   (44.8378, -0.5792, "Bordeaux Nord"),
        "HUB_LILLE":      (50.6292,  3.0573, "Lille Nord"),
        "HUB_NANTES":     (47.2184, -1.5536, "Nantes Atlantique"),
        "HUB_STRASBOURG": (48.5734,  7.7521, "Strasbourg Est"),
    }
    ROUTE_LINES = [
        ("HUB_PARIS","HUB_LYON"),   ("HUB_PARIS","HUB_MARSEILLE"),
        ("HUB_PARIS","HUB_BORDEAUX"),("HUB_PARIS","HUB_LILLE"),
        ("HUB_LYON","HUB_MARSEILLE"),("HUB_PARIS","HUB_NANTES"),
        ("HUB_PARIS","HUB_STRASBOURG"),
    ]

    # ── Hub stats from DB ──
    hub_stats_df = query_db("""
        SELECT hub_id,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE risk_level = 'high') AS high_risk,
               COUNT(*) FILTER (WHERE risk_level = 'medium') AS medium_risk,
               ROUND(AVG(delay_minutes)::numeric,1) AS avg_delay,
               ROUND(100.0 * COUNT(*) FILTER (WHERE delay_minutes<=15)/NULLIF(COUNT(*),0),1) AS on_time_pct
        FROM parcel_state GROUP BY hub_id
    """)

    # ── Build map ──
    fig_map = go.Figure()

    # Route lines
    for h1, h2 in ROUTE_LINES:
        lat1, lon1, _ = HUB_COORDS[h1]
        lat2, lon2, _ = HUB_COORDS[h2]
        fig_map.add_trace(go.Scattermapbox(
            lat=[lat1, lat2], lon=[lon1, lon2],
            mode="lines", line=dict(width=2, color="#90cdf4"),
            showlegend=False, hoverinfo="none",
        ))

    # Hub markers
    has_stats = not hub_stats_df.empty
    for hid, (lat, lon, name) in HUB_COORDS.items():
        stats = hub_stats_df[hub_stats_df["hub_id"] == hid] if has_stats else pd.DataFrame()
        if len(stats) > 0:
            s = stats.iloc[0]
            total = int(s["total"])
            hr = int(s["high_risk"])
            otp = float(s["on_time_pct"] or 0)
            color = "red" if hr > 5 else "orange" if int(s["medium_risk"]) > 10 else "green"
            hover_text = f"<b>{name}</b><br>Colis: {total}<br>Haut risque: {hr}<br>On-time: {otp}%"
        else:
            color = "green"
            hover_text = f"<b>{name}</b><br>Aucun colis"

        fig_map.add_trace(go.Scattermapbox(
            lat=[lat], lon=[lon],
            mode="markers+text",
            marker=dict(size=18, color=color, opacity=0.9),
            text=[name],
            textposition="top center",
            textfont=dict(size=10, color="black"),
            hovertext=[hover_text],
            hoverinfo="text",
            name=name,
            showlegend=False,
        ))

    fig_map.update_layout(
        mapbox_style="carto-positron",
        mapbox=dict(center=dict(lat=46.8, lon=2.5), zoom=4.8),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=550,
    )
    st.plotly_chart(fig_map, use_container_width=True)

    # Hub statistics table
    st.subheader("📊 Statistiques par Hub")
    if not hub_stats_df.empty:
        def color_risk(row):
            if row.get("high_risk", 0) > 5:
                return ["background-color: #fed7d7; color: #742a2a"] * len(row)
            elif row.get("medium_risk", 0) > 10:
                return ["background-color: #fefcbf; color: #744210"] * len(row)
            return ["background-color: #c6f6d5; color: #22543d"] * len(row)
        st.dataframe(
            hub_stats_df.rename(columns={
                "hub_id":"Hub","total":"Total","high_risk":"🔴 Élevé",
                "medium_risk":"🟠 Moyen","avg_delay":"Délai moyen (min)","on_time_pct":"On-time %"
            }).style.apply(color_risk, axis=1),
            use_container_width=True, hide_index=True
        )
    else:
        st.info("⏳ En attente des données (pipeline en cours d'initialisation...)")

# ══════════════════════════════════════════════════════════════
# PAGE 2 : DASHBOARD & ALERTES
# ══════════════════════════════════════════════════════════════
elif page == "📊 Dashboard & Alertes":
    st.markdown('<p class="main-title">📊 Dashboard Temps Réel</p>', unsafe_allow_html=True)

    col_refresh = st.columns([4,1])
    with col_refresh[1]:
        if st.button("🔄 Actualiser"):
            st.rerun()

    # KPIs
    kpi_df = query_db("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status='delivered') AS delivered,
            COUNT(*) FILTER (WHERE status NOT IN ('delivered','planned')) AS in_transit,
            COUNT(*) FILTER (WHERE delay_minutes>15) AS delayed,
            COUNT(*) FILTER (WHERE risk_level='high') AS high_risk,
            ROUND(AVG(delay_minutes)::numeric,1) AS avg_delay,
            ROUND(100.0*COUNT(*) FILTER (WHERE delay_minutes<=15)/NULLIF(COUNT(*),0),1) AS on_time_pct
        FROM parcel_state
    """)

    if not kpi_df.empty:
        r = kpi_df.iloc[0]
        c1,c2,c3,c4,c5 = st.columns(5)
        with c1:
            st.metric("📦 Total colis", f"{int(r['total'] or 0):,}")
        with c2:
            st.metric("✅ Livrés", f"{int(r['delivered'] or 0):,}")
        with c3:
            st.metric("🚚 En transit", f"{int(r['in_transit'] or 0):,}")
        with c4:
            st.metric("⏰ En retard", f"{int(r['delayed'] or 0):,}",
                      delta=None if not r['delayed'] else f"-{r['delayed']} SLA",
                      delta_color="inverse")
        with c5:
            st.metric("🎯 On-time %", f"{r['on_time_pct'] or 0:.1f}%",
                      delta=f"{(r['on_time_pct'] or 0)-95:.1f}% vs SLA 95%",
                      delta_color="normal")
    else:
        st.info("⏳ En attente des données...")

    st.divider()
    col_left, col_right = st.columns([2, 1])

    with col_left:
        # Graphique retards par route
        route_df = query_db("""
            SELECT route_id,
                   ROUND(AVG(delay_minutes)::numeric,1) AS avg_delay,
                   COUNT(*) FILTER (WHERE delay_minutes>15) AS delayed_count,
                   ROUND(100.0*COUNT(*) FILTER (WHERE delay_minutes<=15)/NULLIF(COUNT(*),0),1) AS on_time_pct
            FROM parcel_state WHERE route_id IS NOT NULL
            GROUP BY route_id ORDER BY avg_delay DESC
        """)
        if not route_df.empty:
            fig_bar = px.bar(
                route_df, x="route_id", y="avg_delay",
                color="avg_delay", color_continuous_scale=["#68d391","#f6ad55","#fc8181"],
                title="⏱ Délai moyen par route (minutes)",
                labels={"route_id":"Route","avg_delay":"Délai moyen (min)"},
            )
            fig_bar.update_layout(showlegend=False, height=300,
                                  margin=dict(t=40,b=0,l=0,r=0))
            st.plotly_chart(fig_bar, use_container_width=True)

        # Distribution des risques
        risk_df = query_db("""
            SELECT risk_level, COUNT(*) AS count FROM parcel_state GROUP BY risk_level
        """)
        if not risk_df.empty:
            color_map_risk = {"low":"#68d391","medium":"#f6ad55","high":"#fc8181"}
            fig_pie = px.pie(
                risk_df, names="risk_level", values="count",
                color="risk_level", color_discrete_map=color_map_risk,
                title="🎯 Distribution des niveaux de risque",
            )
            fig_pie.update_layout(height=280, margin=dict(t=40,b=0,l=0,r=0))
            st.plotly_chart(fig_pie, use_container_width=True)

    with col_right:
        st.subheader("🚨 Alertes actives")
        alerts_df = query_db("""
            SELECT a.severity, a.message, a.created_at,
                   p.hub_id, p.client_id
            FROM alerts a
            LEFT JOIN parcel_state p ON p.parcel_id=a.parcel_id
            WHERE a.acknowledged=FALSE
            ORDER BY a.created_at DESC LIMIT 20
        """)
        if not alerts_df.empty:
            for _, row in alerts_df.iterrows():
                sev = row.get("severity","low")
                css = f"alert-{sev if sev in ['critical','high','medium','low'] else 'low'}"
                icon = "🔴" if sev in ("critical","high") else "🟠" if sev=="medium" else "🟢"
                st.markdown(
                    f'<div class="{css}">{icon} <b>{row.get("hub_id","?")}</b> | '
                    f'{row.get("client_id","?")} — {str(row.get("message",""))[:80]}'
                    f'<br><small>{str(row.get("created_at",""))[:19]}</small></div>',
                    unsafe_allow_html=True
                )
        else:
            st.success("✅ Aucune alerte active")

    # Tableau colis à risque
    st.divider()
    st.subheader("📋 Colis à risque / en retard")
    parcels_df = query_db("""
        SELECT parcel_id, client_id, hub_id, route_id, status,
               risk_level, ROUND(delay_minutes::numeric,0) AS delay_min,
               planned_delivery
        FROM parcel_state
        WHERE risk_level IN ('medium','high') OR delay_minutes > 15
        ORDER BY delay_min DESC LIMIT 50
    """)
    if not parcels_df.empty:
        def style_risk(val):
            if val == "high":   return "background-color:#fed7d7;color:#c53030;font-weight:bold"
            if val == "medium": return "background-color:#fefcbf;color:#744210;font-weight:bold"
            if val == "low":    return "background-color:#c6f6d5;color:#22543d"
            return ""
        st.dataframe(
            parcels_df.style.map(style_risk, subset=["risk_level"]),
            use_container_width=True, hide_index=True
        )
    else:
        st.success("✅ Aucun colis critique détecté")

# ══════════════════════════════════════════════════════════════
# PAGE 3 : AGENT IA CHAT
# ══════════════════════════════════════════════════════════════
elif page == "💬 Agent IA":
    st.markdown('<p class="main-title">💬 Assistant IA Control Tower</p>', unsafe_allow_html=True)
    st.caption("Posez une question en langage naturel. L'agent utilisera ses outils pour analyser les données.")

    # Init historique
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "llm_history" not in st.session_state:
        st.session_state.llm_history = []

    # Questions suggérées
    st.markdown("**💡 Questions suggérées :**")
    q_cols = st.columns(3)
    suggestions = [
        "Quels hubs sont à risque élevé en ce moment ?",
        "Quelles routes ont le plus de retards ?",
        "Quels clients sont impactés par des retards ?",
        "Donne-moi un résumé des KPIs de livraison",
        "Quels colis risquent d'être en retard sur la route Paris-Lyon ?",
        "Y a-t-il des alertes critiques non traitées ?",
    ]
    for i, sug in enumerate(suggestions):
        with q_cols[i % 3]:
            if st.button(sug, key=f"sug_{i}", use_container_width=True):
                st.session_state.pending_question = sug
                st.rerun()

    st.divider()

    # ── Affichage de l'historique avec les composants natifs Streamlit ──
    for msg in st.session_state.chat_history:
        if msg["role"] == "user":
            with st.chat_message("user", avatar="👤"):
                st.markdown(msg["content"])
        else:
            with st.chat_message("assistant", avatar="🤖"):
                st.markdown(msg["content"])
                # Raisonnement
                if msg.get("reasoning"):
                    with st.expander("🧠 Raisonnement de l'agent", expanded=False):
                        for step in msg["reasoning"]:
                            st.caption(step)
                # Tool calls
                if msg.get("tool_calls"):
                    with st.expander(f"🔧 {len(msg['tool_calls'])} appels d'outils", expanded=False):
                        for tc in msg["tool_calls"]:
                            st.json({
                                "outil": tc.get("tool", "?"),
                                "paramètres": tc.get("args", {}),
                                "résultat_résumé": str(tc.get("result", ""))[:300]
                            })

    # ── Champ de saisie natif (toujours en bas de page) ──
    # Gérer la question en attente (clic sur suggestion)
    pending = st.session_state.pop("pending_question", None)

    prompt = st.chat_input("Posez votre question ici…")

    # Si une suggestion a été cliquée, l'utiliser comme prompt
    question = pending or prompt

    if question:
        # Afficher immédiatement le message utilisateur
        with st.chat_message("user", avatar="👤"):
            st.markdown(question)
        st.session_state.chat_history.append({"role": "user", "content": question})

        # Appeler l'agent et afficher la réponse
        with st.chat_message("assistant", avatar="🤖"):
            with st.spinner("🧠 L'agent analyse votre question..."):
                response = call_agent(question, st.session_state.llm_history)

            answer = response.get("answer", "Aucune réponse")
            st.markdown(answer)

            reasoning = response.get("reasoning", [])
            tool_calls = response.get("tool_calls", [])

            if reasoning:
                with st.expander("🧠 Raisonnement de l'agent", expanded=False):
                    for step in reasoning:
                        st.caption(step)
            if tool_calls:
                with st.expander(f"🔧 {len(tool_calls)} appels d'outils", expanded=False):
                    for tc in tool_calls:
                        st.json({
                            "outil": tc.get("tool", "?"),
                            "paramètres": tc.get("args", {}),
                            "résultat_résumé": str(tc.get("result", ""))[:300]
                        })

        # Sauvegarder dans l'historique
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": answer,
            "reasoning": reasoning,
            "tool_calls": tool_calls,
        })
        st.session_state.llm_history.append({"role": "user", "content": question})
        st.session_state.llm_history.append({"role": "assistant", "content": answer})

    # Bouton effacer
    if st.session_state.chat_history:
        if st.sidebar.button("🗑️ Effacer la conversation"):
            st.session_state.chat_history = []
            st.session_state.llm_history  = []
            st.rerun()

# ══════════════════════════════════════════════════════════════
# PAGE 4 : JOURNAL ÉVÉNEMENTS
# ══════════════════════════════════════════════════════════════
elif page == "📋 Journal événements":
    st.markdown('<p class="main-title">📋 Journal des Événements</p>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        event_filter = st.selectbox("Type d'événement", ["Tous","accepted","pickup","in_transit","delivered"])
    with col2:
        limit = st.selectbox("Nombre de lignes", [50, 100, 200, 500], index=0)
    with col3:
        hub_filter = st.text_input("Filtrer par Hub ID", "")

    where_clauses = []
    if event_filter != "Tous":
        where_clauses.append(f"event_type = '{event_filter}'")
    if hub_filter:
        where_clauses.append(f"hub_id = '{hub_filter}'")
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    events_df = query_db(f"""
        SELECT parcel_id, event_type, event_timestamp, hub_id, latitude, longitude, ingested_at
        FROM delivery_events_log
        {where_sql}
        ORDER BY event_timestamp DESC LIMIT {limit}
    """)

    if not events_df.empty:
        st.dataframe(events_df, use_container_width=True, hide_index=True)
        st.caption(f"Affichage des {len(events_df)} derniers événements")

        # Graphique timeline
        timeline_df = query_db("""
            SELECT DATE_TRUNC('hour', event_timestamp) AS heure,
                   event_type, COUNT(*) AS count
            FROM delivery_events_log
            GROUP BY heure, event_type ORDER BY heure
        """)
        if not timeline_df.empty:
            fig_timeline = px.line(
                timeline_df, x="heure", y="count", color="event_type",
                title="📈 Flux d'événements par heure",
                labels={"heure":"Heure","count":"Nombre d'événements","event_type":"Type"}
            )
            fig_timeline.update_layout(height=300, margin=dict(t=40,b=0))
            st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("⏳ Aucun événement enregistré. Lancez le pipeline Replay pour générer des données.")

    st.divider()
    st.subheader("❌ Dead Letter Queue (erreurs)")
    errors_df = query_db("SELECT * FROM error_logs ORDER BY created_at DESC LIMIT 20")
    if not errors_df.empty:
        st.dataframe(errors_df, use_container_width=True, hide_index=True)
    else:
        st.success("✅ Aucune erreur dans la DLQ")
