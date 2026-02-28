"""
Service d'Enrichissement & Corrélation (Stateful)

Consomme :
  • planned_shipments  → initialise / met à jour le store d'état
  • delivery_events    → corrèle avec le plan, détecte retards, publie alertes

Produit :
  • alerts             → si retard détecté
  • error_logs         → si parcel_id inconnu
"""

import os, json, time, logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ENRICHISSEMENT] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP       = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DATABASE_URL          = os.getenv("DATABASE_URL", "postgresql://logistream:logistream_secret@postgres:5432/logistream")
DELAY_THRESHOLD_MIN   = float(os.getenv("DELAY_THRESHOLD_MINUTES", "15"))

TOPICS_CONSUME = ["planned_shipments", "delivery_events"]
TOPIC_ALERTS   = "alerts"
TOPIC_ERRORS   = "error_logs"

# ── Connexions ───────────────────────────────────────────────────
def get_db():
    for i in range(15):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = False
            log.info("✅ PostgreSQL connecté")
            return conn
        except Exception as e:
            log.warning(f"PG indisponible ({i+1}/15) : {e}")
            time.sleep(4)
    raise RuntimeError("Impossible de se connecter à PostgreSQL")

def get_kafka_consumer():
    for i in range(15):
        try:
            c = Consumer({
                "bootstrap.servers":  KAFKA_BOOTSTRAP,
                "group.id":           "enrichissement-group",
                "auto.offset.reset":  "earliest",
                "enable.auto.commit": True,
                "session.timeout.ms": 30000,
            })
            c.subscribe(TOPICS_CONSUME)
            log.info(f"✅ Consumer Kafka souscrit à {TOPICS_CONSUME}")
            return c
        except Exception as e:
            log.warning(f"Kafka indisponible ({i+1}/15) : {e}")
            time.sleep(4)
    raise RuntimeError("Impossible de se connecter à Kafka (consumer)")

def get_kafka_producer():
    for i in range(10):
        try:
            p = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "acks": "all",
            })
            log.info("✅ Producer Kafka connecté")
            return p
        except Exception as e:
            log.warning(f"Kafka producer indisponible ({i+1}/10) : {e}")
            time.sleep(4)
    raise RuntimeError("Impossible de se connecter à Kafka (producer)")

# ── Handlers ─────────────────────────────────────────────────────
def handle_planned_shipment(payload: dict, conn):
    """Initialise ou met à jour le store d'état depuis le plan de transport."""
    sql = """
        INSERT INTO parcel_state
            (parcel_id, client_id, route_id, hub_id, status,
             planned_pickup, planned_delivery,
             weather_condition, congestion_level, updated_at)
        VALUES
            (%(parcel_id)s, %(client_id)s, %(route_id)s, %(hub_id)s, 'planned',
             %(planned_pickup)s, %(planned_delivery)s,
             %(weather_condition)s, %(congestion_level)s, NOW())
        ON CONFLICT (parcel_id) DO UPDATE SET
            status            = EXCLUDED.status,
            planned_pickup    = EXCLUDED.planned_pickup,
            planned_delivery  = EXCLUDED.planned_delivery,
            weather_condition = EXCLUDED.weather_condition,
            congestion_level  = EXCLUDED.congestion_level,
            updated_at        = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "parcel_id":        payload["parcel_id"],
            "client_id":        payload.get("client_id"),
            "route_id":         payload.get("route_id"),
            "hub_id":           payload.get("hub_id"),
            "planned_pickup":   payload.get("planned_pickup"),
            "planned_delivery": payload.get("planned_delivery"),
            "weather_condition":payload.get("weather_condition","clear"),
            "congestion_level": payload.get("congestion_level", 0),
        })
    conn.commit()


def handle_delivery_event(payload: dict, conn, producer) -> str | None:
    """
    Corrèle l'événement terrain avec le plan.
    Retourne 'alert' | 'ok' | 'unknown'
    """
    parcel_id  = payload["parcel_id"]
    event_type = payload["event_type"]
    event_ts   = payload["event_timestamp"]

    # ── Log l'événement ──
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO delivery_events_log
                (parcel_id, event_type, event_timestamp, hub_id,
                 latitude, longitude, raw_payload, ingested_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            parcel_id, event_type, event_ts,
            payload.get("hub_id"),
            payload.get("latitude"), payload.get("longitude"),
            json.dumps(payload),
        ))
    conn.commit()

    # ── Récupérer l'état planifié ──
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM parcel_state WHERE parcel_id = %s", (parcel_id,))
        state = cur.fetchone()

    if not state:
        error_msg = {
            "reason":     "unknown_parcel",
            "parcel_id":  parcel_id,
            "event_type": event_type,
            "raw":        payload,
        }
        producer.produce(
            TOPIC_ERRORS,
            key=parcel_id.encode(),
            value=json.dumps(error_msg).encode()
        )
        producer.poll(0)
        log.warning(f"⚠️  Colis inconnu {parcel_id} → error_logs")
        return "unknown"

    # ── Mettre à jour le statut ──
    status_map = {
        "accepted":   "accepted",
        "pickup":     "picked_up",
        "in_transit": "in_transit",
        "delivered":  "delivered",
    }
    new_status = status_map.get(event_type, state["status"])

    # ── Calcul retard ──
    delay_minutes = 0.0
    alert_needed  = False

    if event_type == "pickup" and state.get("planned_pickup"):
        try:
            planned = datetime.fromisoformat(str(state["planned_pickup"]))
            actual  = datetime.fromisoformat(event_ts.replace("Z",""))
            delay_minutes = (actual - planned).total_seconds() / 60
            if delay_minutes > DELAY_THRESHOLD_MIN:
                alert_needed = True
        except Exception as e:
            log.debug(f"Calcul retard pickup échoué : {e}")

    if event_type == "delivered" and state.get("planned_delivery"):
        try:
            planned = datetime.fromisoformat(str(state["planned_delivery"]))
            actual  = datetime.fromisoformat(event_ts.replace("Z",""))
            delay_minutes = (actual - planned).total_seconds() / 60
            if delay_minutes > DELAY_THRESHOLD_MIN:
                alert_needed = True
        except Exception as e:
            log.debug(f"Calcul retard delivery échoué : {e}")

    # ── Niveau de risque ──
    # ≤15 min → low (on-time) | ≤60 min → medium (at risk) | >60 min → high (late)
    risk_level = "low"
    risk_probability = 0.0
    if 15 < delay_minutes <= 60:
        risk_level = "medium"
        risk_probability = min(1.0, 0.3 + (delay_minutes - 15) / 90)
    elif delay_minutes > 60:
        risk_level = "high"
        risk_probability = min(1.0, 0.7 + (delay_minutes - 60) / 150)
    elif delay_minutes > 0:
        risk_probability = min(0.3, delay_minutes / 50)

    # ── Mise à jour store ──
    update_fields = {"status": new_status, "risk_level": risk_level,
                     "delay_minutes": delay_minutes, "risk_probability": risk_probability,
                     "hub_id": payload.get("hub_id", state["hub_id"])}
    if event_type == "pickup":
        update_fields["actual_pickup"] = event_ts
    if event_type == "delivered":
        update_fields["actual_delivery"] = event_ts

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE parcel_state
            SET status           = %(status)s,
                risk_level       = %(risk_level)s,
                risk_probability = %(risk_probability)s,
                delay_minutes    = %(delay_minutes)s,
                hub_id           = %(hub_id)s,
                actual_pickup    = COALESCE(%(actual_pickup)s::timestamp, actual_pickup),
                actual_delivery  = COALESCE(%(actual_delivery)s::timestamp, actual_delivery),
                updated_at       = NOW()
            WHERE parcel_id = %(parcel_id)s
        """, {
            **update_fields,
            "actual_pickup":   update_fields.get("actual_pickup"),
            "actual_delivery": update_fields.get("actual_delivery"),
            "parcel_id":       parcel_id,
        })
    conn.commit()

    # ── Alerte si retard ──
    if alert_needed:
        severity = "critical" if delay_minutes > 120 else "high" if delay_minutes > 60 else "medium" if delay_minutes > 30 else "low"
        alert = {
            "alert_id":    f"ALT-{parcel_id}-{int(time.time())}",
            "parcel_id":   parcel_id,
            "alert_type":  "delay_detected",
            "severity":    severity,
            "delay_min":   round(delay_minutes, 1),
            "event_type":  event_type,
            "hub_id":      payload.get("hub_id"),
            "client_id":   state.get("client_id"),
            "route_id":    state.get("route_id"),
            "message":     f"Colis {parcel_id} en retard de {delay_minutes:.0f} min ({event_type})",
            "created_at":  datetime.utcnow().isoformat(),
        }
        producer.produce(
            TOPIC_ALERTS,
            key=parcel_id.encode(),
            value=json.dumps(alert).encode()
        )
        producer.poll(0)

        # Persist en DB
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alerts (parcel_id, alert_type, severity, message)
                VALUES (%s,%s,%s,%s)
            """, (parcel_id, "delay_detected", severity, alert["message"]))
        conn.commit()

        log.info(f"🚨 ALERTE [{severity.upper()}] {parcel_id} — {delay_minutes:.0f} min de retard")
        return "alert"

    return "ok"

# ── Boucle principale ────────────────────────────────────────────
def main():
    log.info("🚀 Démarrage Service d'Enrichissement & Corrélation")
    conn     = get_db()
    consumer = get_kafka_consumer()
    producer = get_kafka_producer()

    stats = {"planned": 0, "events": 0, "alerts": 0, "errors": 0}

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Kafka error : {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                topic   = msg.topic()

                if topic == "planned_shipments":
                    handle_planned_shipment(payload, conn)
                    stats["planned"] += 1
                    if stats["planned"] % 500 == 0:
                        log.info(f"📦 {stats['planned']} colis planifiés chargés")

                elif topic == "delivery_events":
                    result = handle_delivery_event(payload, conn, producer)
                    stats["events"] += 1
                    if result == "alert":
                        stats["alerts"] += 1
                    elif result == "unknown":
                        stats["errors"] += 1

                    if stats["events"] % 100 == 0:
                        log.info(
                            f"📬 Events={stats['events']} | "
                            f"🚨 Alertes={stats['alerts']} | "
                            f"⚠️ Erreurs={stats['errors']}"
                        )

            except json.JSONDecodeError as e:
                log.error(f"JSON invalide : {e} | raw={msg.value()[:100]}")
            except Exception as e:
                log.error(f"Erreur traitement message : {e}", exc_info=True)
                try:
                    conn.rollback()
                except:
                    pass
                # Reconnexion DB si nécessaire
                try:
                    conn.close()
                except:
                    pass
                conn = get_db()

    except KeyboardInterrupt:
        log.info("Arrêt demandé")
    finally:
        consumer.close()
        producer.flush()
        conn.close()
        log.info(f"✅ Service arrêté. Stats finales : {stats}")

if __name__ == "__main__":
    main()
