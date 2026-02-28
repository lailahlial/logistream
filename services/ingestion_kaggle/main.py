"""
Ingestion Kaggle → PostgreSQL + Topic Kafka `planned_shipments`

Lit le fichier CSV kaggle_planned.csv et :
  1. Insère chaque colis dans la table parcel_state (initialisation).
  2. Publie chaque colis sur le topic Kafka `planned_shipments`.
"""

import os, csv, json, time, logging
from confluent_kafka import Producer, KafkaException
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [INGESTION-KAGGLE] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DATABASE_URL    = os.getenv("DATABASE_URL", "postgresql://logistream:logistream_secret@postgres:5432/logistream")
DATA_FILE       = "/app/data/kaggle_planned.csv"
TOPIC           = "planned_shipments"

def get_db():
    for attempt in range(15):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = False
            log.info("✅ PostgreSQL connecté")
            return conn
        except Exception as e:
            log.warning(f"PG indisponible ({attempt+1}/15) : {e}")
            time.sleep(4)
    raise RuntimeError("Impossible de se connecter à PostgreSQL")

def get_producer():
    for attempt in range(15):
        try:
            p = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "acks": "all",
                "retries": 3,
            })
            log.info("✅ Kafka connecté")
            return p
        except Exception as e:
            log.warning(f"Kafka indisponible ({attempt+1}/15) : {e}")
            time.sleep(5)
    raise RuntimeError("Impossible de se connecter à Kafka")

def delivery_report(err, msg):
    if err:
        log.error(f"❌ Échec publication {msg.key()} : {err}")

def main():
    log.info("🚀 Démarrage Ingestion Kaggle")

    # Attente fichier
    for _ in range(30):
        if os.path.exists(DATA_FILE):
            break
        log.info(f"En attente de {DATA_FILE}...")
        time.sleep(3)
    else:
        log.error(f"❌ Fichier {DATA_FILE} introuvable !")
        return

    conn     = get_db()
    producer = get_producer()

    inserted = 0
    skipped  = 0

    with open(DATA_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parcel_id = row.get("parcel_id", "").strip()
            if not parcel_id:
                skipped += 1
                continue

            payload = {
                "parcel_id":        parcel_id,
                "client_id":        row.get("client_id", ""),
                "route_id":         row.get("route_id", ""),
                "hub_id":           row.get("hub_id", ""),
                "dest_hub_id":      row.get("dest_hub_id", ""),
                "planned_pickup":   row.get("planned_pickup", ""),
                "planned_delivery": row.get("planned_delivery", ""),
                "weather_condition":row.get("weather_condition", "clear"),
                "congestion_level": float(row.get("congestion_level", 0) or 0),
                "distance_km":      float(row.get("distance_km", 0) or 0),
                "weight_kg":        float(row.get("weight_kg", 0) or 0),
            }

            # Insert into parcel_state
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO parcel_state
                            (parcel_id, client_id, route_id, hub_id, status,
                             planned_pickup, planned_delivery,
                             weather_condition, congestion_level, updated_at)
                        VALUES
                            (%(parcel_id)s, %(client_id)s, %(route_id)s, %(hub_id)s, 'planned',
                             %(planned_pickup)s, %(planned_delivery)s,
                             %(weather_condition)s, %(congestion_level)s, NOW())
                        ON CONFLICT (parcel_id) DO UPDATE SET
                            status           = EXCLUDED.status,
                            planned_pickup   = EXCLUDED.planned_pickup,
                            planned_delivery = EXCLUDED.planned_delivery,
                            weather_condition= EXCLUDED.weather_condition,
                            congestion_level = EXCLUDED.congestion_level,
                            updated_at       = NOW()
                    """, payload)
                conn.commit()
            except Exception as e:
                log.error(f"❌ Erreur INSERT {parcel_id} : {e}")
                conn.rollback()
                skipped += 1
                continue

            # Publish to Kafka
            producer.produce(
                TOPIC,
                key=parcel_id.encode(),
                value=json.dumps(payload).encode(),
                callback=delivery_report,
            )
            producer.poll(0)

            inserted += 1
            if inserted % 500 == 0:
                producer.flush()
                log.info(f"  → {inserted} colis insérés et publiés")

    producer.flush()
    conn.close()
    log.info(f"✅ Ingestion terminée : {inserted} colis insérés, {skipped} ignorés")

if __name__ == "__main__":
    main()
