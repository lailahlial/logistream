"""
Script Replay LaDe → Topic Kafka `delivery_events`

Simule l'écoulement du temps : 1h réelle = TIME_ACCELERATION secondes.
Publie les événements dans l'ordre chronologique.
"""

import os, csv, json, time, logging
from datetime import datetime
from confluent_kafka import Producer, KafkaException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [REPLAY-LADE] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TIME_ACCELERATION  = float(os.getenv("TIME_ACCELERATION", "360"))  # 1h réelle = 10s sim
DATA_FILE          = "/app/data/lade_events.csv"
TOPIC              = "delivery_events"

def get_producer():
    for attempt in range(15):
        try:
            p = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "acks": "all",
                "retries": 3,
                "batch.size": 16384,
                "linger.ms": 50,
            })
            log.info("✅ Kafka connecté")
            return p
        except Exception as e:
            log.warning(f"Kafka non disponible ({attempt+1}/15) : {e}")
            time.sleep(5)
    raise RuntimeError("Impossible de se connecter à Kafka")

def delivery_report(err, msg):
    if err:
        log.error(f"❌ Échec {msg.key()} : {err}")

def parse_ts(ts_str: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Format timestamp inconnu : {ts_str}")

def main():
    log.info("🚀 Démarrage Replay LaDe")
    log.info(f"   Facteur accélération : ×{TIME_ACCELERATION} "
             f"(1h réelle = {3600/TIME_ACCELERATION:.1f}s sim)")

    # Attente fichier
    for _ in range(30):
        if os.path.exists(DATA_FILE):
            break
        log.info(f"En attente de {DATA_FILE}...")
        time.sleep(3)

    producer = get_producer()

    while True:
        # ── Chargement et tri des événements ──
        log.info("📂 Chargement des événements LaDe...")
        events = []
        with open(DATA_FILE, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ts = parse_ts(row["event_timestamp"])
                    events.append({**row, "_ts": ts})
                except Exception as e:
                    log.debug(f"Ligne ignorée : {e}")

        events.sort(key=lambda x: x["_ts"])
        log.info(f"   {len(events)} événements chargés | "
                 f"Période : {events[0]['_ts']} → {events[-1]['_ts']}")

        if not events:
            log.error("Aucun événement trouvé !")
            return

        # ── Replay ──
        sim_start_real = time.time()
        data_start_ts  = events[0]["_ts"]
        published      = 0

        for ev in events:
            # Calculer le délai de simulation
            data_elapsed_s   = (ev["_ts"] - data_start_ts).total_seconds()
            sim_elapsed_s    = data_elapsed_s / TIME_ACCELERATION
            real_elapsed_s   = time.time() - sim_start_real
            wait_s           = sim_elapsed_s - real_elapsed_s

            if wait_s > 0:
                time.sleep(wait_s)

            # Publier l'événement
            message = {
                "event_id":         f"EV-{ev['parcel_id']}-{ev['event_type'][:3].upper()}-{int(time.time()*1000)}",
                "parcel_id":        ev["parcel_id"],
                "event_type":       ev["event_type"],
                "event_timestamp":  ev["event_timestamp"],
                "hub_id":           ev.get("hub_id", ""),
                "latitude":         float(ev.get("latitude",  0) or 0),
                "longitude":        float(ev.get("longitude", 0) or 0),
                "replayed_at":      datetime.utcnow().isoformat(),
            }

            producer.produce(
                TOPIC,
                key=ev["parcel_id"].encode(),
                value=json.dumps(message).encode(),
                callback=delivery_report,
            )
            producer.poll(0)
            published += 1

            if published % 200 == 0:
                producer.flush()
                speed = published / (time.time() - sim_start_real)
                log.info(f"  → {published}/{len(events)} événements | {speed:.1f} ev/s")

        producer.flush()
        log.info(f"✅ Replay terminé : {published} événements publiés sur '{TOPIC}'")
        log.info("⏳ Pause 10s avant le prochain cycle...")
        time.sleep(10)

if __name__ == "__main__":
    main()
