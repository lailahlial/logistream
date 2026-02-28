"""
generate_data.py – Génère des datasets synthétiques simulant
    • Kaggle  : historique planifié (planned_shipments)
    • LaDe    : événements temps réel (delivery_events)

Usage : python scripts/generate_data.py
"""

import os, random, json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

random.seed(42)
np.random.seed(42)

# ── Constantes ────────────────────────────────────────────────
HUBS = ["HUB_PARIS", "HUB_LYON", "HUB_MARSEILLE",
        "HUB_BORDEAUX", "HUB_LILLE", "HUB_NANTES", "HUB_STRASBOURG"]
ROUTES = {
    "R_PAR_LYO": ("HUB_PARIS", "HUB_LYON",      4.5),
    "R_PAR_MAR": ("HUB_PARIS", "HUB_MARSEILLE", 7.5),
    "R_PAR_BOR": ("HUB_PARIS", "HUB_BORDEAUX",  5.5),
    "R_PAR_LIL": ("HUB_PARIS", "HUB_LILLE",     2.5),
    "R_LYO_MAR": ("HUB_LYON",  "HUB_MARSEILLE", 3.0),
    "R_PAR_NAN": ("HUB_PARIS", "HUB_NANTES",    4.0),
    "R_PAR_STR": ("HUB_PARIS", "HUB_STRASBOURG",4.8),
}
CLIENTS  = ["CLIENT_A", "CLIENT_B", "CLIENT_C", "CLIENT_D", "CLIENT_E"]
WEATHER  = ["clear", "rain", "fog", "snow", "storm"]
WEATHER_DELAY_FACTOR = {"clear": 1.0, "rain": 1.3, "fog": 1.2, "snow": 1.8, "storm": 2.5}

N_PARCELS = 3000
BASE_DATE = datetime(2024, 1, 8, 6, 0, 0)

# ── Génération Kaggle (planned) ───────────────────────────────
def generate_kaggle(n=N_PARCELS):
    rows = []
    for i in range(n):
        route_id = random.choice(list(ROUTES.keys()))
        origin, dest, avg_h = ROUTES[route_id]
        client   = random.choice(CLIENTS)
        weather  = random.choices(WEATHER, weights=[50,25,10,10,5])[0]
        cong     = round(random.uniform(0, 1), 2)

        # Heure planifiée pickup
        pickup_offset = timedelta(minutes=random.randint(0, 5*24*60))
        planned_pickup = BASE_DATE + pickup_offset

        # Durée planifiée + bruit
        base_duration_h = avg_h * WEATHER_DELAY_FACTOR[weather] * (1 + cong * 0.3)
        planned_delivery = planned_pickup + timedelta(hours=base_duration_h)

        rows.append({
            "parcel_id":        f"FR{100000 + i:06d}",
            "client_id":        client,
            "route_id":         route_id,
            "hub_id":           origin,
            "dest_hub_id":      dest,
            "planned_pickup":   planned_pickup.isoformat(),
            "planned_delivery": planned_delivery.isoformat(),
            "weather_condition":weather,
            "congestion_level": cong,
            "distance_km":      int(random.uniform(200, 900)),
            "weight_kg":        round(random.uniform(0.1, 50.0), 2),
        })
    df = pd.DataFrame(rows)
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/kaggle_planned.csv", index=False)
    print(f"[Kaggle] {len(df)} colis planifiés → data/kaggle_planned.csv")
    return df

# ── Génération LaDe (événements réels) ───────────────────────
def generate_lade(kaggle_df, n=N_PARCELS):
    events = []
    for _, row in kaggle_df.iterrows():
        parcel_id      = row["parcel_id"]
        planned_pickup = datetime.fromisoformat(row["planned_pickup"])
        planned_deliv  = datetime.fromisoformat(row["planned_delivery"])
        weather        = row["weather_condition"]
        cong           = float(row["congestion_level"])

        # Retard aléatoire selon météo + congestion
        delay_factor = WEATHER_DELAY_FACTOR[weather] * (1 + cong * 0.5)
        is_late = random.random() < (0.1 + cong * 0.3 + (delay_factor - 1) * 0.2)

        # 1) Événement ACCEPTED
        events.append({
            "parcel_id":        parcel_id,
            "event_type":       "accepted",
            "event_timestamp":  (planned_pickup - timedelta(hours=1)).isoformat(),
            "hub_id":           row["hub_id"],
            "latitude":         round(random.uniform(43.0, 51.0), 4),
            "longitude":        round(random.uniform(-2.0, 8.0),  4),
        })

        # 2) PICKUP – parfois en retard
        pickup_delay_min = int(random.gauss(0, 15) * (2.0 if is_late else 0.5))
        actual_pickup = planned_pickup + timedelta(minutes=max(0, pickup_delay_min))
        events.append({
            "parcel_id":       parcel_id,
            "event_type":      "pickup",
            "event_timestamp": actual_pickup.isoformat(),
            "hub_id":          row["hub_id"],
            "latitude":        round(random.uniform(43.0, 51.0), 4),
            "longitude":       round(random.uniform(-2.0, 8.0),  4),
        })

        # 3) IN_TRANSIT
        transit_time = planned_deliv - planned_pickup
        in_transit_ts = actual_pickup + transit_time * 0.4
        events.append({
            "parcel_id":       parcel_id,
            "event_type":      "in_transit",
            "event_timestamp": in_transit_ts.isoformat(),
            "hub_id":          row["hub_id"],
            "latitude":        round(random.uniform(43.0, 51.0), 4),
            "longitude":       round(random.uniform(-2.0, 8.0),  4),
        })

        # 4) DELIVERED
        delivery_delay_min = (
            int(random.gauss(60, 30) * delay_factor) if is_late
            else int(random.gauss(-10, 10))
        )
        actual_delivery = planned_deliv + timedelta(minutes=delivery_delay_min)
        events.append({
            "parcel_id":       parcel_id,
            "event_type":      "delivered",
            "event_timestamp": actual_delivery.isoformat(),
            "hub_id":          row["dest_hub_id"],
            "latitude":        round(random.uniform(43.0, 51.0), 4),
            "longitude":       round(random.uniform(-2.0, 8.0),  4),
        })

    df_events = pd.DataFrame(events)
    df_events = df_events.sort_values("event_timestamp").reset_index(drop=True)
    df_events.to_csv("data/lade_events.csv", index=False)
    print(f"[LaDe]   {len(df_events)} événements → data/lade_events.csv")
    return df_events


if __name__ == "__main__":
    print("🚀 Génération des données synthétiques LogiStream...")
    kaggle_df = generate_kaggle()
    generate_lade(kaggle_df)
    print("✅ Données générées avec succès !")
