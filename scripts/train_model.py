"""
train_model.py – Entraîne un modèle XGBoost pour prédire le risque de retard.

Sorties :
  ml/delay_model.pkl    – Modèle sérialisé
  ml/feature_names.json – Liste des features
  ml/metrics.json       – Métriques d'évaluation

Usage : python scripts/train_model.py
"""

import os, json, pickle
import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.preprocessing   import LabelEncoder
from sklearn.metrics         import classification_report, accuracy_score, f1_score
import xgboost as xgb

# ── Risk thresholds (aligned with enrichissement service) ──────
# enrichissement/main.py uses:  ≤15 → low,  15-60 → medium,  >60 → high
DELAY_LOW_MAX     = 15    # minutes
DELAY_MEDIUM_MAX  = 60    # minutes

# ── Chargement des données ─────────────────────────────────────
def load_data():
    planned = pd.read_csv("data/kaggle_planned.csv")
    events  = pd.read_csv("data/lade_events.csv")
    print(f"   Planned CSV : {len(planned)} rows")
    print(f"   Events CSV  : {len(events)} rows")

    # Parse timestamps
    for col in ["planned_pickup", "planned_delivery"]:
        planned[col] = pd.to_datetime(planned[col], format="ISO8601", errors="coerce")
    events["event_timestamp"] = pd.to_datetime(events["event_timestamp"], format="ISO8601", errors="coerce")

    # Pivot events to get actual pickup and delivery timestamps per parcel
    pickup    = events[events["event_type"] == "pickup"][["parcel_id","event_timestamp"]].rename(
                    columns={"event_timestamp": "actual_pickup"})
    delivered = events[events["event_type"] == "delivered"][["parcel_id","event_timestamp"]].rename(
                    columns={"event_timestamp": "actual_delivery"})

    df = planned.merge(pickup,    on="parcel_id", how="left")
    df = df.merge(delivered, on="parcel_id", how="left")

    # Compute delay in minutes (actual_delivery - planned_delivery)
    df["delay_minutes"] = (
        (df["actual_delivery"] - df["planned_delivery"]).dt.total_seconds() / 60
    ).fillna(0)

    # Label de risque
    def risk_label(delay):
        if delay <= DELAY_LOW_MAX:    return 0
        if delay <= DELAY_MEDIUM_MAX: return 1
        return 2

    df["risk_label"] = df["delay_minutes"].apply(risk_label)

    print(f"   Cleaned dataset: {len(df)} rows")
    return df

# ── Feature engineering ────────────────────────────────────────
def build_features(df):
    le_route   = LabelEncoder()
    le_weather = LabelEncoder()
    le_client  = LabelEncoder()
    le_hub     = LabelEncoder()

    df["route_enc"]   = le_route.fit_transform(df["route_id"].fillna("unknown"))
    df["weather_enc"] = le_weather.fit_transform(df["weather_condition"].fillna("clear"))
    df["client_enc"]  = le_client.fit_transform(df["client_id"].fillna("unknown"))
    df["hub_enc"]     = le_hub.fit_transform(df["hub_id"].fillna("unknown"))

    # Features temporelles
    df["planned_pickup_dt"] = pd.to_datetime(df["planned_pickup"])
    df["hour_of_day"]       = df["planned_pickup_dt"].dt.hour
    df["day_of_week"]       = df["planned_pickup_dt"].dt.dayofweek
    df["is_weekend"]        = (df["day_of_week"] >= 5).astype(int)

    # Durée prévue
    df["planned_duration_h"] = (
        (pd.to_datetime(df["planned_delivery"]) - df["planned_pickup_dt"])
        .dt.total_seconds() / 3600
    ).fillna(24)

    # ── Interaction & derived features ──
    # Weather × congestion interaction (bad weather + high congestion = high risk)
    df["weather_congestion"] = df["weather_enc"] * df["congestion_level"]
    # Distance per hour (speed proxy — higher = more risk)
    df["distance_per_hour"]  = (df["distance_km"] / df["planned_duration_h"].replace(0, 1)).fillna(0)
    # High congestion flag
    df["high_congestion"]    = (df["congestion_level"] > 0.7).astype(int)
    # Severe weather flag (snow=3, storm=4 in encoded values)
    df["severe_weather"]     = df["weather_condition"].isin(["snow", "storm"]).astype(int)
    # Weight category (heavy parcels may cause delay)
    df["heavy_parcel"]       = (df["weight_kg"] > 30).astype(int)
    # One-hot weather features for finer granularity
    for w in ["rain", "fog", "snow", "storm"]:
        df[f"weather_{w}"] = (df["weather_condition"] == w).astype(int)
    # Congestion squared (non-linear effect)
    df["congestion_sq"]      = df["congestion_level"] ** 2
    # Weather risk score (continuous proxy for delay_factor)
    weather_risk_map = {"clear": 0.0, "rain": 0.3, "fog": 0.2, "snow": 0.8, "storm": 1.5}
    df["weather_risk_score"] = df["weather_condition"].map(weather_risk_map).fillna(0.0)
    # Composite risk score = weather_risk * (1 + congestion)
    df["composite_risk"]     = df["weather_risk_score"] * (1 + df["congestion_level"])

    feature_cols = [
        "route_enc", "weather_enc", "client_enc", "hub_enc",
        "congestion_level", "distance_km", "weight_kg",
        "hour_of_day", "day_of_week", "is_weekend", "planned_duration_h",
        "weather_congestion", "distance_per_hour", "high_congestion",
        "severe_weather", "heavy_parcel",
        "weather_rain", "weather_fog", "weather_snow", "weather_storm",
        "congestion_sq", "weather_risk_score", "composite_risk",
    ]

    X = df[feature_cols].fillna(0).values
    y = df["risk_label"].values

    encoders = {
        "route":   le_route,
        "weather": le_weather,
        "client":  le_client,
        "hub":     le_hub,
    }
    return X, y, feature_cols, encoders

# ── Entraînement ───────────────────────────────────────────────
def train(X, y):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # ── Class balancing via sample_weight ──
    class_counts = Counter(y_train)
    total = len(y_train)
    n_classes = len(class_counts)
    # Compute balanced weights: total / (n_classes * count_per_class)
    class_weight = {c: total / (n_classes * cnt) for c, cnt in class_counts.items()}
    sample_weights = np.array([class_weight[label] for label in y_train])
    print(f"   Class weights: {class_weight}")

    model = xgb.XGBClassifier(
        n_estimators=800,
        max_depth=5,
        learning_rate=0.03,
        subsample=0.85,
        colsample_bytree=0.75,
        min_child_weight=3,
        gamma=0.1,
        reg_alpha=0.1,
        reg_lambda=1.5,
        eval_metric="mlogloss",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(
        X_train, y_train,
        sample_weight=sample_weights,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    y_pred = model.predict(X_test)
    acc    = accuracy_score(y_test, y_pred)
    f1_w   = f1_score(y_test, y_pred, average="weighted")
    report = classification_report(
        y_test, y_pred,
        target_names=["Low","Medium","High"],
        output_dict=True
    )
    print(f"\n📊 Accuracy : {acc:.3f}")
    print(f"📊 Weighted F1 : {f1_w:.3f}")
    print(classification_report(y_test, y_pred, target_names=["Low","Medium","High"]))

    # Cross-validation
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(model, X, y, cv=cv, scoring="f1_weighted")
    print(f"📊 CV F1 (5-fold) : {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")

    # Feature importance
    feat_imp = model.feature_importances_
    print("\n📊 Top features :")
    for idx in np.argsort(feat_imp)[::-1][:8]:
        print(f"   {idx}: importance={feat_imp[idx]:.3f}")

    return model, {
        "accuracy": acc,
        "f1_weighted": f1_w,
        "cv_f1_mean": float(cv_scores.mean()),
        "cv_f1_std": float(cv_scores.std()),
        "report": report,
    }

# ── Sauvegarde ─────────────────────────────────────────────────
def save_artifacts(model, feature_cols, encoders, metrics):
    os.makedirs("ml", exist_ok=True)

    with open("ml/delay_model.pkl", "wb") as f:
        pickle.dump({"model": model, "encoders": encoders}, f)

    with open("ml/feature_names.json", "w") as f:
        json.dump(feature_cols, f)

    metrics["trained_at"] = datetime.now().isoformat()
    metrics["risk_thresholds"] = {
        "low_max_min": DELAY_LOW_MAX,
        "medium_max_min": DELAY_MEDIUM_MAX,
    }
    with open("ml/metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print("✅ Modèle sauvegardé → ml/delay_model.pkl")


if __name__ == "__main__":
    print("🤖 Entraînement du modèle ML LogiStream...")
    df           = load_data()
    print(f"   Dataset : {len(df)} colis | Distribution risques :")
    print(df["risk_label"].value_counts().rename({0:"Low",1:"Medium",2:"High"}))
    X, y, cols, encoders = build_features(df)
    model, metrics       = train(X, y)
    save_artifacts(model, cols, encoders, metrics)
    print("🚀 Entraînement terminé !")
