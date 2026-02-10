scenarios = {
    "baseline": 1.0,
    "dry": 0.9,
    "wet": 1.1,
    "future": 0.85
}

for name, factor in scenarios.items():
    df[f"score_{name}"] = df["score"] * factor
