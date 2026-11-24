import os
import json
import pandas as pd
from datetime import datetime

# Robust import handling across different Evidently AI versions
try:
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset
except (ImportError, ModuleNotFoundError):
    try:
        from evidently.legacy.report import Report
        from evidently.legacy.metric_preset import DataDriftPreset
    except (ImportError, ModuleNotFoundError):
        from evidently import Report
        from evidently.presets import DataDriftPreset

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
REF_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "train_reference.csv")
STREAM_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "production_stream.csv")
REPORTS_DIR = os.path.join(BASE_DIR, "monitoring", "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

def generate_drift_report(n_samples: int = 2000, inject_drift: bool = False):
    """
    Compares the reference training baseline against incoming production samples
    using Evidently AI and outputs an interactive HTML report and JSON summary.
    """
    print("=" * 70)
    print("RUNNING EVIDENTLY AI DATA DRIFT MONITORING")
    print("=" * 70)

    # 1. Load Reference (Baseline) Data
    print(f"1. Loading reference baseline ({REF_DATA_PATH})...")
    if not os.path.exists(REF_DATA_PATH):
        print(f"Reference dataset not found locally. Auto-pulling from Cloudflare R2...")
        from backend.pipeline.pull_data import download_data_from_r2
        download_data_from_r2()

    df_ref = pd.read_csv(REF_DATA_PATH)
    reference_sample = df_ref.sample(n=min(5000, len(df_ref)), random_state=42)

    # 2. Load Production Stream Data
    print(f"2. Loading production stream batch ({STREAM_DATA_PATH})...")
    if not os.path.exists(STREAM_DATA_PATH):
        print(f"Stream dataset not found locally. Auto-pulling from Cloudflare R2...")
        from backend.pipeline.pull_data import download_data_from_r2
        download_data_from_r2()

    df_stream = pd.read_csv(STREAM_DATA_PATH)
    current_sample = df_stream.sample(n=min(n_samples, len(df_stream)), random_state=123).copy()

    # 3. Optional: Inject artificial domain drift to test alert triggers
    if inject_drift:
        print("⚠️  Injecting realistic domain shift across key terrain features...")
        current_sample["Elevation"] = current_sample["Elevation"] + 500.0
        current_sample["Horizontal_Distance_To_Hydrology"] = current_sample["Horizontal_Distance_To_Hydrology"] * 1.5
        current_sample["Horizontal_Distance_To_Roadways"] = current_sample["Horizontal_Distance_To_Roadways"] * 1.6
        current_sample["Horizontal_Distance_To_Fire_Points"] = current_sample["Horizontal_Distance_To_Fire_Points"] * 1.5
        current_sample["Slope"] = current_sample["Slope"] + 10.0
        current_sample["Hillshade_Noon"] = (current_sample["Hillshade_Noon"] * 0.8).clip(0, 255)

    # Key continuous features to evaluate
    features_to_monitor = [
        "Elevation", "Aspect", "Slope",
        "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
        "Horizontal_Distance_To_Roadways", "Horizontal_Distance_To_Fire_Points",
        "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm"
    ]

    ref_subset = reference_sample[features_to_monitor]
    curr_subset = current_sample[features_to_monitor]

    # 4. Generate Evidently Drift Report
    print("3. Computing statistical drift tests across continuous features...")
    # Trigger overall dataset drift when >= 30% of features drift
    try:
        report = Report(metrics=[DataDriftPreset(drift_share=0.3)])
    except TypeError:
        report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=ref_subset, current_data=curr_subset)

    # 5. Save Output Artifacts
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = "drift_detected" if inject_drift else "normal_stream"
    
    html_filename = f"{prefix}_report_{timestamp}.html"
    html_path = os.path.join(REPORTS_DIR, html_filename)
    report.save_html(html_path)

    json_filename = f"{prefix}_summary_{timestamp}.json"
    json_path = os.path.join(REPORTS_DIR, json_filename)
    try:
        report.save_json(json_path)
    except Exception:
        pass

    latest_html = os.path.join(REPORTS_DIR, "latest_drift_report.html")
    report.save_html(latest_html)

    print("\n✅ Drift Monitoring Complete!")
    print(f"   - Interactive HTML Report: {html_path}")
    print(f"   - Latest Report View:     {latest_html}")
    print("=" * 70)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Evidently AI Drift Monitor")
    parser.add_argument("--drift", action="store_true", help="Inject intentional domain shift to verify alert")
    parser.add_argument("--samples", type=int, default=2000, help="Number of stream rows to evaluate")
    args = parser.parse_args()

    generate_drift_report(n_samples=args.samples, inject_drift=args.drift)
