"""Regenerates grafana/provisioning/dashboards/analytics.json from tools/panels.py.

Run from the repo root:  python tools/build_dashboard.py
CI fails if the committed file differs (anti-drift, same rationale as the
contract test's field-level assertion).
"""
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.panels import DS_PG, DS_PROM, PANELS  # noqa: E402

OUT = pathlib.Path(__file__).resolve().parent.parent / \
    "grafana" / "provisioning" / "dashboards" / "analytics.json"

PG_OPTIONS = {"legend": {"displayMode": "list", "placement": "bottom"},
              "tooltip": {"mode": "multi"}}


def build() -> dict:
    panels = []
    for p in PANELS:
        panel = {
            "id": p["id"],
            "title": p["title"],
            "description": p.get("description", ""),
            "type": p["type"],
            "datasource": DS_PG if p["ds"] == "postgres" else DS_PROM,
            "gridPos": p["gridPos"],
            "fieldConfig": {"defaults": {}, "overrides": []},
            "options": PG_OPTIONS if p["type"] == "timeseries" else {},
            "targets": [],
        }
        if p["ds"] == "postgres":
            panel["targets"].append({
                "refId": "A",
                "format": p["format"],
                "rawSql": p["sql"],
                "datasource": DS_PG,
            })
        else:
            panel["targets"].append({
                "refId": "A",
                "expr": p["expr"],
                "legendFormat": p.get("legend", ""),
                "datasource": DS_PROM,
            })
        panels.append(panel)

    return {
        "annotations": {"list": []},
        "editable": True,
        "graphTooltip": 1,
        "refresh": "1m",
        "schemaVersion": 39,
        "tags": ["analytics", "provisioned"],
        "time": {"from": "now-3h", "to": "now"},
        "timezone": "browser",
        "title": "Streaming Analytics",
        "uid": "streaming-analytics",
        "version": 1,
        "panels": panels,
    }


def main() -> None:
    dashboard = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(dashboard, indent=1) + "\n")
    print(f"wrote {OUT} ({len(dashboard['panels'])} panels)")


if __name__ == "__main__":
    main()
