#!/usr/bin/env python3
"""
peerdb_manager.py
=================
Manages PeerDB peers and mirrors via the PeerDB REST/gRPC-gateway API.

Commands
--------
  apply  <yaml_file>          — Create/update peers or mirrors from YAML definition
  status                      — Print status of all mirrors
  pause  <mirror_name>        — Pause a running mirror
  resume <mirror_name>        — Resume a paused mirror
  drop   <mirror_name>        — Drop mirror (WARNING: irreversible)
  logs   <mirror_name>        — Tail recent mirror logs
  validate                    — Test peer connections without creating mirrors

Usage
-----
  python peerdb_manager.py apply mirrors/peers.yaml
  python peerdb_manager.py apply mirrors/dims_cdc.yaml
  python peerdb_manager.py apply mirrors/facts_cdc.yaml
  python peerdb_manager.py status
  python peerdb_manager.py pause vipani_facts_cdc
  python peerdb_manager.py resume vipani_facts_cdc
  python peerdb_manager.py validate

Environment
-----------
  PEERDB_API_URL   (default: http://localhost:8085)
  PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
  CH_HOST, CH_PORT, CH_DATABASE, CH_USER, CH_PASSWORD
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import requests
import yaml

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

PEERDB_URL = os.getenv("PEERDB_API_URL", "http://localhost:8085")


# =============================================================================
# HTTP helpers
# =============================================================================

def _get(path: str) -> dict:
    r = requests.get(f"{PEERDB_URL}{path}", timeout=15)
    r.raise_for_status()
    return r.json()


def _post(path: str, payload: dict) -> dict:
    r = requests.post(
        f"{PEERDB_URL}{path}",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"POST {path} → {r.status_code}: {r.text[:500]}")
    return r.json()


def _env_sub(value: Any) -> Any:
    """Recursively substitute ${VAR} and ${VAR:-default} in strings."""
    if isinstance(value, str):
        def _replace(m: re.Match) -> str:
            var, _, default = m.group(1).partition(":-")
            return os.getenv(var, default)
        return re.sub(r"\$\{([^}]+)\}", _replace, value)
    if isinstance(value, dict):
        return {k: _env_sub(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_env_sub(item) for item in value]
    return value


# =============================================================================
# Peer management
# =============================================================================

PEER_TYPE_MAP = {
    "POSTGRES":   1,
    "CLICKHOUSE": 8,
    "SNOWFLAKE":  2,
    "BIGQUERY":   3,
}


def _create_peer(peer: dict) -> None:
    peer_type = peer["type"].upper()
    type_int = PEER_TYPE_MAP.get(peer_type)
    if type_int is None:
        raise ValueError(f"Unsupported peer type: {peer_type}")

    # Config key follows PeerDB convention: postgres_config / clickhouse_config …
    config_key = peer_type.lower() + "_config"
    config = _env_sub(peer.get(config_key, {}))

    # Coerce numeric fields
    if "port" in config:
        config["port"] = int(config["port"])

    payload = {
        "peer": {
            "name": peer["name"],
            "type": type_int,
            config_key: config,
        }
    }
    try:
        result = _post("/v1/peers/create", payload)
        print(f"  [peer] Created/updated: {peer['name']} ({peer_type}) → {result}")
    except RuntimeError as exc:
        if "already exists" in str(exc).lower():
            print(f"  [peer] Already exists: {peer['name']} — skipping.")
        else:
            raise


# =============================================================================
# Mirror management
# =============================================================================

def _create_mirror(mirror: dict) -> None:
    mode = mirror.get("mode", "cdc").lower()
    name = mirror["name"]

    if mode == "cdc":
        table_mappings = [
            {
                "source_table_identifier":      m["source_table_identifier"],
                "destination_table_identifier": m["destination_table_identifier"],
                "column_exclude_list":          m.get("column_exclude_list", []),
                "soft_delete":                  m.get("soft_delete", False),
            }
            for m in mirror.get("table_mappings", [])
        ]

        payload = {
            "connection_configs": {
                "flow_job_name":                    name,
                "source":                           {"name": mirror["source_peer"]},
                "destination":                      {"name": mirror["destination_peer"]},
                "table_mappings":                   table_mappings,
                "do_initial_snapshot":              mirror.get("do_initial_snapshot", True),
                "snapshot_num_rows_per_partition":  mirror.get("snapshot_num_rows_per_partition", 100000),
                "snapshot_num_tables_in_parallel":  mirror.get("snapshot_num_tables_in_parallel", 4),
                "max_batch_size":                   mirror.get("max_batch_size", 1000),
                "idle_timeout_seconds":             mirror.get("idle_timeout_seconds", 30),
                "publication_name":                 mirror.get("publication_name", ""),
                "replication_slot_name":            mirror.get("replication_slot_name", ""),
            }
        }
    else:
        raise ValueError(f"Mirror mode '{mode}' not supported yet (only 'cdc').")

    try:
        result = _post("/v1/flows/cdc/create", payload)
        print(f"  [mirror] Created: {name} ({mode}) → {result}")
    except RuntimeError as exc:
        if "already exists" in str(exc).lower():
            print(f"  [mirror] Already exists: {name} — skipping.")
        else:
            raise


# =============================================================================
# Commands
# =============================================================================

def cmd_apply(yaml_file: str) -> None:
    path = Path(yaml_file)
    if not path.exists():
        print(f"ERROR: File not found: {yaml_file}")
        sys.exit(1)

    data = yaml.safe_load(path.read_text())

    if "peers" in data:
        print(f"[apply] Registering {len(data['peers'])} peer(s)…")
        for peer in data["peers"]:
            _create_peer(peer)

    if "mirror" in data:
        print(f"[apply] Creating mirror: {data['mirror']['name']}…")
        _create_mirror(data["mirror"])

    print("[apply] Done.")


def cmd_status() -> None:
    try:
        flows = _get("/v1/flows")
    except Exception as exc:
        print(f"ERROR: Could not reach PeerDB at {PEERDB_URL} — {exc}")
        sys.exit(1)

    items = flows.get("flows", [])
    if not items:
        print("  No mirrors found.")
        return

    print(f"\n{'Mirror':<35} {'Status':<15} {'Source':<25} {'Destination':<25}")
    print("─" * 105)
    for f in items:
        name   = f.get("name", "?")
        status = f.get("status", "?")
        src    = f.get("source_peer", {}).get("name", "?")
        dst    = f.get("destination_peer", {}).get("name", "?")
        print(f"  {name:<33} {status:<15} {src:<25} {dst:<25}")
    print()


def cmd_pause(mirror_name: str) -> None:
    result = _post("/v1/flows/state-change", {
        "flow_job_name": mirror_name,
        "requested_flow_state": "STATE_PAUSED",
    })
    print(f"Paused {mirror_name}: {result}")


def cmd_resume(mirror_name: str) -> None:
    result = _post("/v1/flows/state-change", {
        "flow_job_name": mirror_name,
        "requested_flow_state": "STATE_RUNNING",
    })
    print(f"Resumed {mirror_name}: {result}")


def cmd_drop(mirror_name: str) -> None:
    confirm = input(f"Drop mirror '{mirror_name}'? This is irreversible. Type YES to confirm: ")
    if confirm.strip() != "YES":
        print("Aborted.")
        return
    result = _post("/v1/mirrors/drop", {"flow_job_name": mirror_name})
    print(f"Dropped {mirror_name}: {result}")


def cmd_validate() -> None:
    """Test connections for peers defined in mirrors/peers.yaml."""
    peers_file = Path(__file__).parent.parent / "mirrors" / "peers.yaml"
    if not peers_file.exists():
        print("ERROR: mirrors/peers.yaml not found.")
        sys.exit(1)
    data = yaml.safe_load(peers_file.read_text())
    for peer in data.get("peers", []):
        peer_type = peer["type"].upper()
        config_key = peer_type.lower() + "_config"
        config = _env_sub(peer.get(config_key, {}))
        if "port" in config:
            config["port"] = int(config["port"])
        payload = {
            "peer": {
                "name": peer["name"],
                "type": PEER_TYPE_MAP[peer_type],
                config_key: config,
            }
        }
        try:
            result = _post("/v1/peers/validate", payload)
            ok = result.get("status", "?")
            msg = result.get("message", "")
            print(f"  {peer['name']}: {ok}  {msg}")
        except Exception as exc:
            print(f"  {peer['name']}: ERROR — {exc}")


# =============================================================================
# CLI
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="PeerDB mirror manager")
    sub = ap.add_subparsers(dest="command", required=True)

    p_apply    = sub.add_parser("apply",    help="Apply a YAML peer/mirror definition")
    p_apply.add_argument("yaml_file")

    sub.add_parser("status",   help="Show all mirror statuses")
    sub.add_parser("validate", help="Validate peer connections")

    p_pause  = sub.add_parser("pause",  help="Pause a mirror")
    p_pause.add_argument("mirror_name")

    p_resume = sub.add_parser("resume", help="Resume a mirror")
    p_resume.add_argument("mirror_name")

    p_drop   = sub.add_parser("drop",   help="Drop a mirror")
    p_drop.add_argument("mirror_name")

    args = ap.parse_args()

    if args.command == "apply":
        cmd_apply(args.yaml_file)
    elif args.command == "status":
        cmd_status()
    elif args.command == "validate":
        cmd_validate()
    elif args.command == "pause":
        cmd_pause(args.mirror_name)
    elif args.command == "resume":
        cmd_resume(args.mirror_name)
    elif args.command == "drop":
        cmd_drop(args.mirror_name)


if __name__ == "__main__":
    main()
