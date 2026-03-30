#!/usr/bin/env python3
"""Synthetic data generator for Twingate connector analytics fixtures.

Generates realistic sample events for testing and development:
  - network_access.ndjson   (established/closed connection events)
  - dns_filtering.ndjson    (DNS query events)
  - audit_log.ndjson        (admin audit trail events)
  - connector_stdout.log    (raw ANALYTICS-prefixed lines with noise)

Usage:
    python generate.py [--events N] [--output-dir DIR] [--seed SEED]

Defaults: 100 events per file, output to ./fixtures/, seed=42 for reproducibility.
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Data pools
# ---------------------------------------------------------------------------

USERS: list[dict[str, str]] = [
    {"email": f"{name}@corp.example.com", "id": str(113000 + i)}
    for i, name in enumerate(
        [
            "alice", "bob", "carol", "dave", "eve", "frank", "grace", "hank",
            "iris", "jack", "karen", "leo", "mona", "nick", "olivia", "pat",
            "quinn", "rosa", "sam", "tara",
        ],
        start=1,
    )
]  # 20 users, IDs 113001-113020

RESOURCES: list[dict[str, str]] = [
    {"address": "app.website.com",       "applied_rule": "*.website.com",        "id": "2255001"},
    {"address": "api.internal.io",       "applied_rule": "*.internal.io",        "id": "2255002"},
    {"address": "db.prod.internal.io",   "applied_rule": "*.internal.io",        "id": "2255003"},
    {"address": "wiki.corp.example.com", "applied_rule": "*.corp.example.com",   "id": "2255004"},
    {"address": "gitlab.corp.example.com", "applied_rule": "*.corp.example.com", "id": "2255005"},
    {"address": "jenkins.corp.example.com", "applied_rule": "*.corp.example.com", "id": "2255006"},
    {"address": "grafana.monitoring.io", "applied_rule": "*.monitoring.io",      "id": "2255007"},
    {"address": "vault.secure.io",       "applied_rule": "*.secure.io",          "id": "2255008"},
    {"address": "10.0.1.50",             "applied_rule": "10.0.1.0/24",          "id": "2255009"},
    {"address": "10.0.2.100",            "applied_rule": "10.0.2.0/24",          "id": "2255010"},
    {"address": "10.0.3.25",             "applied_rule": "10.0.3.0/24",          "id": "2255011"},
    {"address": "192.168.10.5",          "applied_rule": "192.168.10.0/24",      "id": "2255012"},
    {"address": "k8s.cluster.internal",  "applied_rule": "*.cluster.internal",   "id": "2255013"},
    {"address": "redis.cache.internal",  "applied_rule": "*.cache.internal",     "id": "2255014"},
    {"address": "postgres.db.internal",  "applied_rule": "*.db.internal",        "id": "2255015"},
]  # 15 resources

CONNECTORS: list[dict[str, str]] = [
    {"id": "84001", "name": "nondescript-caterpillar"},
    {"id": "84002", "name": "ambitious-flamingo"},
    {"id": "84003", "name": "resilient-pangolin"},
    {"id": "84004", "name": "whimsical-octopus"},
    {"id": "84005", "name": "steadfast-wolverine"},
]  # 5 connectors

REMOTE_NETWORKS: list[dict[str, str]] = [
    {"id": "6901", "name": "AWS Network"},
    {"id": "6902", "name": "GCP Network"},
    {"id": "6903", "name": "On-Prem DC"},
]  # 3 remote networks

LOCATIONS: list[dict[str, str | float]] = [
    {"city": "Seattle",       "country": "US", "lat": 47.6062,  "lon": -122.3321, "region": "WA"},
    {"city": "New York",      "country": "US", "lat": 40.7128,  "lon": -74.0060,  "region": "NY"},
    {"city": "San Francisco", "country": "US", "lat": 37.7749,  "lon": -122.4194, "region": "CA"},
    {"city": "Chicago",       "country": "US", "lat": 41.8781,  "lon": -87.6298,  "region": "IL"},
    {"city": "Austin",        "country": "US", "lat": 30.2672,  "lon": -97.7431,  "region": "TX"},
    {"city": "London",        "country": "GB", "lat": 51.5074,  "lon": -0.1278,   "region": "ENG"},
    {"city": "Berlin",        "country": "DE", "lat": 52.5200,  "lon": 13.4050,   "region": "BE"},
    {"city": "Tokyo",         "country": "JP", "lat": 35.6762,  "lon": 139.6503,  "region": "13"},
    {"city": "Sydney",        "country": "AU", "lat": -33.8688, "lon": 151.2093,  "region": "NSW"},
    {"city": "Toronto",       "country": "CA", "lat": 43.6532,  "lon": -79.3832,  "region": "ON"},
]  # 10 locations

DNS_DOMAINS: list[dict[str, str]] = [
    # default (passthrough)
    {"domain": "docs.google.com",             "root": "google.com",       "status": "default"},
    {"domain": "api.github.com",              "root": "github.com",       "status": "default"},
    {"domain": "cdn.jsdelivr.net",            "root": "jsdelivr.net",     "status": "default"},
    {"domain": "registry.npmjs.org",          "root": "npmjs.org",        "status": "default"},
    {"domain": "pypi.org",                    "root": "pypi.org",         "status": "default"},
    {"domain": "slack.com",                   "root": "slack.com",        "status": "default"},
    {"domain": "zoom.us",                     "root": "zoom.us",          "status": "default"},
    {"domain": "app.datadoghq.com",           "root": "datadoghq.com",   "status": "default"},
    {"domain": "console.aws.amazon.com",      "root": "amazon.com",      "status": "default"},
    {"domain": "portal.azure.com",            "root": "azure.com",       "status": "default"},
    {"domain": "mail.google.com",             "root": "google.com",      "status": "default"},
    {"domain": "drive.google.com",            "root": "google.com",      "status": "default"},
    {"domain": "outlook.office365.com",       "root": "office365.com",   "status": "default"},
    {"domain": "notion.so",                   "root": "notion.so",       "status": "default"},
    {"domain": "linear.app",                  "root": "linear.app",      "status": "default"},
    # blocked
    {"domain": "malware.bad-site.com",        "root": "bad-site.com",    "status": "blocked"},
    {"domain": "phishing.evil-domain.net",    "root": "evil-domain.net", "status": "blocked"},
    {"domain": "c2.botnet.ru",               "root": "botnet.ru",       "status": "blocked"},
    {"domain": "tracker.adnetwork.io",        "root": "adnetwork.io",    "status": "blocked"},
    {"domain": "crypto-miner.sketchy.xyz",    "root": "sketchy.xyz",     "status": "blocked"},
    {"domain": "exfil.darkweb.onion.ws",      "root": "onion.ws",        "status": "blocked"},
    {"domain": "keylogger.malicious.cc",      "root": "malicious.cc",    "status": "blocked"},
    {"domain": "redirect.spam-link.info",     "root": "spam-link.info",  "status": "blocked"},
    {"domain": "exploit-kit.crimeware.biz",   "root": "crimeware.biz",   "status": "blocked"},
    {"domain": "dropper.ransomware.top",      "root": "ransomware.top",  "status": "blocked"},
    # allowed (explicit allowlist)
    {"domain": "vpn.partner-corp.com",        "root": "partner-corp.com",  "status": "allowed"},
    {"domain": "api.vendor-saas.io",          "root": "vendor-saas.io",    "status": "allowed"},
    {"domain": "sftp.supplier.net",           "root": "supplier.net",      "status": "allowed"},
    {"domain": "portal.contractor-tools.com", "root": "contractor-tools.com", "status": "allowed"},
    {"domain": "webhook.integration.dev",     "root": "integration.dev",   "status": "allowed"},
]  # 30 DNS domains

BLOCK_REASONS: list[str] = [
    "dns_security_category:malware",
    "dns_security_category:phishing",
    "dns_security_category:command_and_control",
    "dns_security_category:cryptomining",
    "custom_blocklist:corporate_policy",
]

AUDIT_ACTIONS: list[str] = [
    "create", "update", "delete", "login", "logout", "enable", "disable", "revoke",
]  # 8 action types

AUDIT_TARGET_TYPES: list[str] = [
    "resource", "group", "user", "connector", "remote_network", "policy",
]  # 6 target types

AUDIT_TARGET_NAMES: dict[str, list[str]] = {
    "resource":       ["app.website.com", "api.internal.io", "db.prod.internal.io", "vault.secure.io"],
    "group":          ["Engineering", "DevOps", "Security", "All Users", "Contractors"],
    "user":           ["alice@corp.example.com", "bob@corp.example.com", "carol@corp.example.com"],
    "connector":      ["nondescript-caterpillar", "ambitious-flamingo", "resilient-pangolin"],
    "remote_network": ["AWS Network", "GCP Network", "On-Prem DC"],
    "policy":         ["Default Policy", "Strict MFA Policy", "Contractor Access Policy"],
}

AUDIT_CHANGE_FIELDS: dict[str, list[tuple[str, str]]] = {
    "resource":       [("address", "old.host.com"), ("protocols", "TCP")],
    "group":          [("name", "Old Group Name"), ("users_count", "5")],
    "user":           [("role", "member"), ("state", "active")],
    "connector":      [("name", "old-connector-name"), ("state", "alive")],
    "remote_network": [("name", "Old Network"), ("location", "us-east-1")],
    "policy":         [("mfa_required", "false"), ("session_ttl", "3600")],
}

DEVICE_NAMES: list[str] = [
    "Bens-MacBook-Pro", "work-laptop-01", "dev-desktop", "mobile-iphone",
    "linux-workstation", "windows-pc", "chromebook-7", "ipad-air",
]

NOISE_LINES: list[str] = [
    "INFO Connected to relay relay1.example.com:443",
    "INFO Connected to relay relay2.example.com:443",
    "INFO Tunnel established to controller.twingate.com",
    "DEBUG Heartbeat sent, latency=12ms",
    "DEBUG Refreshing peer list",
    "INFO Connector version 2024.192.0 starting",
    "WARN DNS resolution slow for app.website.com (took 340ms)",
    "INFO Successfully authenticated with controller",
    "DEBUG Certificate renewal scheduled in 23h",
    "ERROR Transient connection reset to relay3.example.com, retrying",
    "INFO Peer refresh complete, 3 active peers",
    "DEBUG CBCT token refreshed successfully",
]

# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def _rand_ip() -> str:
    """Generate a random public-ish IP address."""
    return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def _rand_private_ip() -> str:
    """Generate a random 10.x.x.x private IP."""
    return f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def _rand_connection_id() -> str:
    """Generate a connection ID in the format '{16 hex chars}-{2 digit int}'."""
    hex_part = uuid.uuid4().hex[:16]
    seq = random.randint(0, 99)
    return f"{hex_part}-{seq:02d}"


def _make_location_str(loc: dict[str, str | float]) -> str:
    """Build the double-encoded location JSON string."""
    inner = {"geoip": {
        "city": loc["city"],
        "country": loc["country"],
        "lat": loc["lat"],
        "lon": loc["lon"],
        "region": loc["region"],
    }}
    return json.dumps(inner, separators=(",", ":"))


def _iso_ts(dt: datetime) -> str:
    """Format a datetime as ISO 8601 with timezone."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def generate_network_access_event(base_time: datetime, offset_ms: int) -> dict:
    """Generate a single network_access event (established or closed)."""
    event_type = random.choice(["established_connection", "closed_connection"])
    ts = int((base_time.timestamp() + offset_ms / 1000) * 1000)

    user = random.choice(USERS)
    resource = random.choice(RESOURCES)
    connector = random.choice(CONNECTORS)
    network = random.choice(REMOTE_NETWORKS)
    location = random.choice(LOCATIONS)

    connection: dict = {
        "id": _rand_connection_id(),
        "client_ip": _rand_ip(),
        "resource_ip": _rand_private_ip(),
        "resource_port": random.choice([22, 80, 443, 3306, 5432, 6379, 8080, 8443, 3389]),
        "protocol": random.choice(["tcp", "udp"]),
        "rx": random.randint(500, 5_000_000),
        "tx": random.randint(200, 2_000_000),
        "tunnel_path": random.choice(["direct", "relayed"]),
        "tunnel_proto": random.choice(["quic/udp", "tcp"]),
        "cbct_freshness": random.randint(-200, 200),
    }

    if event_type == "closed_connection":
        connection["duration"] = random.randint(100, 30_000_000)  # ms

    event = {
        "event_type": event_type,
        "timestamp": ts,
        "user": {"email": user["email"], "id": user["id"]},
        "device": {"id": str(200000 + random.randint(1, 999))},
        "resource": {
            "address": resource["address"],
            "applied_rule": resource["applied_rule"],
            "id": resource["id"],
        },
        "connection": connection,
        "connector": {"id": connector["id"], "name": connector["name"]},
        "remote_network": {"id": network["id"], "name": network["name"]},
        "location": _make_location_str(location),
        "relays": [],
    }
    return event


def generate_dns_filtering_event(base_time: datetime, offset_ms: int) -> dict:
    """Generate a single dns_filtering event."""
    entry = random.choice(DNS_DOMAINS)
    ts = base_time + timedelta(milliseconds=offset_ms)

    reasons: list[str] = []
    if entry["status"] == "blocked":
        reasons = [random.choice(BLOCK_REASONS)]

    event = {
        "event_type": "dns_filtering",
        "event": {
            "version": 1,
            "time": _iso_ts(ts),
            "domain": entry["domain"],
            "root": entry["root"],
            "device": {
                "id": f"RGV2aWNlOj{random.randint(100000, 999999)}",
                "name": random.choice(DEVICE_NAMES),
                "model": None,
            },
            "connection": {
                "client_ip": _rand_ip(),
                "protocol": random.choice(["DNS", "DNS-over-HTTPS"]),
            },
            "status": entry["status"],
            "reasons": reasons,
        },
    }
    return event


def generate_audit_log_event(base_time: datetime, offset_ms: int) -> dict:
    """Generate a single audit_log event."""
    ts = base_time + timedelta(milliseconds=offset_ms)
    action = random.choice(AUDIT_ACTIONS)
    target_type = random.choice(AUDIT_TARGET_TYPES)
    target_name = random.choice(AUDIT_TARGET_NAMES[target_type])
    actor = random.choice(USERS[:5])  # admins are the first 5 users

    event: dict = {
        "timestamp": _iso_ts(ts),
        "action": action,
        "actor": {"email": actor["email"], "id": actor["id"]},
        "target": {
            "type": target_type,
            "name": target_name,
            "id": str(random.randint(100000, 999999)),
        },
    }

    if action == "update":
        field_name, old_val = random.choice(AUDIT_CHANGE_FIELDS[target_type])
        event["changes"] = {
            field_name: {
                "before": old_val,
                "after": f"new-{old_val}" if isinstance(old_val, str) else str(int(old_val) + 1),
            }
        }

    return event


def generate_connector_stdout_line(event: dict) -> str:
    """Format a network_access event as an ANALYTICS-prefixed stdout line."""
    return f"ANALYTICS {json.dumps(event, separators=(',', ':'))}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Generate fixture files from synthetic Twingate analytics data."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic Twingate connector analytics fixtures.",
    )
    parser.add_argument(
        "--events", type=int, default=100,
        help="Number of events to generate per file (default: 100)",
    )
    parser.add_argument(
        "--output-dir", type=str, default="./fixtures",
        help="Output directory for fixture files (default: ./fixtures)",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    args = parser.parse_args()

    random.seed(args.seed)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    n = args.events
    base_time = datetime(2026, 3, 27, 10, 0, 0, tzinfo=timezone.utc)

    # --- network_access.ndjson ---
    na_path = output_dir / "network_access.ndjson"
    na_events: list[dict] = []
    with open(na_path, "w", encoding="utf-8") as f:
        for i in range(n):
            offset = i * random.randint(500, 5000)
            evt = generate_network_access_event(base_time, offset)
            na_events.append(evt)
            f.write(json.dumps(evt, separators=(",", ":")) + "\n")
    print(f"  Wrote {n} events to {na_path}")

    # --- dns_filtering.ndjson ---
    dns_path = output_dir / "dns_filtering.ndjson"
    with open(dns_path, "w", encoding="utf-8") as f:
        for i in range(n):
            offset = i * random.randint(300, 3000)
            evt = generate_dns_filtering_event(base_time, offset)
            f.write(json.dumps(evt, separators=(",", ":")) + "\n")
    print(f"  Wrote {n} events to {dns_path}")

    # --- audit_log.ndjson ---
    audit_path = output_dir / "audit_log.ndjson"
    with open(audit_path, "w", encoding="utf-8") as f:
        for i in range(n):
            offset = i * random.randint(10000, 60000)
            evt = generate_audit_log_event(base_time, offset)
            f.write(json.dumps(evt, separators=(",", ":")) + "\n")
    print(f"  Wrote {n} events to {audit_path}")

    # --- connector_stdout.log ---
    # Mix network_access events (as ANALYTICS lines) with ~20% noise
    stdout_path = output_dir / "connector_stdout.log"
    total_stdout_lines = n
    noise_count = max(1, int(total_stdout_lines * 0.2))
    analytics_count = total_stdout_lines - noise_count

    lines: list[str] = []
    for i in range(analytics_count):
        offset = i * random.randint(500, 5000)
        evt = generate_network_access_event(base_time, offset)
        lines.append(generate_connector_stdout_line(evt))

    # Insert noise lines at random positions
    for _ in range(noise_count):
        noise = random.choice(NOISE_LINES)
        pos = random.randint(0, len(lines))
        lines.insert(pos, noise)

    with open(stdout_path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
    print(f"  Wrote {len(lines)} lines ({analytics_count} ANALYTICS + {noise_count} noise) to {stdout_path}")

    print(f"\nDone. All fixtures written to {output_dir.resolve()}")


if __name__ == "__main__":
    main()
