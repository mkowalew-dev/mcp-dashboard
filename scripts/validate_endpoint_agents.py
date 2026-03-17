#!/usr/bin/env python3
"""
Validate Site Health endpoint-agents logic against the live API.
Usage: python scripts/validate_endpoint_agents.py [--url URL]
Default URL: https://mcpdash.retaildemo.net/api/data?window=24h
"""
import argparse
import json
import sys
from urllib.request import urlopen

BASE_URL = "https://mcpdash.retaildemo.net/api/data?window=24h"


def first_location_token(s):
    if s is None or s == "":
        return ""
    return str(s).strip().split(",")[0].strip().lower()


def get_site_key(agent):
    return (agent.get("loc") or agent.get("city") or agent.get("name") or agent.get("id") or "").__str__().strip() or (
        "agent-" + str(agent.get("id", ""))
    )


def build_sites(data):
    agents = [a for a in (data.get("ALL_AGENTS") or []) if a.get("type") == "enterprise"]
    sites = {}
    for a in agents:
        key = get_site_key(a)
        if key not in sites:
            sites[key] = {
                "name": key,
                "agents": [],
                "lat": a.get("lat"),
                "lng": a.get("lng"),
                "loc": a.get("loc") or a.get("city") or "",
            }
        sites[key]["agents"].append(a)
        if a.get("lat") is not None and sites[key]["lat"] is None:
            sites[key]["lat"] = a.get("lat")
            sites[key]["lng"] = a.get("lng")
    return sites


def get_endpoint_agents_for_site(site, data):
    all_ep = data.get("ENDPOINT_AGENTS") or []
    all_agents = data.get("ALL_AGENTS") or []
    perf = (data.get("EXTRA_KPI") or {}).get("ep_worst_performers") or []
    list_ = []
    seen_ids = set()

    def add_one(name, loc, id_, device):
        uid = str(id_) if (id_ is not None and id_ != "") else ("n-" + str(name) if (name is not None and name != "") else "r-" + str(len(list_)))
        if uid in seen_ids:
            return
        seen_ids.add(uid)
        metrics = next((p for p in perf if p.get("name") == name or (p.get("name") and p.get("name", "").split(".")[0] == name) or (name and name.find(p.get("name") or "") >= 0)), None) or {}
        list_.append({
            "name": name,
            "loc": loc,
            "id": id_,
            "device": device or "",
            "latency": metrics.get("latency"),
            "rssi": metrics.get("rssi"),
            "gateway_latency": metrics.get("gateway_latency"),
            "gateway_loss": metrics.get("gateway_loss"),
            "cpu": metrics.get("cpu"),
            "memory": metrics.get("memory"),
        })

    def at_site_exact(a):
        return a.get("type") == "endpoint" and (a.get("city") == site.get("name") or a.get("loc") == site.get("loc"))

    for a in all_agents:
        if not at_site_exact(a):
            continue
        ep = next((e for e in all_ep if e.get("id") == a.get("id") or e.get("name") == a.get("name")), None)
        add_one(a.get("name"), a.get("loc") or a.get("city"), a.get("id"), ep.get("device") if ep else a.get("device"))

    site_token = first_location_token(site.get("name")) or first_location_token(site.get("loc"))
    for e in all_ep:
        token = first_location_token(e.get("loc"))
        if token and site_token and token == site_token:
            add_one(e.get("name"), e.get("loc"), e.get("id"), e.get("device"))

    return list_


def main():
    parser = argparse.ArgumentParser(description="Validate endpoint agents logic against live API")
    parser.add_argument("--url", default=BASE_URL, help="API data URL (ignored if --file is set)")
    parser.add_argument("--file", help="Use saved API response JSON file instead of fetching (e.g. from browser DevTools)")
    args = parser.parse_args()

    if args.file:
        print(f"Loading: {args.file}")
        try:
            with open(args.file, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"ERROR: Failed to load file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Fetching: {args.url}")
        try:
            with urlopen(args.url, timeout=30) as resp:
                data = json.load(resp)
        except Exception as e:
            print(f"ERROR: Failed to fetch API: {e}", file=sys.stderr)
            sys.exit(1)

    all_agents = data.get("ALL_AGENTS") or []
    endpoint_agents = data.get("ENDPOINT_AGENTS") or []
    ep_by_type = [a for a in all_agents if a.get("type") == "endpoint"]
    print(f"ALL_AGENTS: {len(all_agents)} total, {len(ep_by_type)} type=endpoint")
    print(f"ENDPOINT_AGENTS: {len(endpoint_agents)} entries")
    print()

    sites = build_sites(data)
    print(f"Sites (from enterprise agents): {list(sites.keys())}")
    print()

    all_ep_names = {e.get("name") for e in endpoint_agents}
    for key, site in sorted(sites.items()):
        ep_list = get_endpoint_agents_for_site(site, data)
        names = [e["name"] for e in ep_list]
        print(f"Site: {key!r}")
        print(f"  Endpoint agents at site: {len(ep_list)}")
        for n in names:
            print(f"    - {n}")
        print()

    # Austin site: expect all 4 endpoint agents (from live API they all have loc "Austin, Texas, US")
    austin_key = "Austin, Texas, US"
    if austin_key in sites:
        austin_ep = get_endpoint_agents_for_site(sites[austin_key], data)
        expected = {"Martin S25 Ultra", "Martin's Deskpro", "MKOWALEW-M-YQVQ", "MKOWALEW-WINDOWS"}
        got = {e["name"] for e in austin_ep}
        if got == expected and len(austin_ep) == 4:
            print("PASS: Austin, Texas, US has exactly 4 endpoint agents and names match.")
        else:
            print("FAIL: Austin, Texas, US endpoint agents mismatch:")
            print(f"  Expected names: {expected}")
            print(f"  Got ({len(austin_ep)}): {got}")
            if got != expected:
                print(f"  Missing: {expected - got}")
                print(f"  Extra: {got - expected}")
            sys.exit(1)
    else:
        print("Note: No site 'Austin, Texas, US' in this dataset (no enterprise agents there).")
        print("Endpoint agents in API (all have loc Austin, Texas, US):", list(all_ep_names))

    print("Validation done.")


if __name__ == "__main__":
    main()
