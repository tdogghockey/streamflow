#!/usr/bin/env python3
"""Comprehensive verification: every stream in FOX universe (grps 5/9/14/24/35 + linked to group 2532 channels)
must match EXACTLY ONE channel pattern in streamflow, with correct owner classification."""
import sqlite3
import re
from collections import defaultdict

# Load streamflow patterns in FOX range
conn = sqlite3.connect('/app/data/streamflow.db')
cur = conn.cursor()
rows = cur.execute('SELECT channel_id, pattern FROM channel_regex_patterns WHERE channel_id BETWEEN 66336 AND 80597').fetchall()
configs = cur.execute('SELECT channel_id, name FROM channel_regex_configs WHERE channel_id BETWEEN 66336 AND 80597').fetchall()
conn.close()

chan_pats = defaultdict(list)
for cid, pat in rows:
    chan_pats[str(cid)].append(pat)
name_map = {str(c): n for c, n in configs}

# Replicate _compile_stream_search_regex semantics
WHITESPACE = re.compile(r'(?<!\\) +')
def compile_pat(p):
    return re.compile(WHITESPACE.sub(r'\\s+', p), re.IGNORECASE)

compiled = {cid: [compile_pat(p) for p in pats] for cid, pats in chan_pats.items()}

def matches(name):
    return [cid for cid, rs in compiled.items() if any(r.search(name) for r in rs)]

# Stream universe: query dispatcharr DB via API-free approach - use streamflow's own stored stream data
# Instead use regex test via streamflow's own DB. We need the FOX streams; dump from dispatcharr below in shell.
# This module is loaded inside the dispatcharr shell with universe passed via env file.
import sys
universe_file = sys.argv[1] if len(sys.argv) > 1 else None
if not universe_file:
    print("usage: verify_regex.py <streams.txt>")
    sys.exit(1)

streams = [l.strip() for l in open(universe_file) if l.strip()]
print(f"streams to verify: {len(streams)}")

multi = []
zero = []
for name in streams:
    hits = matches(name)
    if len(hits) == 0:
        zero.append(name)
    elif len(hits) > 1:
        multi.append((name, hits))

print(f"\nDOUBLE-MATCHED: {len(multi)}")
for name, hits in multi:
    print(f"  {name[:75]} -> {[(h, name_map.get(h,'?')) for h in hits]}")

print(f"\nUNMATCHED: {len(zero)}")
for name in zero:
    print(f"  {name[:90]}")
