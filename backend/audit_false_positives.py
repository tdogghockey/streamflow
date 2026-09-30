#!/usr/bin/env python3
"""Full false-positive audit: every candidate stream vs ALL FOX-range patterns.
Replicates streamflow _compile_stream_search_regex exactly."""
import sqlite3
import re
import sys
from collections import defaultdict

conn = sqlite3.connect('/app/data/streamflow.db')
cur = conn.cursor()
rows = cur.execute('SELECT channel_id, pattern FROM channel_regex_patterns WHERE channel_id BETWEEN 66336 AND 80597').fetchall()
configs = cur.execute('SELECT channel_id, name FROM channel_regex_configs WHERE channel_id BETWEEN 66336 AND 80597').fetchall()
conn.close()

chan_pats = defaultdict(list)
for cid, pat in rows:
    chan_pats[str(cid)].append(pat)
name_map = {str(c): n for c, n in configs}

WHITESPACE = re.compile(r'(?<!\\) +')
def compile_pat(p):
    return re.compile(WHITESPACE.sub(r'\\s+', p), re.IGNORECASE)

compiled = {cid: [compile_pat(p) for p in pats] for cid, pats in chan_pats.items()}

def matches(name):
    return [cid for cid, rs in compiled.items() if any(r.search(name) for r in rs)]

streams = [l.strip() for l in open(sys.argv[1]) if l.strip()]
print(f"candidate streams: {len(streams)}  patterns: {len(chan_pats)}")

multi = []
single = []
zero = []
for name in streams:
    hits = matches(name)
    if len(hits) == 0:
        zero.append(name)
    elif len(hits) > 1:
        multi.append((name, hits))
    else:
        single.append((name, hits[0]))

print(f"\n=== DOUBLE-MATCHED (conflation risk): {len(multi)} ===")
for name, hits in multi:
    print(f"  {name[:80]} -> {[(h, name_map.get(h,'?')) for h in hits]}")

print(f"\n=== UNMATCHED (not claimed by any FOX pattern): {len(zero)} ===")
for name in zero:
    print(f"  {name[:90]}")

# Save single matches to a file for review of false positives
with open('/app/fox_single_matches.txt', 'w') as f:
    for name, cid in sorted(single, key=lambda x: (x[1], x[0])):
        f.write(f"{cid}\t{name_map.get(cid,'?')}\t{name}\n")
print(f"\n=== SINGLE-MATCHED: {len(single)} (saved to /app/fox_single_matches.txt) ===")
