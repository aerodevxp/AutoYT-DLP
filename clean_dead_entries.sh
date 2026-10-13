#!/bin/bash
#Clean fingerprints for unexisting files. Run this if you've deleted files from the library + './auto_ytdlp.py --scan'.
./venv/bin/python - <<'PY'
import json, os
p = ".dedupe_library.json"
d = json.load(open(p))
fps = d.get("fingerprints", {})
new, dead = {}, 0
for fp, paths in fps.items():
    live = [x for x in paths if os.path.isfile(x)]
    if live:
        new[fp] = live
    else:
        dead += 1
json.dump({"fingerprints": new}, open(p, "w"), indent=1)
print(f"kept {len(new)} fingerprints, pruned {dead} dead entries")
PY
