"""Group Sigma gap-scanner features into causes that recur across workbooks.

The scanner reports every refused calculated field as its own feature, named after the
field ("Converter-refused calculated field — <datasource>: <calc>"), so no two workbooks
share one and an estate-wide count of "feature types" is just a count of formulas. This
module reads the scanner's formula-audit.json (written beside each gaps report) and files
each refused formula under its causes instead:

  * the converter's warning, minus the formula text ("LOD expression not converted")
  * each Tableau function in it that Sigma's converter does not translate, per the
    vendored function catalog ("Function not converted: WINDOW_SUM (table calculation)")

A formula with neither is filed as "Formula refused, no reason recorded". Every other
feature keeps its scanner name, which is already a recurring feature type.
"""

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CATALOG = REPO_ROOT / "vendor" / "tableau-to-sigma" / "refs" / "functions.json"
REFUSED_PREFIX = "Converter-refused calculated field"
REFUSED_STATUSES = {"not_converted", "unmapped"}       # formula statuses the scanner refuses
NOT_TRANSLATED = {"not_converted", "unmapped", "chart_only"}  # function statuses worth naming
NO_REASON = "Formula refused, no reason recorded"

_catalog = None


def function_catalog():
    """{TABLEAU_FN: (status, category)} from the vendored catalog; empty if missing."""
    global _catalog
    if _catalog is None:
        try:
            rows = json.loads(CATALOG.read_text(encoding="utf-8")).get("functions") or []
            _catalog = {r["tableau_fn"].upper(): (r.get("status"), r.get("category")) for r in rows}
        except (OSError, ValueError, KeyError):
            _catalog = {}
    return _catalog


def warning_cause(text):
    """'⚠ LOD expression not converted: {FIXED ...' -> 'LOD expression not converted'."""
    t = re.sub(r"^[^\w]+", "", text or "").strip()
    t = re.split(r":|\s[—–-]\s", t, maxsplit=1)[0].strip()
    return t[:80] or None


def formula_causes(row):
    causes = set()
    for w in row.get("warnings") or []:
        c = warning_cause(w)
        if c:
            causes.add(f"Formula: {c}")
    cat = function_catalog()
    for fn in row.get("tableau_functions") or row.get("function_names") or []:
        status, category = cat.get(str(fn).upper(), ("unmapped", None))
        if status in NOT_TRANSLATED:
            causes.add(f"Function not converted: {str(fn).upper()}" + (f" ({category})" if category else ""))
    return causes or {NO_REASON}


def categorized_features(features, gaps_dir):
    """Scanner features -> [{name, status, count}] with refused formulas grouped by cause.

    gaps_dir holds formula-audit.json; without it refused formulas fall back to one
    'no reason recorded' bucket rather than one feature each."""
    out = [f for f in features if not str(f.get("name") or "").startswith(REFUSED_PREFIX)]
    n_refused = len(features) - len(out)
    if not n_refused:
        return out
    rows = []
    fa = Path(gaps_dir) / "formula-audit.json" if gaps_dir else None
    if fa and fa.exists():
        try:
            rows = [r for r in json.loads(fa.read_text(encoding="utf-8")).get("formulas") or []
                    if r.get("status") in REFUSED_STATUSES and not r.get("unused")]
        except ValueError:
            rows = []
    counts = {}
    if rows:
        for r in rows:
            for c in formula_causes(r):
                counts[c] = counts.get(c, 0) + 1
    else:
        counts[NO_REASON] = n_refused
    status = next((f.get("status") for f in features if str(f.get("name") or "").startswith(REFUSED_PREFIX)),
                  "unhandled")
    return out + [{"name": c, "status": status, "count": n} for c, n in sorted(counts.items())]
