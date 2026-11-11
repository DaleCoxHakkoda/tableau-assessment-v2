# Vendored from sigmacomputing/sigma-migration-skills

Copied unmodified from https://github.com/sigmacomputing/sigma-migration-skills
at commit `6af710c3b0745f489508749f378a3f701198b21e` (2026-10-29), path `plugins/tableau-to-sigma/skills/tableau-to-sigma/`,
under the Apache License 2.0 (`LICENSE`, `NOTICE` in this folder).

| File | Role |
|---|---|
| `scripts/scan-workbook-gaps.py` | Gap scanner: detects Tableau features and their Sigma conversion tier |
| `scripts/formula-audit.mjs` | Formula audit helper, run by the scanner with Node.js |
| `scripts/lib/tableau_functions.json` | Function catalog read by the helper |
| `refs/functions.json` | Function translation table |
| `converter/tableau.mjs`, `converter/PROVENANCE.json` | Converter bundle the helper loads |

The folder layout mirrors upstream because the scanner and helper find each other by
relative path. Do not edit these files; to update, copy the same paths from a newer
upstream commit and record it here. `scripts/complexity.py` (step 05) runs the scanner.
