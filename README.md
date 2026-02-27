# CMS Stars Visualizer

Static web dashboard for CMS Stars data (2017-2026) with three views:
- Contract
- Parent Company (lives-weighted)
- All MA (lives-weighted)

## Files
- `index.html` - dashboard UI
- `app.js` / `app.css` - frontend logic and styles
- `data/data.js` - generated dataset consumed by UI
- `scripts/build-cms-stars-dataset.ps1` - CMS ETL and data generation
- `scripts/open-app.ps1` - opens the dashboard in your default browser

## Build Data
Run from repo root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build-cms-stars-dataset.ps1 -StartYear 2017 -EndYear 2026
```

Incremental options:

```powershell
# rebuild/refresh only one year shard then merge full window
powershell -ExecutionPolicy Bypass -File .\scripts\build-cms-stars-dataset.ps1 -StartYear 2017 -EndYear 2026 -Years 2026

# force one year refresh from source
powershell -ExecutionPolicy Bypass -File .\scripts\build-cms-stars-dataset.ps1 -StartYear 2017 -EndYear 2026 -Years 2026 -ForceYear 2026

# merge using cache only (no network)
powershell -ExecutionPolicy Bypass -File .\scripts\build-cms-stars-dataset.ps1 -StartYear 2017 -EndYear 2026 -SkipDownload
```

## Open App
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\open-app.ps1
```

## Generated Data
- `data/data.js` is generated and intentionally not committed to git.
- Build it with the script above before opening `index.html`.

## Notes
- Current star weight handling uses equal weights (`1.0`) per available measure because CMS measure tables do not publish an explicit star-weight column in the downloaded measure tables.
- `calculated_raw_stars_score` uses:
  - `(measure_stars * star_weight) / sum_available_weights(contract-year)`
  - Measures without valid stars are excluded from `sum_available_weights`.
- Measures are keyed across years by normalized canonical measure name (not just code like `C27`).
- Alias overrides live in `data/measure_aliases.json`.
- `total_raw_stars_score` is included and equals the sum of per-measure calculated scores at the contract-year level.
- MA-PD proxy filter keeps contract prefixes `H`, `R`, `E` and excludes PDP-like plan type rows when that field is available in enrollment files.
