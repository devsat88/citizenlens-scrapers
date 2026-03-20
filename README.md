# CitizenLens Scrapers

Python scraping system that seeds the CitizenLens civic accountability database with data from Indian government portals. Covers 10 cities, 5 complaint categories, and ~94 government URLs organized in 4 tiers.

## Setup

```bash
python -m venv myenv
source myenv/bin/activate
pip install -r requirements.txt

cp .env.example .env   # add Supabase + AWS credentials if needed
```

## Quick Start

```bash
# Run a single task (safe — writes JSON to output/ only)
python run_task.py tasks/tier1_structured/opencity_swachh_survekshan.yaml --mode dry_run

# Run all Tier 1 tasks
python run_tier.py --tier 1 --mode dry_run

# Run Tier 2 roads tasks only
python run_tier.py --tier 2 --category roads --mode dry_run

# Run Tier 4 for one city
python run_tier.py --tier 4 --city MUM --mode dry_run

# Run by category across all tiers
python run_category.py --category garbage --mode dry_run
```

## 3-Mode System

| Mode         | Output                      | Secrets needed |
|--------------|-----------------------------|----------------|
| `dry_run`    | JSON files in `output/`     | None           |
| `staging`    | Supabase `_staging` tables  | Yes            |
| `production` | Supabase production tables  | Yes            |

**Always run `dry_run` first.** Review `output/` JSON, then promote via `upload_approved.py`.

## Running Tests

```bash
pip install pytest
python -m pytest tests/ -v
```

## GitHub Actions Workflows

| Workflow              | Trigger                    | Purpose                              |
|-----------------------|----------------------------|--------------------------------------|
| `dryrun.yml`          | Manual (no secrets needed) | Validate any tier/category/city      |
| `staging-tier2.yml`   | Manual                     | Tier 2 national portals → staging    |
| `staging-tier3.yml`   | Manual                     | Tier 3 state portals → staging       |
| `staging-tier4.yml`   | Manual                     | Tier 4 city portals → staging        |
| `refresh-weekly.yml`  | Every Monday 7:30 AM IST   | Weekly production refresh            |
| `run-single-task.yml` | Manual                     | Run any single task in any mode      |

## Project Structure

```
tasks/          # ~106 YAML task configs (one per URL per city/state)
fetchers/       # web, api, csv, pdf fetchers
parsers/        # html_table, json_extract, csv_normalize, geo_parser, pdf_table, aspnet_form
transformers/   # cost_cleaner, date_cleaner, name_normalizer, validator, deduplicator
loaders/        # json_writer, db_writer, s3_uploader, gap_tracker
config/         # cities, authorities, settings
migrations/     # Supabase SQL migrations
```
