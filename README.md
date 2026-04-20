# Reddit CS Career Intelligence

Automated pipeline that collects Reddit discussions from r/csMajors and r/cscareerquestions, processes them with PySpark, and visualizes career insights through a Streamlit dashboard hosted on Streamlit Community Cloud.

---

## Architecture

```
python data-processing/scripts/collector.py   (run manually, local machine)
  → collects posts from Reddit API
  → uploads to s3://bigdata-cs-careers/raw/YYYY-MM-DD/

python data-processing/scripts/process_reddit_data.py --date YYYY-MM-DD
  → reads raw JSON from S3
  → PySpark local processing
  → uploads 8 CSVs to s3://bigdata-cs-careers/processed/YYYY-MM-DD/

Streamlit Community Cloud (app.py)
  → reads processed CSVs from S3 (public bucket, no credentials needed)
  → renders interactive dashboard
```

> **Note:** The collector must be run from a local machine — Reddit blocks requests from AWS IP addresses (HTTP 403).

---

## Prerequisites

- Python 3.9+ with conda or pip
- Java 8 or 11 (required by PySpark)
- AWS Learner Lab session (active) — needed to upload to S3

### Install dependencies

**Conda (recommended):**
```bash
conda env create -f environment.yml
conda activate reddit-career-analysis
```

**Pip:**
```bash
pip install -r requirements.txt
```

---

## AWS Credentials Setup

This project uses **AWS Academy Learner Lab**, which issues temporary STS credentials that expire every ~4 hours. You must refresh credentials each new lab session.

### Step 1 — Start your Learner Lab session
In the AWS Academy portal, click **Start Lab** and wait for the indicator to turn green.

### Step 2 — Get your credentials
Click **AWS Details** → **Show** next to "AWS CLI". You will see three values:
- `aws_access_key_id`
- `aws_secret_access_key`
- `aws_session_token`

### Step 3 — Configure the AWS CLI
```bash
aws configure
```
Enter your access key and secret key when prompted. Set region to `us-east-1`. Leave output format blank.

Then set the session token (required for Learner Lab — `aws configure` does not prompt for this):
```bash
aws configure set aws_session_token YOUR_SESSION_TOKEN
```

Verify it works:
```bash
aws s3 ls s3://bigdata-cs-careers/
```

---

## Step 1 — Collect Data

Run the collector from your local machine:

```bash
python data-processing/scripts/collector.py
```

This fetches up to ~2000 posts from r/csMajors and r/cscareerquestions and uploads them to:
```
s3://bigdata-cs-careers/raw/YYYY-MM-DD/posts.json
s3://bigdata-cs-careers/raw/YYYY-MM-DD/metadata.json
```

**Runtime:** ~3–5 minutes (respects Reddit rate limits).

> Requires active Learner Lab credentials (for S3 upload). Reddit credentials are not needed — the public API is used.

---

## Step 2 — Process Data

After collection, run the processor:

```bash
python data-processing/scripts/process_reddit_data.py --date 2026-04-20
```

Replace the date with today's UTC date (defaults to today if omitted).

**What it does:**
1. Reads `raw/YYYY-MM-DD/posts.json` directly from S3 into memory
2. Runs PySpark locally (single-threaded, 4 GB driver memory)
3. Saves 8 CSV datasets to `data/processed/`
4. Uploads all CSVs to `s3://bigdata-cs-careers/processed/YYYY-MM-DD/`

**Runtime:** ~5–10 minutes.

> **Windows note:** Spark runs in `local[1]` (single-threaded) to avoid Python UDF socket errors on Windows.

### Process multiple dates at once

```bash
for date in 2026-04-17 2026-04-18 2026-04-19 2026-04-20; do
    python data-processing/scripts/process_reddit_data.py --date $date
done
```

---

## Step 3 — View Dashboard

### Locally

```bash
streamlit run app.py
```

The app reads directly from the public S3 bucket — no credentials needed.

### Deployed on Streamlit Community Cloud

1. Push code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app** → connect your repo
3. Set main file to `app.py`
4. Deploy — no secrets needed since the S3 bucket is public

The app auto-detects the most recent processed date in S3 and loads all charts.

---

## Outputs

Eight CSV datasets written to `s3://bigdata-cs-careers/processed/YYYY-MM-DD/`:

| Dataset | Description |
|---|---|
| `topic_analysis/` | Post count, avg score, avg comments per topic |
| `sentiment_by_topic/` | Positive/Neutral/Negative post counts per topic |
| `posts_by_industry/` | Post volume and engagement by industry |
| `salary_stats/` | Salary mention counts and engagement by industry |
| `experience_distribution/` | Post counts by experience level |
| `skills_summary/` | Most mentioned programming skills and tools |
| `temporal_trends/` | Monthly post volume and sentiment over time |
| `network_metrics/` | Overall dataset statistics (total posts, comments, etc.) |

---

## Troubleshooting

**`aws s3 ls` returns a credentials error**
→ Re-run `aws configure` and `aws configure set aws_session_token` with fresh Learner Lab credentials.

**Collector gets HTTP 403 from Reddit**
→ Reddit blocks AWS IP addresses. The collector must be run locally, not from Lambda.

**Processor fails with `NoCredentialsError`**
→ AWS CLI credentials have expired. Refresh them (see AWS Credentials Setup).

**Processor fails with `ERROR: Could not download...`**
→ The collector has not been run for that date — no raw data exists in S3.

**Spark fails with `SocketException: Software caused connection abort`**
→ Ensure `local[1]` is set in the SparkSession — this is a known Windows issue with multi-threaded Python UDFs.

**Streamlit shows "No processed data found in S3"**
→ The processor has not been run yet for any date.
