# Reddit CS Career Intelligence

Automated pipeline that collects Reddit discussions from r/csMajors and r/cscareerquestions daily, processes them with PySpark, and visualizes career insights through a Streamlit dashboard hosted on Streamlit Community Cloud.

---

## Architecture

```
CloudWatch (daily 6 AM UTC)
  → AWS Lambda (collector.py)
  → s3://bigdata-cs-careers/raw/YYYY-MM-DD/posts.json

python process_reddit_data.py --date YYYY-MM-DD   (run manually)
  → downloads raw JSON from S3
  → PySpark local processing
  → uploads 8 CSVs to s3://bigdata-cs-careers/processed/YYYY-MM-DD/

Streamlit Community Cloud (app.py)
  → reads processed CSVs from S3 via s3fs
  → renders interactive dashboard
```

---

## Prerequisites

- Python 3.9+ with conda or pip
- AWS Learner Lab session (active)
- Java 8 or 11 (required by PySpark)

### Install dependencies

**Conda (recommended):**
```bash
conda env create -f environment.yml
conda activate reddit-career-analysis
```
OR 

**Pip:**
```bash
pip install -r requirements.txt
```

---

## AWS Credentials Setup

This project uses **AWS Academy Learner Lab**, which issues temporary STS credentials that expire every ~4 hours when the lab session ends. You must refresh credentials each new lab session.

### Step 1 — Start your Learner Lab session
In the AWS Academy portal, click **Start Lab** and wait for the indicator to turn green.

### Step 2 — Get your credentials
Click **AWS Details** -> **Show** next to "AWS CLI". You will see three values:
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

### Step 4 — Update Streamlit secrets
Edit `.streamlit/secrets.toml` with the same three credential values:
```toml
[aws]
access_key_id     = "YOUR_ACCESS_KEY_ID"
secret_access_key = "YOUR_SECRET_ACCESS_KEY"
session_token     = "YOUR_SESSION_TOKEN"
```

> **Note:** `.streamlit/secrets.toml` is gitignored and never committed. For the deployed Streamlit app, add these same values in the app dashboard under **Settings -> Secrets**.

---

## AWS Lambda — Automated Daily Collection

The Lambda function (`collector.py`) runs automatically every day at 6:00 AM UTC via a CloudWatch Events rule.

### Lambda function setup

1. In the AWS Console, go to **Lambda -> Create function**
   - Name: `cs-career-collector`
   - Runtime: Python 3.9
   - Execution role: `LabRole` (existing role — do not create a new one)

2. Upload the deployment package:
   ```bash
   cd data-processing/scripts
   zip collector_lambda.zip collector.py
   ```
   In the Lambda console: **Code → Upload from -> .zip file** → select `collector_lambda.zip`

3. Add the `requests` layer or set the handler to include dependencies. Alternatively, build a full package:
   ```bash
   pip install requests -t package/
   cp collector.py package/
   cd package && zip -r ../collector_lambda.zip .
   ```

4. Set the handler to `collector.lambda_handler`

5. Under **Configuration -> General**:
   - Memory: 512 MB
   - Timeout: 5 minutes

### CloudWatch Events trigger (daily cron)

1. In the Lambda console, click **Add trigger**
2. Select **EventBridge (CloudWatch Events)**
3. Create a new rule:
   - Rule name: `cs-career-daily`
   - Rule type: Schedule expression
   - Expression: `cron(0 6 * * ? *)` — runs daily at 6:00 AM UTC
4. Click **Add**

### Test the Lambda function

In the Lambda console, go to the **Test** tab:
1. Create a new test event named `TestRun` with body `{}`
2. Click **Test**

Check the S3 bucket after it runs — you should see:
```
s3://bigdata-cs-careers/raw/YYYY-MM-DD/posts.json
s3://bigdata-cs-careers/raw/YYYY-MM-DD/metadata.json
```

### Run the collector manually (local)

You can also run the collector locally to collect and upload today's data:
```bash
python data-processing/scripts/collector.py
```
This saves data locally to `data/raw_posts_YYYY-MM-DD.json` and uploads to S3.

---

## Processing Data

After data has been collected (either by Lambda or locally), run the processor:

```bash
python data-processing/scripts/process_reddit_data.py --date 2026-04-17
```

Replace the date with the date you want to process. Defaults to today's UTC date if omitted.

**What it does:**
1. Checks if `data/raw_posts_YYYY-MM-DD.json` exists locally — uses it if found (skips download)
2. Otherwise downloads `raw/YYYY-MM-DD/posts.json` from S3
3. Runs PySpark locally (single-threaded, 4 GB driver memory)
4. Saves 8 CSV datasets to `data/processed/`
5. Uploads all CSVs to `s3://bigdata-cs-careers/processed/YYYY-MM-DD/`

**Runtime:** ~5–10 minutes depending on system specs.

> **Windows note:** The processor runs Spark in `local[1]` (single-threaded) mode to avoid Python UDF socket errors on Windows.

---

## Running the Streamlit Dashboard

### Locally

Make sure `.streamlit/secrets.toml` has valid credentials (see AWS Credentials Setup above), then:

```bash
streamlit run app.py
```

The app auto-detects the most recent processed date in S3 and loads all charts.

### Deployed on Streamlit Community Cloud

1. Push code to GitHub (secrets.toml is gitignored — never committed)
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app** -> connect your repo
3. Set main file to `app.py`
4. Under **Settings -> Secrets**, paste the contents of your `.streamlit/secrets.toml`
5. Deploy

> **Each new Learner Lab session:** update the secrets in the Streamlit app dashboard — credentials expire with the lab session.

---

## Outputs

Eight CSV datasets are written to `s3://bigdata-cs-careers/processed/YYYY-MM-DD/`:

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

**`aws s3 ls` returns an error about credentials**
→ Re-run `aws configure` and `aws configure set aws_session_token` with fresh Learner Lab credentials.

**Processor fails with `NoCredentialsError`**
→ AWS CLI credentials have expired. Refresh them (see AWS Credentials Setup).

**Streamlit app shows "No processed data found in S3"**
→ The processor has not been run for any date yet, or the S3 credentials in secrets.toml are expired.

**Spark fails with `SocketException: Software caused connection abort`**
→ Ensure `local[1]` is set in the SparkSession (not `local[*]`) — this is a known Windows issue with multi-threaded Python UDFs.

**Lambda times out**
→ Increase timeout to 10 minutes in Lambda configuration. Collection of ~2000 posts typically takes 90–120 seconds.
