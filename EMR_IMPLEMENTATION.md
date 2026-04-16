# Implementation Summary: EMR-Ready Architecture

## What Was Implemented

### 1. **Dual-Mode Processing Script** 
**File:** `data-processing/scripts/process_reddit_data.py`

- Added CLI argument parsing with `argparse`:
  - `--mode [local|emr]` - Choose execution environment
  - `--s3-bucket` - S3 bucket name (default: `bigdata-cs-careers`)
  - `--s3-prefix` - S3 path prefix (e.g., "reddit-data/")
  - `--local-output-dir` - Local output directory
  - `--download-results` - Auto-download from S3 after job

**Behavior:**
- **LOCAL MODE** (default):
  - Reads from: `data/raw_posts.json`
  - Writes to: `data/processed/` using **Pandas** (single CSV files)
  - Fast for development/testing
  
- **EMR MODE**:
  - Reads from: `s3://bucket/prefix/raw_posts.json`
  - Processes on Spark cluster
  - Writes to: `s3://bucket/prefix/processed_data/` using **Spark CSV Writer**
  - Auto-downloads results to local machine via `aws s3 sync`

### 2. **EMR Submission Tool** 
**File:** `data-processing/scripts/emr_submit.py`

**Purpose:** Simplify job submission to EMR clusters

**Features:**
- Validates AWS CLI is installed
- Checks EMR cluster is running
- Verifies S3 bucket exists and contains data
- Uploads script to S3 (one-time, minimal write)
- Submits Spark job to cluster
- Returns step ID for monitoring

**Note:** Script upload to S3 is necessary for EMR to access the code. This is a minimal, one-time write operation (~100KB). All data processing results are saved to `/tmp/` on the cluster, not S3.

**Usage:**
```bash
python data-processing/scripts/emr_submit.py \
  --cluster-id j-XXXXX \
  --s3-bucket bigdata-cs-careers \
  --s3-prefix reddit-data/
```

### 3. **EMR Documentation** ✅
**File:** `data-processing/README_EMR.md`

**Contents:**
- Prerequisites & AWS CLI setup
- Step-by-step EMR cluster creation
- Job submission methods (CLI & Console)
- Progress monitoring & troubleshooting
- Results download procedures
- Cost estimates
- Advanced configurations

### 4. **Quick Start Guide** ✅
**File:** `SETUP_EMR.md` (project root)

**Contents:**
- 5-minute TL;DR for examiners
- Architecture diagram
- Local testing instructions
- Script reference table

---

## How It Works

### Local Execution (Testing)
```bash
python data-processing/scripts/process_reddit_data.py --mode local
# Output: data/processed/*.csv
```

### EMR Execution (Production)
```bash
# Step 1: Create cluster (AWS Console or CLI)
aws emr create-cluster --name reddit-analysis ...

# Step 2: Submit job
python data-processing/scripts/emr_submit.py \
  --cluster-id j-XXXXX \
  --s3-bucket bigdata-cs-careers

# Step 3: Job processes on cluster (~10-15 min)
# Results saved to: /tmp/reddit-results/ (on EMR master)

# Step 4: Download results via SCP
scp -r ec2-user@<master-ip>:/tmp/reddit-results/* data/processed/
# Output: data/processed/*.csv
```

---

## File Structure

```
cs-careers/
├── SETUP_EMR.md                          ← Quick start guide for examiners
│
└── data-processing/
    ├── README_EMR.md                     ← Detailed EMR documentation
    ├── README.md                         ← Original readme
    │
    └── scripts/
        ├── process_reddit_data.py        ← Main job (LOCAL & EMR modes)
        ├── emr_submit.py                 ← EMR submission tool
        └── ...
```

---

## Key Features

 **CLI Arguments** - No hard-coding; flexible configuration
 **Dual Mode** - Works locally for testing, on EMR for production
 **SCP Download** - Results saved to /tmp on EMR, download via SSH key pair
 **Validation** - Pre-flight validation prevents common errors
 **Documentation** - Complete setup & troubleshooting guide
 **Public Data** - S3 bucket is public; anyone can access via AWS CLI
 **Pandas for Output** - Simple single-CSV output (no Hadoop dependency)
 **No S3 Writes** - Only S3 READ permissions needed

---

## Usage for Examiners

**Examiner receives:**
1. GitHub repo with code
2. `SETUP_EMR.md` - quick start
3. Public S3 bucket: `bigdata-cs-careers`

**Examiner does:**
```bash
# 1. Clone repo & install requirements
git clone <repo>
pip install pyspark pandas boto3

# 2. Configure AWS (Learner Lab credentials)
aws configure

# 3. Create EMR cluster (AWS Console or CLI)

# 4. Submit job
python data-processing/scripts/emr_submit.py \
  --cluster-id j-XXXXX \
  --s3-bucket bigdata-cs-careers

# 5. Wait ~10-15 minutes for results
# Results auto-download to: data/processed/

# 6. Use CSVs in Streamlit app (downstream)
```

1. **Run locally** for quick testing:
   ```bash
   python data-processing/scripts/process_reddit_data.py --mode local
   ```

2. **Run on EMR** for production:
   ```bash
   python data-processing/scripts/emr_submit.py --cluster-id j-XXXXX --s3-bucket bigdata-cs-careers
   ```

3. **Use custom S3 paths**:
   ```bash
   python data-processing/scripts/emr_submit.py \
     --cluster-id j-XXXXX \
     --s3-bucket my-bucket \
     --s3-prefix my-prefix/
   ```

4. **Integrate with Streamlit** using CSVs from `data/processed/`

---

## Architecture Summary

```
┌─────────────────────────────────────────────────────────┐
│  Professor's Laptop (AWS Learner Lab)                    │
│  ┌───────────────────────────────────────────────────┐  │
│  │ 1. AWS CLI                                        │  │
│  │ 2. Python + PySpark                               │  │
│  │ 3. Clone repo with scripts                        │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                            │
                  ┌─────────┴──────────┐
                  ▼                    ▼
    ┌─────────────────────┐  ┌──────────────────┐
    │ Local Mode          │  │ EMR Mode         │
    │ (Testing)           │  │ (Production)     │
    │                     │  │                  │
    │ • Fast              │  │ • Distributed    │
    │ • Single CSV files  │  │ • Scalable       │
    │ • Pandas writer     │  │ • Spark writer   │
    │ • data/raw_posts    │  │ • S3 input/out   │
    └─────────────────────┘  └──────────────────┘
                  │                    │
                  │                    ▼
                  │         ┌──────────────────┐
                  │         │ EMR Cluster      │
                  │         │ (Spark)          │
                  │         │                  │
                  │         │ • 12 steps       │
                  │         │ • 5-15 min       │
                  │         │ • Auto S3 sync   │
                  │         └──────────────────┘
                  │                    │
                  └────────┬───────────┘
                           ▼
            ┌──────────────────────────┐
            │ data/processed/          │
            │ (CSV Results)            │
            │                          │
            │ • topic_analysis.csv     │
            │ • sentiment_by_topic.csv │
            │ • ... (8 total)          │
            └──────────────────────────┘
                           │
                           ▼
                  ┌──────────────────┐
                  │ Streamlit App    │
                  │ (Downstream)     │
                  └──────────────────┘
```

---


