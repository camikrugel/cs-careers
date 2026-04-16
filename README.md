# Reddit CS Career Data Analysis

## Project Overview

This project processes Reddit posts from CS career communities to extract insights about salaries, job search trends, required skills, and industry sentiment. The data is processed using Apache Spark and can run locally or on AWS EMR for distributed processing.

Original Data Source: Reddit posts from r/cscareerquestions, r/cscq, and related subreddits
Input: raw_posts.json 
Output: 8 analytical CSV datasets

---

## Quick Start

### Option 1: Local Processing (Development)

Prerequisites (Choose One):

**Option A: Using Conda (Recommended)**
```bash
# Create environment from file
conda env create -f environment.yml

# Activate environment
conda activate reddit-career-analysis
```

**Option B: Using Pip**
```bash
pip install -r requirements.txt
```

Run:
```bash
python data-processing/scripts/process_reddit_data.py --mode local
```

Output: data/processed/ (all 8 CSVs)
Time: 5-10 minutes (depends on system specs)

### Option 2: AWS EMR (Production)

Prerequisites:
- AWS Learner Lab access
- SSH key pair (vockey)

Steps:
1. Follow full instructions: SETUP_EMR.md
2. Create EMR cluster with 3 x m5.xlarge instances
3. Submit Spark job pointing to s3://bigdata-cs-careers/scripts/process_reddit_data.py
4. Download results via SCP (10-15 minutes)

Permissions Required:
- S3 READ: bigdata-cs-careers/raw_posts.json
- S3 READ: bigdata-cs-careers/scripts/process_reddit_data.py
- EMR cluster management
- NO S3 write permissions needed

---

## Analytics Generated

The pipeline produces 8 CSV datasets with actionable insights:

### 1. Topic Analysis (topic_analysis.csv)
- Breaks down post distribution by topic category
- Topics: internship, interview, salary, job_search, resume, projects, courses, new_grad, faang
- Metrics: post count, percentage distribution

### 2. Sentiment by Topic (sentiment_by_topic.csv)
- Sentiment analysis (positive/neutral/negative) per topic
- Identifies which topics have most positive/negative sentiment
- Useful for: Understanding community sentiment on different career topics

### 3. Posts by Industry (posts_by_industry.csv)
- Career field distribution (FAANG, startups, BigTech, etc.)
- Post counts per industry
- Useful for: Market trend analysis

### 4. Salary Statistics (salary_stats.csv)
- Average, min, max salaries by industry
- Salary mention frequency
- Distribution analysis
- Useful for: Compensation benchmarking

### 5. Experience Distribution (experience_distribution.csv)
- Posts categorized by career level (intern, entry, mid, senior, exec)
- Post frequency per level
- Useful for: Understanding which career stages discuss Reddit most

### 6. Skills Summary (skills_summary.csv)
- Ranked list of most mentioned skills/technologies
- Includes: Python, Java, C++, JavaScript, React, AWS, Docker, Kubernetes, etc.
- Metrics: mention count, popularity ranking
- Useful for: Resume optimization, hiring trends

### 7. Temporal Trends (temporal_trends.csv)
- Post volume over time (by month)
- Sentiment trends by month
- Useful for: Identifying seasonal patterns in job market

### 8. Network Metrics (network_metrics.csv)
- Aggregate statistics: total posts, avg length, engagement patterns
- Text statistics: average words per post, title length
- Useful for: High-level dataset overview

---

## Data Processing Pipeline

The pipeline applies 12 sequential transformations:

1. JSON Loading - Parse raw Reddit posts from S3 or local file
2. Text Cleaning - Remove URLs, markdown, special chars; normalize text
3. Topic Classification - Regex-based categorization (9 topics)
4. Salary Extraction - Parse salary mentions (patterns: $120k, tc: 120k)
5. Sentiment Analysis - TextBlob polarity/subjectivity scores
6. Industry Detection - Identify target career fields
7. Experience Level Extraction - Parse career stage from text
8. Skill Extraction - Identify 50+ programming languages/frameworks
9. Topic-Sentiment Aggregation - Group sentiment by topic
10. Industry Statistics - Salary/post aggregations by industry
11. Temporal Analysis - Group posts by month for trends
12. Final Export - Write 8 CSV files

Key Transformation Details

Text Cleaning Pipeline:
- Removes URLs, emails, Reddit markdown
- Converts to lowercase
- Removes HTML entities
- Filters stopwords (the, a, an, etc.)

Salary Extraction Patterns:
- $120k, $120K becomes 120,000
- $120,000 becomes 120,000
- tc: 120k, total comp: 120k becomes 120,000
- Validates range: 30k-500k (outliers excluded)

Skill Detection:
- 50+ programming languages/frameworks
- Includes: Python, Java, JavaScript, C++, Go, Rust, etc.
- Frameworks: React, Django, Spring, Vue, etc.
- Cloud: AWS, GCP, Azure
- DevOps: Docker, Kubernetes, Terraform, etc.

---

## Usage Examples

Run Locally:
```bash
python data-processing/scripts/process_reddit_data.py --mode local
```

Run on EMR:
```bash
python data-processing/scripts/process_reddit_data.py \
  --mode emr \
  --s3-bucket bigdata-cs-careers \
  --local-output-dir /tmp/reddit-results/
```

Integration with Streamlit (Downstream):
```python
import pandas as pd

salary = pd.read_csv('data/processed/salary_stats/salary_stats.csv')
skills = pd.read_csv('data/processed/skills_summary/skills_summary.csv')

# Build visualizations in Streamlit
```

---

## Technical Details

Languages & Libraries:
- Apache Spark 3.x (PySpark)
- Python 3.9+
- TextBlob (sentiment analysis)
- Pandas (local output)

Processing Performance:
- Local: 5-10 minutes
- EMR (3x m5.xlarge): 10-15 minutes

Data Volume:
- Input: 50MB+ JSON
- Output: 10-20MB CSV (8 files)

---

## Troubleshooting

Local Mode Issues

Error: "Hadoop home directory not found"
- This is expected on Windows/local. Pandas writer handles it.

Error: "raw_posts.json not found"
- Ensure data/raw_posts.json exists with Reddit posts data

EMR Mode Issues

See data-processing/README_EMR.md for detailed EMR troubleshooting

---

## For Instructor

This project demonstrates:
- PySpark SQL transformations and aggregations
- Distributed processing on AWS EMR
- Cloud data pipeline architecture (S3 integration)
- CLI argument handling and flexible configuration
- Local vs cloud execution patterns
- Data validation and error handling
- Production-ready code structure

Submission Package Includes:
1. GitHub repo with all code
2. SETUP_EMR.md - Quick start guide
3. Public S3 bucket: bigdata-cs-careers (read-only access)
4. Pre-loaded script in S3 (ready to submit)

---

## Support & Documentation

- EMR Setup: SETUP_EMR.md
- Detailed EMR Guide: data-processing/README_EMR.md
- Technical Architecture: EMR_IMPLEMENTATION.md
- AWS EMR Docs: https://docs.aws.amazon.com/emr/
- PySpark Docs: https://spark.apache.org/docs/latest/api/python/
