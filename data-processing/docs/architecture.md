## Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│  Local Machine                                                     │
│                                                                    │
│  python data-processing/scripts/collector.py                       │
│                                                                    │
│  • Fetches r/csMajors and r/cscareerquestions via Reddit API       │
│  • Listings: new + top (up to 2000 posts per run)                  │
│  • Downloads seen_ids.json from S3 — skips already-collected IDs  │
│  • HTTP 429 exponential backoff (up to 5 retries)                  │
│  • Uploads seen_ids.json back to S3 after run                      │
└──────────────────────────────────┬────────────────────────────────┘
                                   │ boto3 upload
                                   ▼
┌──────────────────────────────────────────────────┐
│  S3: bigdata-cs-careers                          │
│                                                  │
│  raw/YYYY-MM-DD/                                 │
│    posts.json                                    │
│    metadata.json                                 │
│  metadata/                                       │
│    seen_ids.json   ← cumulative dedup registry   │
└──────────────────────────────────┬───────────────┘
                                   │
          ┌────────────────────────┘
          │ anonymous boto3 read (public bucket)
          ▼
┌───────────────────────────────────────────────────────────────────┐
│  Local Machine                                                     │
│                                                                    │
│  python process_reddit_data.py --date YYYY-MM-DD                  │
│                                                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │  PySpark local[1]                                           │  │
│  │                                                             │  │
│  │  1.  Load JSON → createDataFrame                           │  │
│  │  2.  Clean text (lowercase, remove URLs/special chars)     │  │
│  │  3.  Tokenize → stopword removal                           │  │
│  │  4.  Industry categorization (UDF, keyword-based)          │  │
│  │  5.  Topic classification (UDF, keyword-based)             │  │
│  │  6.  TF-IDF (CountVectorizer → IDF, vocabSize=500)        │  │
│  │  7.  Engagement analysis (by topic/industry/hour)          │  │
│  │  8.  Company mention extraction (29 companies)             │  │
│  │  9.  Advice category detection                             │  │
│  │  10. Sentiment analysis (keyword-based pos/neg/neutral)    │  │
│  │  11. Time series aggregation (by year/month/day)           │  │
│  │  12. Skills × Industry cross-analysis                      │  │
│  │  13. Salary string parsing (median/min/max per industry)   │  │
│  │  14. Upload 11 CSVs directly to S3 (no local writes)       │  │
│  └─────────────────────────────────────────────────────────────┘  │
└──────────────────────────┬─────────────────────────────────────────┘
                           │ boto3 upload (credentialed)
                           ▼
┌──────────────────────────────────────────────────┐
│  S3: bigdata-cs-careers                          │
│                                                  │
│  processed/YYYY-MM-DD/                           │
│    topic_analysis/topic_analysis.csv             │
│    sentiment_by_topic/sentiment_by_topic.csv     │
│    posts_by_industry/posts_by_industry.csv       │
│    salary_stats/salary_stats.csv                 │
│    experience_distribution/...csv                │
│    skills_summary/skills_summary.csv             │
│    temporal_trends/temporal_trends.csv           │
│    network_metrics/network_metrics.csv           │
│    company_mentions/company_mentions.csv         │
│    topic_by_industry/topic_by_industry.csv       │
│    skills_by_industry/skills_by_industry.csv     │
└──────────────────────────┬───────────────────────┘
                           │
                           │ s3fs anonymous reads (public bucket)
                           ▼
┌──────────────────────────────────────────────────┐
│  Streamlit Community Cloud (app.py)              │
│                                                  │
│  • Lists all processed/YYYY-MM-DD/ date folders  │
│  • Aggregates CSVs across all dates              │
│  • @st.cache_data(ttl=3600) per date+subfolder   │
│  • Interactive Plotly charts                     │
│  • Sidebar: topic + industry multiselect filters │
│  • No AWS credentials needed (public bucket)     │
└──────────────────────────────────────────────────┘
```

## Component Summary

| Component | Technology | Trigger |
|---|---|---|
| Data collection | Python script (collector.py) | Manual — run locally |
| Cross-day deduplication | `metadata/seen_ids.json` in S3 | Updated each collector run |
| Raw storage | AWS S3 (`raw/YYYY-MM-DD/`) | Written by collector |
| Processing | PySpark `local[1]`, pandas | Manual — `python process_reddit_data.py` |
| Processed storage | AWS S3 (`processed/YYYY-MM-DD/`) | Written by processor (no local files) |
| Dashboard | Streamlit Community Cloud | Always-on, reads from S3 on load |

## AWS Resources

- **S3 bucket**: `bigdata-cs-careers`
  - `raw/` — raw Reddit posts per date
  - `processed/` — 11 CSV datasets per date
  - `metadata/seen_ids.json` — cumulative post ID registry for deduplication

## Why Not Lambda?

Lambda was the original plan for automated daily collection, but after 5 days of collecting data Reddit blocked us. We started receiving a HTTP 403 error even though the first collection runs were successful. Lambda functions run on AWS infrastructure, so every collection attempt fails immediately. The collector must run on a local machine (non-AWS IP) to reach the Reddit API.

## Data Collection Detail

- Subreddits: `r/csMajors`, `r/cscareerquestions`
- Listings: `new`, `top` (both per subreddit)
- Up to 10 pages × 100 posts per listing = ~2000 posts before deduplication
- Cross-day deduplication: post IDs tracked in `metadata/seen_ids.json` in S3
- Rate limiting: 1s between pages, 2s between subreddit/listing changes
- HTTP 429 retry: exponential backoff (up to 5 attempts, 1s/2s/4s/8s/16s)
