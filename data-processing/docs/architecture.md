## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  AWS Cloud                                                        │
│                                                                   │
│  ┌─────────────────┐    triggers daily     ┌──────────────────┐  │
│  │  CloudWatch     │ ──── 6:00 AM UTC ───▶ │  Lambda          │  │
│  │  Events         │                        │  (collector.py)  │  │
│  └─────────────────┘                        │                  │  │
│                                             │  • HTTP GET to   │  │
│                                             │    Reddit API    │  │
│                                             │  • ~2000 posts   │  │
│                                             │  • deduplication │  │
│                                             │  • 429 backoff   │  │
│                                             └────────┬─────────┘  │
│                                                      │            │
│                                                      ▼            │
│                              ┌───────────────────────────────┐   │
│                              │  S3: bigdata-cs-careers       │   │
│                              │                               │   │
│                              │  raw/YYYY-MM-DD/              │   │
│                              │    posts.json                 │   │
│                              │    metadata.json              │   │
│                              └───────────────┬───────────────┘   │
└──────────────────────────────────────────────┼───────────────────┘
                                               │
                    ┌──────────────────────────┘
                    │ boto3 download
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
│  │  8.  Company mention extraction (30+ companies)            │  │
│  │  9.  Advice category detection (9 categories)              │  │
│  │  10. Sentiment analysis (keyword-based pos/neg/neutral)     │  │
│  │  11. Time series aggregation (by year/month/day)           │  │
│  │  12. Save 8 CSVs to data/processed/                        │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                          │                                         │
│                          │ boto3 upload                            │
└──────────────────────────┼─────────────────────────────────────────┘
                           │
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
└──────────────────────────┬───────────────────────┘
                           │
                           │ s3fs reads (st.secrets credentials)
                           ▼
┌──────────────────────────────────────────────────┐
│  Streamlit Community Cloud (app.py)              │
│                                                  │
│  • Auto-detects latest processed date in S3      │
│  • Loads CSVs via s3fs + pandas                  │
│  • @st.cache_data(ttl=3600) per CSV              │
│  • Interactive Plotly charts                     │
│  • Sidebar: topic + industry multiselect filters │
└──────────────────────────────────────────────────┘
```

## Component Summary

| Component | Technology | Trigger |
|---|---|---|
| Data collection | AWS Lambda (Python 3.9) | CloudWatch Events — daily 6 AM UTC |
| Raw storage | AWS S3 (`raw/YYYY-MM-DD/`) | Written by Lambda |
| Processing | PySpark `local[1]`, pandas | Manual — `python process_reddit_data.py` |
| Processed storage | AWS S3 (`processed/YYYY-MM-DD/`) | Written by processor |
| Dashboard | Streamlit Community Cloud | Always-on, reads from S3 on load |

## AWS Resources

- **S3 bucket**: `bigdata-cs-careers`
- **Lambda function**: `cs-career-collector`
  - Runtime: Python 3.9 | Memory: 512 MB | Timeout: 5 min
  - IAM Role: `LabRole`
- **CloudWatch rule**: `cs-career-daily` — `cron(0 6 * * ? *)`

## Data Collection Detail

- Subreddits: `r/csMajors`, `r/cscareerquestions`
- Listings: `new`, `top` (both per subreddit)
- Up to 10 pages × 100 posts per listing = ~2000 posts before deduplication
- Rate limiting: 1s between pages, 2s between subreddit/listing changes
- HTTP 429 retry: exponential backoff (up to 5 attempts, 1s/2s/4s/8s/16s)
- Post deduplication by ID across all subreddit/listing combinations
