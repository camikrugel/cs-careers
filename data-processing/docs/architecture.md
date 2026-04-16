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
